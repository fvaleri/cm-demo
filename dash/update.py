#!/usr/bin/env python3
# Polls kafka-cluster-mirrors.sh --describe --json and writes the output to /tmp/data.json.
# Serves index.html on --port (default 8099) which fetches /tmp/data.json every second.

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent


def parse_args():
    parser = argparse.ArgumentParser(description="Poll kafka-cluster-mirrors and serve the dashboard.")
    parser.add_argument("--kafka-home", required=True, help="Kafka installation directory")
    parser.add_argument("--bootstrap-server", required=True, help="Target Kafka broker")
    parser.add_argument("--interval", type=int, default=2, help="Seconds between updates (default: 2)")
    parser.add_argument("--port", type=int, default=8099, help="HTTP server port (default: 8099)")
    parser.add_argument("--command-config", default=None, help="Property file for Admin client")
    parser.add_argument("--mirrors", default=None, help="Comma-separated list of mirrors to describe")
    return parser.parse_args()


class Handler(SimpleHTTPRequestHandler):
    def translate_path(self, path):
        # /tmp/data.json lives outside the web root, so intercept it here
        if path.split("?")[0] == "/tmp/data.json":
            return "/tmp/data.json"
        return super().translate_path(path)


def start_http_server(port):
    os.chdir(SCRIPT_DIR)
    server = HTTPServer(("127.0.0.1", port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def build_base_cmd(args):
    cmd = [
        str(Path(args.kafka_home) / "bin" / "kafka-cluster-mirrors.sh"),
        "--bootstrap-server", args.bootstrap_server,
        "--describe", "--json",
    ]
    if args.command_config:
        cmd += ["--command-config", str(SCRIPT_DIR / args.command_config)]
    return cmd


def fetch_all(base_cmd):
    result = subprocess.run(base_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return None
    try:
        json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    return result.stdout


def fetch_per_mirror(base_cmd, mirrors):
    combined = []
    for mirror in mirrors:
        result = subprocess.run(base_cmd + ["--mirror", mirror], capture_output=True, text=True)
        if result.returncode != 0:
            return None
        try:
            combined.extend(json.loads(result.stdout))
        except json.JSONDecodeError:
            return None
    return json.dumps(combined)


def write_data(data_file, content):
    # atomic rename to avoid partial reads from the dashboard
    tmp = data_file.with_suffix(".json.tmp")
    tmp.write_text(content)
    tmp.rename(data_file)
    return len(content)


def main():
    args = parse_args()
    data_file = Path("/tmp") / "data.json"
    # remove stale data from a previous run
    if data_file.exists():
        data_file.unlink()
    base_cmd = build_base_cmd(args)
    mirrors = [m.strip() for m in args.mirrors.split(",")] if args.mirrors else []

    server = start_http_server(args.port)

    print(f"[*] Dashboard: http://127.0.0.1:{args.port}")
    print(f"[*] Polling every {args.interval}s")
    print()

    def shutdown(signum, frame):
        print("\nShutting down...")
        server.shutdown()
        tmp = data_file.with_suffix(".json.tmp")
        if tmp.exists():
            tmp.unlink()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    last_write = 0.0
    while True:
        if mirrors:
            content = fetch_per_mirror(base_cmd, mirrors)
        else:
            content = fetch_all(base_cmd)

        ts = time.strftime("%H:%M:%S")
        if content is not None:
            size = write_data(data_file, content)
            last_write = time.monotonic()
            print(f"[{ts}] Updated data.json ({size} bytes)")
        else:
            # remove stale data so the dashboard does not show outdated state
            if data_file.exists() and time.monotonic() - last_write > 5:
                data_file.unlink()
            print(f"[{ts}] Failed to fetch mirror data")

        time.sleep(args.interval)


if __name__ == "__main__":
    main()
