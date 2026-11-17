#!/usr/bin/env python3
"""Local-only command runner for development tests.

Run with sudoers configured for the exact test command(s), or run the server
itself as root. Never expose this listener beyond loopback.
"""
from http.server import BaseHTTPRequestHandler, HTTPServer
import argparse
import json
import os
import secrets
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--workdir", default=os.getcwd())
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args()

    token = secrets.token_urlsafe(32)
    print(f"Listening on http://127.0.0.1:{args.port}", flush=True)
    print(f"Bearer token: {token}", flush=True)
    print("Commands run through sudo -n; no password is accepted over HTTP.", flush=True)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *values):
            sys.stderr.write((fmt % values) + "\n")

        def send_json(self, status, payload):
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            if self.path != "/run":
                self.send_json(404, {"error": "not found"})
                return
            if self.headers.get("Authorization") != f"Bearer {token}":
                self.send_json(401, {"error": "unauthorized"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                request = json.loads(self.rfile.read(length))
                command = request["command"]
                if not isinstance(command, list) or not command or not all(isinstance(x, str) for x in command):
                    raise ValueError("command must be a non-empty JSON string array")
            except (ValueError, KeyError, json.JSONDecodeError) as exc:
                self.send_json(400, {"error": str(exc)})
                return

            try:
                result = subprocess.run(
                    ["sudo", "-n", "--", *command],
                    cwd=args.workdir,
                    capture_output=True,
                    text=True,
                    timeout=args.timeout,
                    check=False,
                )
                self.send_json(200, {
                    "exit_code": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                })
            except subprocess.TimeoutExpired as exc:
                self.send_json(504, {
                    "error": "command timed out",
                    "stdout": exc.stdout or "",
                    "stderr": exc.stderr or "",
                })
            except OSError as exc:
                self.send_json(500, {"error": str(exc)})

    HTTPServer(("127.0.0.1", args.port), Handler).serve_forever()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
