#!/usr/bin/env python3
"""
Load test for anti-fraud API.
Sends requests with controlled concurrency to generate visible metrics
without overwhelming the cluster.

Usage:
    python scripts/load_test.py --url http://<EXTERNAL-IP>/predict
    python scripts/load_test.py --url http://<EXTERNAL-IP>/predict --rps 50 --duration 300
"""

import argparse
import json
import random
import time
import threading
from urllib.request import Request, urlopen
from urllib.error import URLError


def send_request(url):
    """Send a single prediction request."""
    payload = json.dumps({
        "transaction_id": random.randint(1, 1000000),
        "tx_amount": random.uniform(1.0, 50000.0),
        "tx_datetime": f"2026-03-22 {random.randint(0, 23):02d}:{random.randint(0, 59):02d}:00",
    }).encode()
    req = Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=10) as resp:
            return resp.status
    except Exception:
        return 0


def main():
    parser = argparse.ArgumentParser(description="Load test for anti-fraud API")
    parser.add_argument("--url", required=True, help="API predict endpoint URL")
    parser.add_argument("--rps", type=int, default=20, help="Target requests per second (default: 20)")
    parser.add_argument("--duration", type=int, default=300, help="Test duration in seconds (default: 300)")
    args = parser.parse_args()

    print(f"Load test: {args.url}")
    print(f"Target: ~{args.rps} RPS for {args.duration}s")
    print()

    start_time = time.time()
    total_ok = 0
    total_err = 0
    lock = threading.Lock()
    interval = 1.0 / args.rps

    while time.time() - start_time < args.duration:
        t = threading.Thread(target=lambda: None, daemon=True)

        def do_request():
            nonlocal total_ok, total_err
            status = send_request(args.url)
            with lock:
                if status == 200:
                    total_ok += 1
                else:
                    total_err += 1

        t = threading.Thread(target=do_request, daemon=True)
        t.start()
        time.sleep(interval)

        elapsed = time.time() - start_time
        if int(elapsed) % 10 == 0 and int(elapsed) > 0:
            with lock:
                rps = (total_ok + total_err) / elapsed
            if int(elapsed * 10) % 100 == 0:
                print(f"[{elapsed:.0f}s] OK: {total_ok}, Err: {total_err}, RPS: {rps:.1f}")

    elapsed = time.time() - start_time
    print(f"\nDone: {total_ok + total_err} requests in {elapsed:.0f}s "
          f"({(total_ok + total_err) / elapsed:.1f} RPS), "
          f"{total_err} errors")


if __name__ == "__main__":
    main()
