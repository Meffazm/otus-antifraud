#!/usr/bin/env python3
"""
Load test for anti-fraud API.
Sends requests with increasing concurrency to simulate DDoS attack.

Usage:
    python scripts/load_test.py --url http://<EXTERNAL-IP>/predict
    python scripts/load_test.py --url http://<EXTERNAL-IP>/predict --max-workers 200 --duration 300
"""

import argparse
import json
import random
import time
import threading
from concurrent.futures import ThreadPoolExecutor
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
    except URLError:
        return 0
    except Exception:
        return -1


def main():
    parser = argparse.ArgumentParser(description="Load test for anti-fraud API")
    parser.add_argument("--url", required=True, help="API predict endpoint URL")
    parser.add_argument("--max-workers", type=int, default=100, help="Max concurrent workers (default: 100)")
    parser.add_argument("--duration", type=int, default=600, help="Total test duration in seconds (default: 600)")
    parser.add_argument("--ramp-interval", type=int, default=30, help="Seconds between ramp-up steps (default: 30)")
    args = parser.parse_args()

    print(f"Load test: {args.url}")
    print(f"Ramping from 1 to {args.max_workers} workers over {args.duration}s")
    print(f"Ramp interval: {args.ramp_interval}s")
    print()

    start_time = time.time()
    total_requests = 0
    total_errors = 0
    lock = threading.Lock()

    running = True

    def worker():
        nonlocal total_requests, total_errors
        while running:
            status = send_request(args.url)
            with lock:
                total_requests += 1
                if status != 200:
                    total_errors += 1

    workers = []
    current_workers = 0
    steps = args.max_workers // max(1, (args.duration // args.ramp_interval))
    if steps < 1:
        steps = 1

    try:
        while time.time() - start_time < args.duration:
            # Ramp up workers
            new_count = min(current_workers + steps, args.max_workers)
            for _ in range(new_count - current_workers):
                t = threading.Thread(target=worker, daemon=True)
                t.start()
                workers.append(t)
            current_workers = new_count

            elapsed = time.time() - start_time
            with lock:
                rps = total_requests / max(1, elapsed)
                err_rate = (total_errors / max(1, total_requests)) * 100
            print(f"[{elapsed:.0f}s] Workers: {current_workers}, "
                  f"Total: {total_requests}, RPS: {rps:.0f}, "
                  f"Errors: {err_rate:.1f}%")

            time.sleep(args.ramp_interval)

    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        running = False
        elapsed = time.time() - start_time
        print(f"\nResults: {total_requests} requests in {elapsed:.0f}s "
              f"({total_requests / max(1, elapsed):.0f} RPS), "
              f"{total_errors} errors ({(total_errors / max(1, total_requests)) * 100:.1f}%)")


if __name__ == "__main__":
    main()
