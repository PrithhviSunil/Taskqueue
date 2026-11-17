#!/usr/bin/env python3
"""
Load test for the task queue. Standard library only, no installs needed.

Phase 1 (enqueue): N HTTP requests to POST /jobs from C client threads.
                   Reports requests/sec and p50/p99 request latency.
Phase 2 (process): waits until every one of those N jobs is DONE, then asks the
                   server for throughput/latency computed from database timestamps.

Usage:
  python3 bench/bench.py --jobs 20000 --type noop
  python3 bench/bench.py --jobs 2000 --type sleep --ms 20
  python3 bench/bench.py --jobs 20000 --enqueue-only   # load the queue, don't wait
"""
import argparse
import http.client
import json
import statistics
import threading
import time
from urllib.parse import urlparse


def percentile(values, p):
    values = sorted(values)
    k = (len(values) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (k - lo)


def enqueue_all(base, n, concurrency, body):
    host = urlparse(base)
    per_thread = [n // concurrency + (1 if i < n % concurrency else 0) for i in range(concurrency)]
    latencies, ids, errors = [], [], []
    lock = threading.Lock()
    payload = json.dumps(body)

    def worker(count):
        # One keep-alive connection per thread, like a real client would use.
        conn = http.client.HTTPConnection(host.hostname, host.port or 80, timeout=30)
        local_lat, local_ids, local_err = [], [], 0
        for _ in range(count):
            t0 = time.perf_counter()
            conn.request("POST", "/jobs", payload, {"Content-Type": "application/json"})
            resp = conn.getresponse()
            data = resp.read()
            local_lat.append((time.perf_counter() - t0) * 1000)
            if resp.status == 202:
                local_ids.append(json.loads(data)["id"])
            else:
                local_err += 1
        conn.close()
        with lock:
            latencies.extend(local_lat)
            ids.extend(local_ids)
            errors.append(local_err)

    threads = [threading.Thread(target=worker, args=(c,)) for c in per_thread]
    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - start
    return ids, latencies, sum(errors), elapsed


def get_json(base, path):
    host = urlparse(base)
    conn = http.client.HTTPConnection(host.hostname, host.port or 80, timeout=30)
    conn.request("GET", path)
    data = json.loads(conn.getresponse().read())
    conn.close()
    return data


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8080")
    ap.add_argument("--stats-url", default=None, help="defaults to --url")
    ap.add_argument("--jobs", type=int, default=10000)
    ap.add_argument("--concurrency", type=int, default=16)
    ap.add_argument("--type", default="noop", choices=["noop", "sleep", "flaky"])
    ap.add_argument("--ms", type=int, default=20, help="duration of each sleep job")
    ap.add_argument("--fail-rate", type=float, default=0.5, help="for flaky jobs")
    ap.add_argument("--enqueue-only", action="store_true")
    ap.add_argument("--timeout", type=int, default=600)
    args = ap.parse_args()
    stats_url = args.stats_url or args.url

    payloads = {"sleep": {"ms": args.ms}, "flaky": {"failRate": args.fail_rate}}
    body = {"type": args.type, "payload": payloads.get(args.type, {})}
    print(f"Enqueuing {args.jobs} '{args.type}' jobs with {args.concurrency} client threads...")
    ids, lat, errors, elapsed = enqueue_all(args.url, args.jobs, args.concurrency, body)
    print(f"  enqueue: {len(ids)/elapsed:,.0f} req/s | "
          f"p50 {statistics.median(lat):.1f} ms | p99 {percentile(lat, 0.99):.1f} ms | "
          f"errors {errors}")

    first, last = min(ids), max(ids)
    print(f"  job ids {first}..{last}")
    if args.enqueue_only:
        return

    print("Waiting for all jobs to finish...")
    deadline = time.time() + args.timeout
    while True:
        w = get_json(stats_url, f"/stats/window?from={first}&to={last}")
        # DEAD jobs are finished too: they've used up their retries.
        if w["done"] + w["dead"] >= len(ids) or time.time() > deadline:
            break
        time.sleep(0.5)

    done, secs = w["done"], w["processing_seconds"]
    print(f"  processed: {done}/{len(ids)} done, {w['dead']} dead, in {secs:.2f} s "
          f"-> {done / secs:,.0f} jobs/s")
    print(f"  enqueue->done latency: p50 {w['p50_latency_ms']:.0f} ms | "
          f"p99 {w['p99_latency_ms']:.0f} ms")
    print(f"  claimed more than once (retried or recovered): {w['claimed_more_than_once']}")


if __name__ == "__main__":
    main()
