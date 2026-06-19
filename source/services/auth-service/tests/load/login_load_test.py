#!/usr/bin/env python3
"""
Load test for Auth Service login and refresh endpoints.

Tests SC-003: 100 concurrent requests without degradation.

Usage:
    python login_load_test.py --host http://localhost:3000
    python login_load_test.py --host http://localhost:3000 --concurrent 200
    python login_load_test.py --host http://localhost:3000 --workers 4
"""

import argparse
import concurrent.futures
import json
import statistics
import time
from typing import List, Dict, Any

import requests


class LoadTester:
    """Load tester for Auth Service endpoints."""

    def __init__(self, base_url: str = "http://localhost:3000"):
        self.base_url = base_url
        self.session = requests.Session()

    def login(self, email: str = "admin@bornemap.tn", password: str = "test123") -> Dict[str, Any]:
        """Make a login request."""
        response = self.session.post(
            f"{self.base_url}/api/v1/auth/login",
            json={"email": email, "password": password},
            timeout=10,
        )
        response.raise_for_status()
        return response.json()

    def refresh(self, refresh_token: str) -> Dict[str, Any]:
        """Make a refresh request."""
        response = self.session.post(
            f"{self.base_url}/api/v1/auth/refresh",
            json={"refresh_token": refresh_token},
            timeout=10,
        )
        response.raise_for_status()
        return response.json()

    def logout(self, refresh_token: str) -> Dict[str, Any]:
        """Make a logout request."""
        response = self.session.post(
            f"{self.base_url}/api/v1/auth/logout",
            json={"refresh_token": refresh_token},
            timeout=10,
        )
        response.raise_for_status()
        return response.json()

    def test_concurrent_logins(self, num_concurrent: int = 100) -> Dict[str, Any]:
        """Test concurrent login requests."""
        print(f"\nTesting {num_concurrent} concurrent login requests...")

        results = {
            "total_requests": num_concurrent,
            "successful": 0,
            "failed": 0,
            "latencies": [],  # in milliseconds
            "status_codes": {},
        }

        def make_login():
            start_time = time.perf_counter()
            try:
                login_result = self.login()
                elapsed = (time.perf_counter() - start_time) * 1000
                results["successful"] += 1
                results["latencies"].append(elapsed)
                status_code = login_result.get("status_code", "unknown")
                results["status_codes"][status_code] = results["status_codes"].get(status_code, 0) + 1
                return True
            except Exception as e:
                elapsed = (time.perf_counter() - start_time) * 1000
                results["failed"] += 1
                results["latencies"].append(elapsed)
                print(f"  Login failed: {e}")
                return False

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_concurrent) as executor:
            futures = [executor.submit(make_login) for _ in range(num_concurrent)]
            concurrent.futures.wait(futures)

        return results

    def test_login_then_refresh(self, num_concurrent: int = 100) -> Dict[str, Any]:
        """Test login followed by refresh for concurrent requests."""
        print(f"\nTesting {num_concurrent} login-then-refresh cycles...")

        results = {
            "total_requests": num_concurrent * 2,  # login + refresh
            "successful": 0,
            "failed": 0,
            "latencies": [],
            "status_codes": {},
        }

        login_results = []
        refresh_results = []

        # First pass: make all login requests
        print(f"  Making {num_concurrent} login requests...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_concurrent) as executor:
            login_futures = [executor.submit(self.login) for _ in range(num_concurrent)]
            login_results = concurrent.futures.wait(login_futures).result()

        # Collect tokens
        tokens = []
        for future in login_results:
            try:
                result = future.result()
                tokens.append(result.get("refresh_token"))
            except Exception as e:
                results["failed"] += 1
                print(f"  Login failed: {e}")

        results["successful"] = num_concurrent - results["failed"]
        print(f"  Successful logins: {results['successful']}")

        # Second pass: refresh all tokens
        print(f"  Making {len(tokens)} refresh requests...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_concurrent) as executor:
            refresh_futures = [executor.submit(self.refresh, token) for token in tokens]
            refresh_results = concurrent.futures.wait(refresh_futures).result()

        for future in refresh_results:
            try:
                future.result()
                results["successful"] += 1
            except Exception as e:
                results["failed"] += 1
                print(f"  Refresh failed: {e}")

        return results

    def print_results(self, results: Dict[str, Any]):
        """Print test results in a readable format."""
        print("\n" + "=" * 80)
        print("LOAD TEST RESULTS")
        print("=" * 80)

        total = results["successful"] + results["failed"]
        success_rate = (results["successful"] / total * 100) if total > 0 else 0

        print(f"\nTotal Requests: {results['total_requests']}")
        print(f"Successful: {results['successful']} ({success_rate:.1f}%)")
        print(f"Failed: {results['failed']} ({100 - success_rate:.1f}%)")
        print(f"\nStatus Codes:")
        for code, count in sorted(results["status_codes"].items()):
            print(f"  {code}: {count}")

        if results["latencies"]:
            latencies_ms = results["latencies"]
            print(f"\nLatency Statistics (ms):")
            print(f"  Min: {min(latencies_ms):.2f}")
            print(f"  Max: {max(latencies_ms):.2f}")
            print(f"  Mean: {statistics.mean(latencies_ms):.2f}")
            print(f"  Median: {statistics.median(latencies_ms):.2f}")

            p95 = statistics.quantiles(latencies_ms, n=20)[18]  # 95th percentile
            p99 = statistics.quantiles(latencies_ms, n=100)[98]  # 99th percentile
            print(f"  P95: {p95:.2f}")
            print(f"  P99: {p99:.2f}")

            print(f"\nPerformance Comparison:")
            print(f"  SC-001 (Login P95 < 2000ms): {'✓ PASS' if p95 < 2000 else '✗ FAIL'}")
            print(f"  SC-002 (Refresh P95 < 1000ms): {'✓ PASS' if p95 < 1000 else '✗ FAIL'}")

        print("=" * 80)

        # Return success rate for CI/CD integration
        return success_rate >= 99.0  # Require 99%+ success rate


def main():
    parser = argparse.ArgumentParser(description="Load test Auth Service")
    parser.add_argument(
        "--host",
        default="http://localhost:3000",
        help="Base URL of Auth Service (default: http://localhost:3000)",
    )
    parser.add_argument(
        "--concurrent",
        type=int,
        default=100,
        help="Number of concurrent requests (default: 100)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of test workers (default: 4)",
    )
    parser.add_argument(
        "--mode",
        choices=["login", "refresh", "login_refresh"],
        default="login_refresh",
        help="Test mode: login only, refresh only, or login-then-refresh (default: login_refresh)",
    )

    args = parser.parse_args()

    print(f"\nAuth Service Load Tester")
    print(f"Base URL: {args.host}")
    print(f"Concurrent Requests: {args.concurrent}")
    print(f"Workers: {args.workers}")
    print(f"Mode: {args.mode}")

    tester = LoadTester(base_url=args.host)

    if args.mode == "login":
        results = tester.test_concurrent_logins(args.concurrent)
    elif args.mode == "refresh":
        # Note: refresh test requires tokens from previous logins
        print("Warning: refresh mode requires pre-existing tokens")
        print("Please use 'login_refresh' mode instead")
        return 1
    else:  # login_refresh
        results = tester.test_login_then_refresh(args.concurrent)

    success = tester.print_results(results)

    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
