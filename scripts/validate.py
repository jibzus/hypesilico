#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hypesilico Validation Script

Compares API responses against expected golden data.
Uses only Python standard library (no external dependencies).

Usage:
    python3 validate.py [--base-url URL] [--expected FILE] [-v]
"""

import argparse
import json
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError


# === Configuration ===
DEFAULT_BASE_URL = "http://localhost:8080"
DEFAULT_EXPECTED_FILE = "validation/expected.json"


# === Data Classes ===
@dataclass
class TestResult:
    """Result of a single test case."""
    name: str
    passed: bool
    message: str
    expected: Any = None
    actual: Any = None


# === Utility Functions ===
def is_decimal_string(s: str) -> bool:
    """Check if string looks like a decimal number."""
    try:
        Decimal(s)
        return True
    except (InvalidOperation, ValueError, TypeError):
        return False


def decimal_approx_equal(expected: str, actual: str, tolerance_places: int = 2) -> bool:
    """Compare decimal strings with tolerance."""
    try:
        exp = Decimal(expected)
        act = Decimal(actual)
        tolerance = Decimal(10) ** -tolerance_places
        return abs(exp - act) <= tolerance
    except (InvalidOperation, ValueError, TypeError):
        return expected == actual


def make_request(base_url: str, endpoint: str, params: Optional[Dict] = None) -> Tuple[int, Any]:
    """
    Make HTTP GET request and return (status_code, json_or_body).

    Returns:
        Tuple of (HTTP status code, parsed JSON or raw body string)
        Returns (-1, error_message) on connection error
    """
    url = f"{base_url}{endpoint}"
    if params:
        url += "?" + urlencode(params)

    req = Request(url, headers={"Accept": "application/json"})
    try:
        with urlopen(req, timeout=30) as response:
            body = response.read().decode("utf-8")
            try:
                return response.status, json.loads(body)
            except json.JSONDecodeError:
                return response.status, body
    except HTTPError as e:
        try:
            body = e.read().decode("utf-8")
        except Exception:
            body = str(e)
        return e.code, body
    except URLError as e:
        return -1, str(e.reason)
    except Exception as e:
        return -1, str(e)


# === Validators ===
def validate_health_endpoints(base_url: str, expected: dict, verbose: bool = False) -> List[TestResult]:
    """Validate /health and /ready endpoints."""
    results = []

    for name, config in expected.get("health_tests", {}).items():
        endpoint = config.get("endpoint", f"/{name}")
        expected_status = config.get("expected_status", 200)
        expected_body = config.get("expected_body")

        status, body = make_request(base_url, endpoint)

        status_ok = status == expected_status
        body_ok = expected_body is None or expected_body in str(body)

        passed = status_ok and body_ok

        if not passed:
            message = f"Status={status} (expected {expected_status})"
            if not body_ok:
                message += f", Body={body!r} (expected {expected_body!r})"
        else:
            message = "OK"

        results.append(TestResult(
            name=f"{endpoint}",
            passed=passed,
            message=message,
            expected={"status": expected_status, "body": expected_body},
            actual={"status": status, "body": body}
        ))

    return results


def validate_user_endpoint(
    base_url: str,
    user_address: str,
    endpoint_path: str,
    test_name: str,
    test_config: dict,
    tolerance_places: int = 2
) -> TestResult:
    """Validate a user-specific endpoint."""
    params = {"user": user_address, **test_config.get("params", {})}
    expected = test_config.get("expected", {})

    status, response = make_request(base_url, endpoint_path, params)

    # Check for HTTP errors
    if status != 200:
        return TestResult(
            name=f"{endpoint_path} [{test_name}]",
            passed=False,
            message=f"HTTP {status}: {response}",
            expected=expected,
            actual={"status": status, "response": response}
        )

    errors = []

    # Check required fields exist
    if "has_fields" in expected:
        for field in expected["has_fields"]:
            if field not in response:
                errors.append(f"Missing field: {field}")

    # Check exact field values
    for key, exp_value in expected.items():
        if key in ("has_fields", "is_array", "min_count"):
            continue  # Skip meta-fields

        if key not in response:
            # Fail on missing expected keys (not just has_fields)
            errors.append(f"Missing expected field '{key}' in response")
            continue

        actual = response[key]

        # Handle different comparison types
        if isinstance(exp_value, str) and is_decimal_string(exp_value):
            if not decimal_approx_equal(exp_value, str(actual), tolerance_places):
                errors.append(f"{key}: expected ~{exp_value}, got {actual}")
        elif isinstance(exp_value, (int, bool)):
            if actual != exp_value:
                errors.append(f"{key}: expected {exp_value}, got {actual}")
        elif isinstance(exp_value, str):
            if str(actual) != exp_value:
                errors.append(f"{key}: expected {exp_value!r}, got {actual!r}")

    passed = len(errors) == 0
    return TestResult(
        name=f"{endpoint_path} [{test_name}]",
        passed=passed,
        message="OK" if passed else "; ".join(errors),
        expected=expected,
        actual=response if not passed else None
    )


def validate_pnl(base_url: str, user: dict, test_name: str, test_config: dict, tolerance: int) -> TestResult:
    """Validate /v1/pnl endpoint."""
    return validate_user_endpoint(base_url, user["address"], "/v1/pnl", test_name, test_config, tolerance)


def validate_trades(base_url: str, user: dict, test_name: str, test_config: dict, tolerance: int) -> TestResult:
    """Validate /v1/trades endpoint."""
    params = {"user": user["address"], **test_config.get("params", {})}
    expected = test_config.get("expected", {})

    status, response = make_request(base_url, "/v1/trades", params)

    if status != 200:
        return TestResult(
            name=f"/v1/trades [{test_name}]",
            passed=False,
            message=f"HTTP {status}: {response}"
        )

    errors = []

    # Check has_fields
    if "has_fields" in expected:
        for field in expected["has_fields"]:
            if field not in response:
                errors.append(f"Missing field: {field}")

    # Check trade count if specified
    if "trade_count" in expected:
        actual_count = len(response.get("trades", []))
        if actual_count != expected["trade_count"]:
            errors.append(f"trade_count: expected {expected['trade_count']}, got {actual_count}")

    passed = len(errors) == 0
    return TestResult(
        name=f"/v1/trades [{test_name}]",
        passed=passed,
        message="OK" if passed else "; ".join(errors),
        expected=expected,
        actual={"trade_count": len(response.get("trades", []))} if not passed else None
    )


def validate_positions(base_url: str, user: dict, test_name: str, test_config: dict, tolerance: int) -> TestResult:
    """Validate /v1/positions/history endpoint."""
    params = {"user": user["address"], **test_config.get("params", {})}
    expected = test_config.get("expected", {})

    status, response = make_request(base_url, "/v1/positions/history", params)

    if status != 200:
        return TestResult(
            name=f"/v1/positions/history [{test_name}]",
            passed=False,
            message=f"HTTP {status}: {response}"
        )

    errors = []

    # Check has_fields
    if "has_fields" in expected:
        for field in expected["has_fields"]:
            if field not in response:
                errors.append(f"Missing field: {field}")

    # Check snapshot count if specified
    if "snapshot_count" in expected:
        actual_count = len(response.get("snapshots", []))
        if actual_count != expected["snapshot_count"]:
            errors.append(f"snapshot_count: expected {expected['snapshot_count']}, got {actual_count}")

    passed = len(errors) == 0
    return TestResult(
        name=f"/v1/positions/history [{test_name}]",
        passed=passed,
        message="OK" if passed else "; ".join(errors)
    )


def validate_deposits(base_url: str, user: dict, test_name: str, test_config: dict, tolerance: int) -> TestResult:
    """Validate /v1/deposits endpoint."""
    params = {"user": user["address"], **test_config.get("params", {})}
    expected = test_config.get("expected", {})

    status, response = make_request(base_url, "/v1/deposits", params)

    if status != 200:
        return TestResult(
            name=f"/v1/deposits [{test_name}]",
            passed=False,
            message=f"HTTP {status}: {response}"
        )

    errors = []

    # Check has_fields
    if "has_fields" in expected:
        for field in expected["has_fields"]:
            if field not in response:
                errors.append(f"Missing field: {field}")

    # Check deposit count if specified
    if "deposit_count" in expected and response.get("depositCount") != expected["deposit_count"]:
        errors.append(f"depositCount: expected {expected['deposit_count']}, got {response.get('depositCount')}")

    # Check total deposits if specified
    if "total_deposits" in expected:
        if not decimal_approx_equal(expected["total_deposits"], response.get("totalDeposits", "0"), tolerance):
            errors.append(f"totalDeposits: expected ~{expected['total_deposits']}, got {response.get('totalDeposits')}")

    passed = len(errors) == 0
    return TestResult(
        name=f"/v1/deposits [{test_name}]",
        passed=passed,
        message="OK" if passed else "; ".join(errors)
    )


def validate_leaderboard(base_url: str, test_config: Dict[str, Any]) -> TestResult:
    """Validate /v1/leaderboard endpoint."""
    params = test_config.get("params", {})
    expected = test_config.get("expected", {})
    description = test_config.get("description", str(params))

    status, response = make_request(base_url, "/v1/leaderboard", params)

    if status != 200:
        return TestResult(
            name=f"/v1/leaderboard [{description}]",
            passed=False,
            message=f"HTTP {status}: {response}"
        )

    errors: List[str] = []

    # Check is_array
    if expected.get("is_array") and not isinstance(response, list):
        errors.append(f"Expected array, got {type(response).__name__}")

    # Check minimum entry count
    if "entry_count_min" in expected and isinstance(response, list):
        if len(response) < expected["entry_count_min"]:
            errors.append(f"Expected at least {expected['entry_count_min']} entries, got {len(response)}")

    passed = len(errors) == 0
    return TestResult(
        name=f"/v1/leaderboard [{description}]",
        passed=passed,
        message="OK" if passed else "; ".join(errors)
    )


def validate_error_endpoint(base_url: str, test_name: str, test_config: Dict[str, Any]) -> TestResult:
    """Validate that an endpoint returns the expected error status."""
    endpoint = test_config.get("endpoint", "")
    params = test_config.get("params", {})
    expected_status = test_config.get("expected_status", 400)
    description = test_config.get("description", test_name)

    status, response = make_request(base_url, endpoint, params if params else None)

    passed = status == expected_status
    if passed:
        message = "OK"
    else:
        message = f"Expected status {expected_status}, got {status}"

    return TestResult(
        name=f"{endpoint} [{description}]",
        passed=passed,
        message=message,
        expected={"status": expected_status},
        actual={"status": status, "response": str(response)[:100]}
    )


# === Main Runner ===
def run_validation(base_url: str, expected_file: str, verbose: bool = False) -> bool:
    """
    Run all validation tests.

    Returns:
        True if all tests passed, False otherwise
    """
    try:
        with open(expected_file) as f:
            expected = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: Expected file not found: {expected_file}")
        return False
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in expected file: {e}")
        return False

    # Get configuration
    config = expected.get("validation_config", {})
    tolerance = config.get("decimal_tolerance_places", 2)

    all_results: List[TestResult] = []

    # Health endpoints
    print("=== Health Endpoints ===")
    health_results = validate_health_endpoints(base_url, expected, verbose)
    all_results.extend(health_results)
    for r in health_results:
        status = "\033[92mPASS\033[0m" if r.passed else "\033[91mFAIL\033[0m"
        print(f"  [{status}] {r.name}: {r.message}")

    # Error/negative tests
    error_tests = expected.get("error_tests", {})
    if error_tests:
        print("\n=== Error Tests (Negative Cases) ===")
        for test_name, test_config in error_tests.items():
            r = validate_error_endpoint(base_url, test_name, test_config)
            all_results.append(r)
            status = "\033[92mPASS\033[0m" if r.passed else "\033[91mFAIL\033[0m"
            print(f"  [{status}] {r.name}: {r.message}")
            if verbose and not r.passed and r.actual:
                print(f"         Actual: {r.actual}")

    # User endpoints
    print("\n=== User Endpoints ===")
    for user in expected.get("test_users", []):
        addr_short = user["address"][:10] + "..." + user["address"][-4:]
        desc = user.get("description", "")
        print(f"\nUser: {addr_short} ({desc})")

        tests = user.get("tests", {})

        # Map test names to validators
        test_handlers = {
            "pnl": lambda name, cfg: validate_pnl(base_url, user, name, cfg, tolerance),
            "pnl_with_coin": lambda name, cfg: validate_pnl(base_url, user, name, cfg, tolerance),
            "pnl_builder_only": lambda name, cfg: validate_pnl(base_url, user, name, cfg, tolerance),
            "pnl_time_range": lambda name, cfg: validate_pnl(base_url, user, name, cfg, tolerance),
            "trades": lambda name, cfg: validate_trades(base_url, user, name, cfg, tolerance),
            "trades_with_coin": lambda name, cfg: validate_trades(base_url, user, name, cfg, tolerance),
            "trades_builder_only": lambda name, cfg: validate_trades(base_url, user, name, cfg, tolerance),
            "positions_history": lambda name, cfg: validate_positions(base_url, user, name, cfg, tolerance),
            "positions_with_coin": lambda name, cfg: validate_positions(base_url, user, name, cfg, tolerance),
            "deposits": lambda name, cfg: validate_deposits(base_url, user, name, cfg, tolerance),
        }

        for test_name, test_config in tests.items():
            handler = test_handlers.get(test_name)
            if handler:
                r = handler(test_name, test_config)
                all_results.append(r)
                status = "\033[92mPASS\033[0m" if r.passed else "\033[91mFAIL\033[0m"
                print(f"  [{status}] {r.name}: {r.message}")
                if verbose and not r.passed and r.actual:
                    print(f"         Actual: {json.dumps(r.actual, indent=2)[:200]}")

    # Leaderboard endpoints
    print("\n=== Leaderboard Endpoints ===")
    for lb_test in expected.get("leaderboard_tests", []):
        r = validate_leaderboard(base_url, lb_test)
        all_results.append(r)
        status = "\033[92mPASS\033[0m" if r.passed else "\033[91mFAIL\033[0m"
        print(f"  [{status}] {r.name}: {r.message}")

    # Summary
    passed = sum(1 for r in all_results if r.passed)
    failed = sum(1 for r in all_results if not r.passed)
    total = len(all_results)

    print(f"\n{'='*50}")
    if failed == 0:
        print(f"\033[92mSUMMARY: {passed}/{total} tests passed\033[0m")
    else:
        print(f"\033[91mSUMMARY: {passed}/{total} tests passed ({failed} failed)\033[0m")
    print(f"{'='*50}")

    return failed == 0


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Hypesilico Validation Script - Compare API responses against expected data"
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"API base URL (default: {DEFAULT_BASE_URL})"
    )
    parser.add_argument(
        "--expected",
        default=DEFAULT_EXPECTED_FILE,
        help=f"Expected data file (default: {DEFAULT_EXPECTED_FILE})"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output (show actual values on failure)"
    )

    args = parser.parse_args()

    print(f"Hypesilico Validation")
    print(f"Base URL: {args.base_url}")
    print(f"Expected file: {args.expected}")
    print()

    success = run_validation(args.base_url, args.expected, args.verbose)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
