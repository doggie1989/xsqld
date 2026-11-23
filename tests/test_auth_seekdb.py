#!/usr/bin/env python3
"""T8: Bearer token 鉴权集成测试（pytest 版）。

由 tests/integration_seekdb.py 的 run_auth_tests 转换而来，断言原文保持不变。
需要独立起一个带 --auth-token 的 xsqld，故用独立端口与独立 fixture。

运行：pytest tests/test_auth_seekdb.py -v
"""
import os
import signal
import subprocess
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(__file__))
from integration_seekdb import HttpClient, exec_stmt, must_ok_hdr  # noqa: E402

TOKEN = "pytest-auth-token"
HTTP_PORT = int(os.environ.get("XSQLD_TEST_AUTH_HTTP_PORT", "18282"))
ADMIN_PORT = int(os.environ.get("XSQLD_TEST_AUTH_ADMIN_PORT", "18283"))
BIN = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "target", "debug", "xsqld"))

user_http = admin_http = None
token = TOKEN


@pytest.fixture(scope="module", autouse=True)
def auth_server():
    global user_http, admin_http
    mport = os.environ.get("XSQLD_TEST_MYSQL_PORT", "12981")
    env = {k: v for k, v in os.environ.items()
           if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")}
    env["RUST_LOG"] = "warn"
    proc = subprocess.Popen(
        [BIN, "--mysql-host", "127.0.0.1", "--mysql-port", str(mport), "--mysql-user", "root",
         "--http-listen-addr", f"127.0.0.1:{HTTP_PORT}",
         "--admin-listen-addr", f"127.0.0.1:{ADMIN_PORT}",
         "--auth-token", TOKEN],
        stdout=open("/tmp/xsqld-pytest-auth.log", "w"), stderr=subprocess.STDOUT, env=env)
    user_http = HttpClient(f"http://127.0.0.1:{HTTP_PORT}")
    admin_http = HttpClient(f"http://127.0.0.1:{ADMIN_PORT}")
    for _ in range(120):
        try:
            st, _, _ = user_http.request("GET", "/health", {"Host": "localhost"})
            if st == 200:
                break
        except Exception:
            pass
        time.sleep(0.5)
    else:
        proc.kill()
        raise RuntimeError("auth xsqld did not become healthy; see /tmp/xsqld-pytest-auth.log")
    yield
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=30)
    except Exception:
        proc.kill()


def test_health_is_exempt_but_metrics_and_data_require_token():
    st, _, _ = user_http.request("GET", "/health", {"Host": "localhost"})
    assert st == 200, st
    st, _, _ = user_http.request("GET", "/metrics", {"Host": "localhost"})
    assert st == 401, st
    st, _ = exec_stmt(user_http, "authdb.db", "SELECT 1")
    assert st == 401, st
    st, _ = exec_stmt(
        user_http, "authdb.db", "SELECT 1",
        extra_headers={"Authorization": "Bearer wrong-token"},
    )
    assert st == 401, st


def test_admin_create_rejects_wrong_token_and_accepts_correct_one():
    st, _, raw = admin_http.request(
        "POST", "/v1/namespaces/authdb/create",
        {"Host": "localhost", "Content-Type": "application/json", "Authorization": "Bearer wrong"},
        b"{}",
    )
    assert st == 401, (st, raw)
    auth_hdr = {"Authorization": f"Bearer {token}"}
    st, _, raw = admin_http.request(
        "POST", "/v1/namespaces/authdb/create",
        {"Host": "localhost", "Content-Type": "application/json", **auth_hdr},
        b"{}",
    )
    assert st == 200, (st, raw)


def test_valid_token_caller_is_not_blocked_by_browser_origin():
    auth_hdr = {"Authorization": f"Bearer {token}"}
    _, r = must_ok_hdr(
        user_http, "authdb.db", "SELECT 42",
        {**auth_hdr, "Origin": "https://app.example"},
    )
    assert r["rows"] == [[{"type": "integer", "value": "42"}]]


def test_admin_delete_requires_token():
    auth_hdr = {"Authorization": f"Bearer {token}"}
    st, _, raw = admin_http.request(
        "DELETE", "/v1/namespaces/authdb", {"Host": "localhost", **auth_hdr},
    )
    assert st == 200, (st, raw)