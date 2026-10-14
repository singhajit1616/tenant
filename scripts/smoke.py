"""Verify the deployed Compose application; uses Python's standard library only."""

import concurrent.futures
import json
import os
import time
import urllib.error
import urllib.request
import uuid

BASE = os.environ.get("API_URL", "http://localhost:8080")


# Send an HTTP request and return its status and parsed JSON, including error responses.
def request(method, path, body=None):
    payload = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=payload, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        response = urllib.request.urlopen(req, timeout=5)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.status, json.load(response)


# Poll readiness for up to 90 seconds and fail if the deployed app never becomes ready.
def wait_ready():
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            status, _ = request("GET", "/readyz")
            if status == 200:
                return
        except (OSError, ValueError):
            pass
        time.sleep(0.5)
    raise AssertionError("application did not become ready")


# Poll a tenant for up to 30 seconds until the expected status appears, failing on errors or failure state.
def wait_tenant(tenant_id, state):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        status, tenant = request("GET", "/v1/tenants/" + tenant_id)
        assert status == 200, (status, tenant)
        if tenant["status"] == state:
            return tenant
        assert tenant["status"] != "failed", tenant
        time.sleep(0.1)
    raise AssertionError(f"tenant {tenant_id} did not reach {state}")


# Verify deployed create/update/destroy, concurrent slug and version conflicts, task outcomes, and destroyed slug uniqueness.
def main():
    wait_ready()
    slug = "smoke-" + uuid.uuid4().hex[:12]
    body = {"slug": slug, "name": "Smoke"}
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        creates = list(pool.map(lambda _: request("POST", "/v1/tenants", body), range(8)))
    winners = [tenant for status, tenant in creates if status == 201]
    assert len(winners) == 1, creates
    for status, result in creates:
        if status != 201:
            assert status == 409 and result["error"]["code"] == "tenant_already_exists", result
    tenant_id = winners[0]["id"]
    active = wait_tenant(tenant_id, "active")
    assert active["version"] == 3, active

    patch = {"name": "Smoke updated", "version": active["version"]}
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        patches = list(pool.map(
            lambda _: request("PATCH", "/v1/tenants/" + tenant_id, patch), range(8)
        ))
    assert sum(status == 200 for status, _ in patches) == 1, patches
    for status, result in patches:
        if status != 200:
            assert status == 409 and result["error"]["code"] == "tenant_version_conflict", result
    active = wait_tenant(tenant_id, "active")
    assert active["name"] == "Smoke updated" and active["version"] == 6, active
    status, _ = request("DELETE", "/v1/tenants/" + tenant_id)
    assert status == 202, status
    destroyed = wait_tenant(tenant_id, "destroyed")
    assert destroyed["version"] == 9, destroyed
    status, page = request("GET", "/v1/tenants/" + tenant_id + "/tasks")
    assert status == 200 and len(page["items"]) == 3, page
    assert all(task["status"] == "done" for task in page["items"]), page
    assert {task["type"] for task in page["items"]} == {"deploy", "update", "destroy"}, page
    status, result = request("POST", "/v1/tenants", body)
    assert status == 409 and result["error"]["code"] == "tenant_already_exists", result
    print("PASS: deployed lifecycle, concurrent slug creation/PATCH, and destroyed slug uniqueness")


if __name__ == "__main__":
    main()
