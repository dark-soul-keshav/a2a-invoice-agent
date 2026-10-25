"""Quick local smoke test: python smoke_test.py (service must already be running)."""
import json, os, sys, urllib.request, urllib.error

base = os.getenv("TEST_BASE_URL", "http://127.0.0.1:8000")
token = os.getenv("TEST_BEARER_TOKEN", "local-smoke-token")
req = urllib.request.Request(base + "/.well-known/agent-card.json")
with urllib.request.urlopen(req, timeout=10) as r:
    card = json.loads(r.read())
assert card["name"] and card["version"] and isinstance(card["capabilities"], dict)
assert card["supportedInterfaces"][0]["protocolBinding"] == "HTTP+JSON"
print("Agent Card OK")
headers = {"Authorization": "Bearer " + token, "A2A-Version": "1.0"}
req = urllib.request.Request(base + "/a2a/tasks", headers=headers)
with urllib.request.urlopen(req, timeout=10) as r:
    listing = json.loads(r.read())
assert isinstance(listing.get("tasks"), list)
print("Authenticated task listing OK")
# A wrong version must fail without consuming any assignment check.
bad = urllib.request.Request(base + "/a2a/tasks", headers={"Authorization": "Bearer " + token, "A2A-Version": "0.9"})
try:
    urllib.request.urlopen(bad, timeout=10)
except urllib.error.HTTPError as e:
    assert e.code == 400
    print("Version rejection OK")
else:
    raise AssertionError("Expected 400 for wrong A2A-Version")
