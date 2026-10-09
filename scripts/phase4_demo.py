#!/usr/bin/env python3
"""Phase 4 demo: inject the three classic merchant bugs next to clean payments and watch Concord flag them.

Needs the whole system running (docker compose up, or the jars) with a short merchant checkout timeout
(MERCHANT_CHECKOUT_TIMEOUT_MS=4000) so the "order timed out" bug happens within seconds.

  1. missed webhook + checkout timeout -> PAID_BUT_CANCELLED   (capture webhook dropped, order cancelled at 4 s)
  2. redirect bug                      -> UNPAID_BUT_FULFILLED (shop trusts the browser and marks an authorized order paid)
  3. lost refund event                 -> REFUNDED_BUT_ACTIVE  (merchant stops accepting this order's events after the capture)

Usage: scripts/phase4_demo.py [simulator_url] [merchant_url] [concord_url]
"""
import json, sys, time, urllib.request

SIM, MER, CON = (sys.argv[1:4] + ["http://127.0.0.1:8081", "http://127.0.0.1:8090", "http://127.0.0.1:8080"][len(sys.argv[1:4]):])

def req(method, url, body=None):
    r = urllib.request.Request(url, data=None if body is None else json.dumps(body).encode(), method=method,
                               headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(r) as resp:
        t = resp.read()
        return json.loads(t) if t else None

run_id = str(int(time.time()))[-6:]
late, redirect, refund = f"pay_late_{run_id}", f"pay_redirect_{run_id}", f"pay_refund_{run_id}"
scenario = {"name": "phase4-demo", "seed": int(run_id), "timeScale": 10,
    "payments": [
        {"paymentId": late, "orderId": "order_" + late, "amount": 249900,
         "steps": [{"state": "AUTHORIZED", "atMs": 0}, {"state": "CAPTURED", "atMs": 60000}]},
        {"paymentId": redirect, "orderId": "order_" + redirect, "amount": 89900,
         "steps": [{"state": "AUTHORIZED", "atMs": 0}]},
        {"paymentId": refund, "orderId": "order_" + refund, "amount": 1499900,
         "steps": [{"state": "AUTHORIZED", "atMs": 0}, {"state": "CAPTURED", "atMs": 1000}, {"state": "REFUNDED", "atMs": 100000}]}],
    "directives": [{"paymentId": late, "state": "CAPTURED", "action": "DROP"}],
    "generate": {"count": 30, "spreadMs": 8000, "stepGapMs": 2000}}

plan = req("POST", SIM + "/sim/plan", scenario)
for p in plan["payments"]:   # the merchant creates its orders; each one registers its payment with Concord
    req("POST", MER + "/orders", {"orderId": p["orderId"], "gateway": "A", "paymentId": p["paymentId"],
                                  "amountMinor": p["amount"], "currency": "INR"})
t0 = time.time()
req("POST", SIM + "/sim/runs", scenario)
time.sleep(1)
req("POST", MER + f"/orders/order_{redirect}/mark-paid")       # bug 2
time.sleep(2)
req("POST", MER + f"/faults/reject-events/order_{refund}")     # bug 3

want = {("PAID_BUT_CANCELLED", late), ("UNPAID_BUT_FULFILLED", redirect), ("REFUNDED_BUT_ACTIVE", refund)}
seen = {}
while time.time() - t0 < 120 and not want <= set(seen):
    for i in req("GET", CON + "/api/incidents"):
        seen.setdefault((i["type"], i["paymentId"]), (time.time() - t0, i))
    time.sleep(0.5)

for (t, i) in sorted(seen.values(), key=lambda v: v[0]):
    print(f"t+{t:5.1f}s  {i['severity']:8} {i['type']:22} {i['paymentId']}: order {i['merchantStatus']}, gateway {i['gatewayState']}")
    print(f"           {i['explanation']}")
false_positives = [k for k in seen if k not in want]
print("missing:", sorted(want - set(seen)) or "none", "| incidents on clean payments:", false_positives or "none")
print("dashboard:", CON + "/")
sys.exit(0 if want <= set(seen) and not false_positives else 1)
