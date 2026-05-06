"""
backfill.py — Process last N videos from the AIAware channel sequentially.
Run from Windows: python D:/ai/aiaware/backfill.py
Calls the running server at localhost:11440.
"""
import urllib.request, urllib.error, json, time, sys

SERVER = "http://localhost:11440"

VIDEOS = [
    # Last 7 days — oldest first so history builds in order
    ("2026-04-30", "JvCtGjrn_N0", "Microsoft Is Testing Claude Against Its Own Copilot"),
    ("2026-05-01", "iUSdS-6uwr4", "RTX 5090, Mac Studio, or DGX Spark? I tried all three."),
    ("2026-05-02", "FDkvRl1RlT0", "Anthropic Might Buy Atlassian For $40B"),
    ("2026-05-03", "XGvDbeoSN3E", "Stripe, Visa, Mastercard, Microsoft, Meta. All Building The Same Thing"),
    ("2026-05-04", "rYqt6mMlv7o", "AI's 'Thin Ice' Moment: Is Your Job Already Gone?"),
    ("2026-05-05", "Z0HizICooiw", "Consumer AI Has a Problem Nobody's Naming."),
    # 2026-05-06 b1fxYGPbHeo already done
]

def get(path):
    req = urllib.request.Request(f"{SERVER}{path}")
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())

def post(path, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(f"{SERVER}{path}", data=data,
                                  headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())

def wait_for_completion(vid, timeout=600):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            ep = get(f"/api/episode/{vid}")
            status = ep.get("status")
            if status == "ready":
                return True
            if status == "error":
                print(f"  [!] error: {ep.get('error')}")
                return False
            print(f"  ... {ep.get('step', status)}", flush=True)
        except Exception as e:
            print(f"  ... polling ({e})", flush=True)
        time.sleep(10)
    print("  [!] timeout")
    return False

def main():
    print(f"AIAware backfill — {len(VIDEOS)} videos\n")

    for date, vid, title in VIDEOS:
        url = f"https://www.youtube.com/watch?v={vid}"

        # Skip if already done
        try:
            ep = get(f"/api/episode/{vid}")
            if ep.get("status") == "ready":
                print(f"[skip] {date} {vid} — already done")
                continue
        except Exception:
            pass  # not found, proceed

        print(f"[{date}] {vid} — {title[:60]}")
        try:
            resp = post("/api/trigger", {"url": url, "force": False})
            print(f"  triggered: {resp.get('message', resp)}")
        except urllib.error.HTTPError as e:
            body = json.loads(e.read())
            print(f"  [!] {body.get('error')}")
            # If something else is running, wait and retry
            if e.code == 409:
                print("  waiting for current job to finish...")
                time.sleep(15)
                try:
                    post("/api/trigger", {"url": url, "force": True})
                except Exception as e2:
                    print(f"  [!] retry failed: {e2}")
                    continue

        ok = wait_for_completion(vid)
        print(f"  {'done' if ok else 'FAILED'}\n")
        time.sleep(2)

    print("Backfill complete.")
    try:
        eps = get("/api/episodes")
        print(f"Total episodes in catalog: {len(eps)}")
        for ep in eps:
            print(f"  {ep.get('upload_date','')} {ep.get('vid','')} [{ep.get('status','')}] {ep.get('title','')[:60]}")
    except Exception as e:
        print(f"Could not list episodes: {e}")

if __name__ == "__main__":
    main()
