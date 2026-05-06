#!/usr/bin/env python3
"""migrate_to_db.py — one-time migration from catalog.json to episodes.db.
Run on the 70 machine before restarting aiaware with the new server.py.
"""
import json, sqlite3, os, sys
from pathlib import Path

DB_DIR       = os.environ.get("AIAWARE_DATA_DIR", os.path.expanduser("~/aiaware-data"))
DB_FILE      = os.path.join(DB_DIR, "episodes.db")
CATALOG_FILE = os.environ.get("AIAWARE_CATALOG",
               os.path.expanduser("~/aiaware/catalog.json"))

if not os.path.exists(CATALOG_FILE):
    print(f"ERROR: catalog.json not found at {CATALOG_FILE}")
    sys.exit(1)

os.makedirs(DB_DIR, exist_ok=True)

KEYS = ['vid','title','upload_date','channel','channel_url','url','analysis',
        'status','feed_name','started_at','completed_at','updated_at']

episodes = json.loads(Path(CATALOG_FILE).read_text())
print(f"Found {len(episodes)} episodes in catalog.json")

con = sqlite3.connect(DB_FILE)
con.execute('''CREATE TABLE IF NOT EXISTS episodes (
    vid TEXT PRIMARY KEY,
    title TEXT,
    upload_date TEXT,
    channel TEXT,
    channel_url TEXT,
    url TEXT,
    analysis TEXT,
    status TEXT DEFAULT 'ready',
    feed_name TEXT,
    started_at TEXT,
    completed_at TEXT,
    updated_at TEXT
)''')
for ep in episodes:
    con.execute(
        f"INSERT OR REPLACE INTO episodes ({','.join(KEYS)}) VALUES ({','.join(':'+k for k in KEYS)})",
        {k: ep.get(k) for k in KEYS}
    )
con.commit()
n = con.execute("SELECT COUNT(*) FROM episodes").fetchone()[0]
con.close()
print(f"Migration complete: {n} episodes in {DB_FILE}")
