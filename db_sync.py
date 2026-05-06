#!/usr/bin/env python3
"""db_sync.py — called via SSH pipe; reads episode JSON from stdin, upserts into local episodes.db."""
import sys, json, sqlite3, os

DB_DIR  = os.environ.get("AIAWARE_DATA_DIR", os.path.expanduser("~/aiaware-data"))
DB_FILE = os.path.join(DB_DIR, "episodes.db")
os.makedirs(DB_DIR, exist_ok=True)

KEYS = ['vid','title','upload_date','channel','channel_url','url','analysis',
        'status','feed_name','started_at','completed_at','updated_at']

ep = json.loads(sys.stdin.read())
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
con.execute(
    f"INSERT OR REPLACE INTO episodes ({','.join(KEYS)}) VALUES ({','.join(':'+k for k in KEYS)})",
    {k: ep.get(k) for k in KEYS}
)
con.commit()
con.close()
print(f"OK: {ep.get('vid')}")
