"""
aiaware/server.py — AI content awareness pipeline.
FastAPI on port 11440 (pipeline) or 10073 (reader/70-machine).

AIAWARE_MODE=pipeline  (default) — full pipeline: Whisper + Ollama + daily scheduler
AIAWARE_MODE=reader               — static reader: serves episodes.db pushed by pipeline machine
"""
import json, sys, re, os, threading, time, subprocess, sqlite3, urllib.request, urllib.error
from datetime import datetime, date
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

MODE = os.environ.get("AIAWARE_MODE", "pipeline")  # "pipeline" | "reader"
PORT = int(os.environ.get("AIAWARE_PORT", "11440" if MODE == "pipeline" else "10073"))

LOCAL_DIR    = Path("/mnt/d/ai/local")
PROJECT_DIR  = Path(os.environ.get("AIAWARE_DIR", "/mnt/d/ai/aiaware"))
OUTPUT_DIR   = LOCAL_DIR / "output"
FEEDS_FILE   = PROJECT_DIR / "feeds.json"

# SQLite — local DB (pipeline: in project dir; reader: in data dir set by env)
_data_dir    = os.environ.get("AIAWARE_DATA_DIR", str(PROJECT_DIR))
DB_FILE      = Path(_data_dir) / "episodes.db"

# Remote 70-machine sync (pipeline mode only)
REMOTE_HOST        = os.environ.get("AIAWARE_REMOTE_HOST",    "root@70.36.101.83")
REMOTE_SYNC_SCRIPT = os.environ.get("AIAWARE_SYNC_SCRIPT",    "/root/aiaware/db_sync.py")
REMOTE_SSH_KEY     = os.environ.get("AIAWARE_SSH_KEY",        "/home/rwheeler/.ssh/granbury_70")
REMOTE_READER_URL  = os.environ.get("AIAWARE_REMOTE_URL",     "https://aiaware.stinkhead.net")

WHISPER_PY    = Path("/home/rwheeler/whisper-4090-venv/bin/python")
TRANSCRIBE_PY = LOCAL_DIR / "transcribe_wsl.py"
OLLAMA_URL    = "http://localhost:11434/api/generate"
OLLAMA_MODEL  = "gemma4:e4b"

HIGHLIGHT_PROMPT = """\
You are an AI content intelligence analyst. Analyze this video transcript and produce a structured intelligence briefing.

Format your response EXACTLY in these sections using markdown:

## TL;DR
One or two sentences maximum. The single most important thing to know from this video.

## AI Tools & Technologies
List every AI model, tool, framework, product, or company mentioned. For each:
- **Name** — how it was discussed (announcement / criticism / comparison / hype / demo / pricing / etc.)

## Key Insights
The 5 most significant insights, claims, or findings. For each:
**[MM:SS]** **Title** — Specific explanation. Quote actual words from the transcript when the phrasing is important.

## Verbatim Quotes
2-3 exact direct quotes worth saving. Format:
> "[exact quote]" — [MM:SS]

## Red Flags & Caveats
Limitations, hype warnings, risks, important nuance, or contradictions raised in the video. Be blunt.

## Action Items
Concrete, specific things to do or investigate based on this content. Not generic advice.

Rules:
- Reference timestamps [MM:SS] from the timed transcript data — these will become clickable links
- For quotes: use the actual words, not paraphrases
- Flag opinion vs. fact when it matters
- Skip filler language: "it's worth noting", "essentially", "basically", "interestingly"
- If the video is thin on substance, say so plainly in TL;DR
"""

app = FastAPI(title="AIAware", version="3.0")
app.mount("/static", StaticFiles(directory=str(PROJECT_DIR / "static")), name="static")

_jobs: dict[str, dict] = {}
_latest_vid: Optional[str] = None
_running_vid: Optional[str] = None
_lock = threading.Lock()
_done_today: set[str] = set()

DB_KEYS = ['vid','title','upload_date','channel','channel_url','url','analysis',
           'status','feed_name','started_at','completed_at','updated_at']


class TriggerRequest(BaseModel):
    url: Optional[str] = None
    force: bool = False


# ── DB helpers ────────────────────────────────────────────────────────────────

def _init_db():
    DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(DB_FILE))
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
    con.commit()
    con.close()


def _upsert_episode(job: dict):
    con = sqlite3.connect(str(DB_FILE))
    con.execute(
        f"INSERT OR REPLACE INTO episodes ({','.join(DB_KEYS)}) "
        f"VALUES ({','.join(':'+k for k in DB_KEYS)})",
        {k: job.get(k) for k in DB_KEYS},
    )
    con.commit()
    con.close()


def _load_from_db() -> list[dict]:
    if not DB_FILE.exists():
        return []
    try:
        con = sqlite3.connect(str(DB_FILE))
        con.row_factory = sqlite3.Row
        rows = con.execute(
            "SELECT * FROM episodes WHERE status='ready' "
            "ORDER BY upload_date DESC, updated_at DESC"
        ).fetchall()
        con.close()
        return [dict(r) for r in rows]
    except Exception as e:
        print(f"[db] load error: {e}", flush=True)
        return []


def _sync_episode_remote(job: dict):
    """Push one episode to 70-machine SQLite via SSH + db_sync.py."""
    payload = json.dumps({k: job.get(k) for k in DB_KEYS}).encode()
    cmd = [
        "ssh", "-i", REMOTE_SSH_KEY,
        "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=10",
        REMOTE_HOST,
        f"python3 {REMOTE_SYNC_SCRIPT}",
    ]
    try:
        r = subprocess.run(cmd, input=payload, capture_output=True, timeout=20)
        if r.returncode == 0:
            print(f"[sync] {job.get('vid')} -> remote DB: {r.stdout.decode().strip()}", flush=True)
        else:
            print(f"[sync] WARNING: {r.stderr.decode().strip()}", flush=True)
    except Exception as e:
        print(f"[sync] WARNING: {e}", flush=True)


def _trigger_remote_reload():
    """Tell reader node to refresh _jobs from its DB via SSH localhost (bypasses Cloudflare)."""
    remote_port = os.environ.get("AIAWARE_REMOTE_PORT", "10073")
    cmd = [
        "ssh", "-i", REMOTE_SSH_KEY,
        "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=10",
        REMOTE_HOST,
        f"curl -sf -X POST http://localhost:{remote_port}/api/reload",
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if r.returncode == 0:
            print(f"[sync] remote reload: {r.stdout.strip()}", flush=True)
        else:
            print(f"[sync] reload WARNING: {r.stderr.strip()}", flush=True)
    except Exception as e:
        print(f"[sync] reload WARNING: {e}", flush=True)


# ── misc helpers ──────────────────────────────────────────────────────────────

def _load_feeds() -> list[dict]:
    if not FEEDS_FILE.exists():
        return []
    try:
        return json.loads(FEEDS_FILE.read_text())
    except Exception as e:
        print(f"[feeds] error: {e}", flush=True)
        return []


def _video_id(url: str) -> str:
    m = re.search(r"(?:v=|youtu\.be/|/shorts/)([A-Za-z0-9_-]{11})", url)
    return m.group(1) if m else re.sub(r"[^A-Za-z0-9_-]", "", url)[:20]


def _fmt_date(yyyymmdd: str) -> str:
    try:
        return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"
    except Exception:
        return yyyymmdd


def _srt_to_timed_text(srt_path: Path) -> str:
    if not srt_path.exists():
        return ""
    lines, ts_label = [], "[00:00]"
    for raw in srt_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if re.match(r"^\d+$", line):
            continue
        if "-->" in line:
            start = line.split("-->")[0].strip()
            parts = start.replace(",", ".").split(":")
            try:
                h, m, s = int(parts[0]), int(parts[1]), float(parts[2])
                total = int(h * 3600 + m * 60 + s)
                ts_label = f"[{total // 60:02d}:{total % 60:02d}]"
            except Exception:
                ts_label = "[??:??]"
        elif line:
            lines.append(f"{ts_label} {line}")
    return "\n".join(lines)


def _get_meta(vid: str) -> dict:
    info_path = LOCAL_DIR / "tmp" / f"{vid}.info.json"
    if info_path.exists():
        try:
            d = json.loads(info_path.read_text())
            return {
                "title": d.get("title", vid),
                "upload_date": _fmt_date(d.get("upload_date", "")),
                "channel": d.get("channel", ""),
                "channel_url": d.get("channel_url", ""),
            }
        except Exception:
            pass
    return {"title": vid, "upload_date": "", "channel": "", "channel_url": ""}


# ── pipeline ──────────────────────────────────────────────────────────────────

def _run_transcribe(url: str) -> tuple[bool, str, Optional[Path]]:
    vid = _video_id(url)
    cached_txt = OUTPUT_DIR / vid / f"{vid}.txt"
    cached_srt = OUTPUT_DIR / vid / f"{vid}.srt"
    if cached_txt.exists():
        print(f"[transcribe] cached: {cached_txt}", flush=True)
        return True, vid, cached_srt if cached_srt.exists() else None
    cmd = [str(WHISPER_PY), str(TRANSCRIBE_PY), url, "--lang", "en"]
    print(f"[transcribe] {url}", flush=True)
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if r.returncode != 0:
        print(f"[transcribe] FAILED:\n{r.stderr[-1000:]}", flush=True)
        return False, vid, None
    return True, vid, cached_srt if cached_srt.exists() else None


def _run_analysis(vid: str, srt_path: Optional[Path], url: str) -> tuple[bool, str]:
    txt_path = OUTPUT_DIR / vid / f"{vid}.txt"
    if not txt_path.exists():
        return False, "Transcript not found"

    cached = OUTPUT_DIR / vid / f"{vid}.aiaware.txt"
    if cached.exists():
        raw = cached.read_text(encoding="utf-8")
        analysis = raw.split("--- ANALYSIS ---", 1)[1].strip() if "--- ANALYSIS ---" in raw else raw
        print(f"[analyze] cached: {cached}", flush=True)
        return True, analysis

    transcript = txt_path.read_text(encoding="utf-8")
    timed = _srt_to_timed_text(srt_path) if srt_path else ""
    content = f"=== TIMED TRANSCRIPT ===\n{timed}\n\n=== FULL TRANSCRIPT ===\n{transcript}" if timed else transcript
    if len(content) > 28000:
        content = content[:28000] + "\n[truncated]"

    body = {
        "model": OLLAMA_MODEL,
        "prompt": f"{HIGHLIGHT_PROMPT}\n\n{content}",
        "stream": True, "keep_alive": 0,
        "options": {"num_ctx": 16384, "temperature": 0.3},
    }
    print(f"[analyze] calling {OLLAMA_MODEL}", flush=True)
    req = urllib.request.Request(OLLAMA_URL, data=json.dumps(body).encode(),
                                  headers={"Content-Type": "application/json"})
    parts = []
    try:
        with urllib.request.urlopen(req, timeout=600) as resp:
            for line in resp:
                line = line.strip()
                if not line:
                    continue
                chunk = json.loads(line).get("response", "")
                if chunk:
                    parts.append(chunk)
                    sys.stdout.write(chunk)
                    sys.stdout.flush()
    except Exception as e:
        return False, f"Ollama error: {e}"

    analysis = "".join(parts).strip()
    cached.write_text(
        f"URL: {url}\nMODEL: {OLLAMA_MODEL}\nDATE: {datetime.now().isoformat()}\n"
        f"PROMPT:\n{HIGHLIGHT_PROMPT}\n\n--- ANALYSIS ---\n{analysis}\n",
        encoding="utf-8",
    )
    print(f"\n[analyze] saved {cached}", flush=True)
    return True, analysis


def _process_job(url: str, feed_name: str = "manual"):
    global _latest_vid, _running_vid
    vid = _video_id(url)

    with _lock:
        _running_vid = vid
        _jobs[vid] = {
            **_jobs.get(vid, {}),
            "vid": vid, "url": url, "feed_name": feed_name,
            "status": "running", "step": "transcribing",
            "analysis": None, "error": None,
            "started_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
        }
        _latest_vid = vid

    try:
        ok, vid, srt_path = _run_transcribe(url)
        if not ok:
            raise RuntimeError("Transcription failed")

        with _lock:
            _jobs[vid]["step"] = "analyzing"
            _jobs[vid]["updated_at"] = datetime.now().isoformat()

        ok, analysis = _run_analysis(vid, srt_path, url)
        if not ok:
            raise RuntimeError(f"Analysis failed: {analysis}")

        meta = _get_meta(vid)
        with _lock:
            _jobs[vid].update({
                "status": "ready", "step": "done",
                "analysis": analysis,
                "title": meta["title"],
                "upload_date": meta["upload_date"],
                "channel": meta["channel"],
                "channel_url": meta.get("channel_url", ""),
                "updated_at": datetime.now().isoformat(),
                "completed_at": datetime.now().isoformat(),
            })
            _done_today.add(feed_name)
            _latest_vid = vid

        _upsert_episode(_jobs[vid])
        _sync_episode_remote(_jobs[vid])
        _trigger_remote_reload()

    except Exception as e:
        print(f"[job] ERROR: {e}", flush=True)
        with _lock:
            _jobs[vid].update({"status": "error", "error": str(e),
                                "updated_at": datetime.now().isoformat()})
    finally:
        with _lock:
            if _running_vid == vid:
                _running_vid = None


def _get_latest_channel_url(feed: dict) -> Optional[str]:
    channel_url = feed.get("url")
    if not channel_url:
        return None
    if re.search(r"(?:v=|youtu\.be/)([A-Za-z0-9_-]{11})", channel_url):
        return channel_url
    cmd = [str(WHISPER_PY), "-m", "yt_dlp",
           "--playlist-end", "1", "--dump-json", "--no-playlist", channel_url]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if r.returncode == 0 and r.stdout.strip():
            vid = json.loads(r.stdout.strip().splitlines()[0]).get("id")
            return f"https://www.youtube.com/watch?v={vid}" if vid else None
    except Exception as e:
        print(f"[scheduler] channel resolve failed: {e}", flush=True)
    return None


def _scheduler():
    while True:
        time.sleep(300)
        feeds = _load_feeds()
        today = date.today().isoformat()
        now = datetime.now()
        for feed in feeds:
            name = feed.get("name", "default")
            if now.hour < int(feed.get("schedule_hour", 10)):
                continue
            done_key = f"{name}:{today}"
            with _lock:
                if done_key in _done_today or _running_vid:
                    continue
            with _lock:
                already = any(
                    j.get("upload_date") == today and j.get("status") == "ready"
                    for j in _jobs.values()
                )
            if already:
                with _lock:
                    _done_today.add(done_key)
                continue
            url = _get_latest_channel_url(feed)
            if not url:
                continue
            print(f"[scheduler] triggering {name}", flush=True)
            with _lock:
                _done_today.add(done_key)
            threading.Thread(target=_process_job, args=(url, name), daemon=True).start()


def _startup_recover_pipeline():
    global _latest_vid
    _init_db()

    # Load what's already in the DB (fast path)
    existing = {ep["vid"]: ep for ep in _load_from_db()}
    _jobs.update(existing)

    # Scan .aiaware.txt files for anything not yet in DB
    new_jobs = []
    for apath in OUTPUT_DIR.glob("*/*.aiaware.txt"):
        vid = apath.parent.name
        if vid in existing:
            continue
        try:
            raw = apath.read_text(encoding="utf-8")
            url = next((l[5:].strip() for l in raw.splitlines()[:5] if l.startswith("URL: ")), "")
            analysis = raw.split("--- ANALYSIS ---", 1)[1].strip() if "--- ANALYSIS ---" in raw else raw
            meta = _get_meta(vid)
            mtime = apath.stat().st_mtime
            job = {
                "vid": vid, "url": url, "feed_name": "restored",
                "status": "ready", "step": "done",
                "analysis": analysis,
                "title": meta["title"], "upload_date": meta["upload_date"],
                "channel": meta["channel"], "channel_url": meta.get("channel_url", ""),
                "error": None,
                "started_at": datetime.fromtimestamp(mtime).isoformat(),
                "updated_at": datetime.fromtimestamp(mtime).isoformat(),
                "completed_at": datetime.fromtimestamp(mtime).isoformat(),
            }
            _jobs[vid] = job
            _upsert_episode(job)
            new_jobs.append(job)
        except Exception as e:
            print(f"[startup] skip {apath}: {e}", flush=True)

    if _jobs:
        _latest_vid = sorted(
            _jobs.keys(),
            key=lambda v: _jobs[v].get("upload_date", ""),
            reverse=True,
        )[0]

    print(f"[startup] pipeline: {len(existing)} from DB, {len(new_jobs)} new from disk, latest: {_latest_vid}", flush=True)

    # Sync any newly discovered episodes to remote
    for job in new_jobs:
        _sync_episode_remote(job)
    if new_jobs:
        _trigger_remote_reload()


def _startup_recover_reader():
    global _latest_vid
    _init_db()
    episodes = _load_from_db()
    for ep in episodes:
        vid = ep.get("vid")
        if vid:
            _jobs[vid] = ep
    if episodes:
        _latest_vid = episodes[0].get("vid")
    print(f"[startup] reader: {len(episodes)} episodes from DB", flush=True)


# ── startup ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
def startup():
    if MODE == "pipeline":
        _startup_recover_pipeline()
        threading.Thread(target=_scheduler, daemon=True).start()
    else:
        _startup_recover_reader()
    print(f"[startup] AIAware v3 mode={MODE} port={PORT}", flush=True)


# ── routes ────────────────────────────────────────────────────────────────────

@app.get("/")
def index():
    return FileResponse(str(PROJECT_DIR / "static" / "index.html"))


@app.get("/api/status")
def status():
    with _lock:
        job = _jobs.get(_latest_vid) if _latest_vid else None
        running = _running_vid if MODE == "pipeline" else None
    return {
        "job": job,
        "running": running,
        "feeds": _load_feeds() if MODE == "pipeline" else [],
        "server_time": datetime.now().isoformat(),
    }


@app.get("/api/episodes")
def episodes():
    with _lock:
        eps = [
            {k: v for k, v in j.items() if k != "analysis"}
            for j in _jobs.values()
            if j.get("status") in ("ready", "running", "error")
        ]
    eps.sort(key=lambda e: e.get("upload_date") or e.get("started_at", ""), reverse=True)
    return eps


@app.get("/api/episode/{vid}")
def episode(vid: str):
    with _lock:
        job = _jobs.get(vid)
    if not job:
        return JSONResponse({"error": "Not found"}, status_code=404)
    return job


@app.post("/api/trigger")
def trigger(req: TriggerRequest, background_tasks: BackgroundTasks):
    if MODE == "reader":
        return JSONResponse({"error": "This node is read-only"}, status_code=403)
    feeds = _load_feeds()
    url = req.url or (_get_latest_channel_url(feeds[0]) if feeds else None)
    if not url:
        return JSONResponse({"error": "No URL and no feeds configured"}, status_code=400)
    with _lock:
        if _running_vid and not req.force:
            return JSONResponse({"error": f"Already running: {_running_vid}"}, status_code=409)
    background_tasks.add_task(_process_job, url, "manual")
    return {"message": f"Triggered: {url}", "vid": _video_id(url)}


@app.post("/api/reload")
def reload_catalog():
    """Reader mode: reload _jobs from local DB (called after db_sync.py writes a new episode)."""
    if MODE == "pipeline":
        return JSONResponse({"error": "Not in reader mode"}, status_code=400)
    global _latest_vid
    episodes = _load_from_db()
    with _lock:
        _jobs.clear()
        for ep in episodes:
            vid = ep.get("vid")
            if vid:
                _jobs[vid] = ep
        _latest_vid = episodes[0].get("vid") if episodes else None
    return {"loaded": len(episodes)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
