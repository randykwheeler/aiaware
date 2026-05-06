'use strict';

marked.setOptions({ gfm: true, breaks: false });

let activeVid = null;
let activeUrl = null;

function tsToSeconds(ts) {
  const m = ts.match(/(\d+):(\d+)/);
  return m ? parseInt(m[1]) * 60 + parseInt(m[2]) : 0;
}

function linkifyTimestamps(html, ytUrl) {
  if (!ytUrl) return html;
  const base = ytUrl.split('&t=')[0];
  return html.replace(/\[(\d{1,2}:\d{2})\]/g, (_, ts) => {
    const secs = tsToSeconds(ts);
    return `<a href="${base}&t=${secs}s" target="_blank" class="ts-link">${ts}</a>`;
  });
}

function setVideoEmbed(vid) {
  const wrap = document.getElementById('video-wrap');
  const iframe = document.getElementById('video-iframe');
  if (!vid) { wrap.classList.add('hidden'); return; }
  iframe.src = `https://www.youtube.com/embed/${vid}`;
  wrap.classList.remove('hidden');
}

function setPipeline(step, status) {
  const dots = {
    transcribe: document.querySelector('#ps-transcribe .p-dot'),
    analyze:    document.querySelector('#ps-analyze .p-dot'),
    highlights: document.querySelector('#ps-highlights .p-dot'),
  };
  Object.values(dots).forEach(d => { if (d) d.className = 'p-dot'; });
  if (status === 'running') {
    if (step === 'transcribing') { dots.transcribe?.classList.add('active'); }
    else { dots.transcribe?.classList.add('done'); dots.analyze?.classList.add('active'); }
  } else if (status === 'ready') {
    Object.values(dots).forEach(d => d?.classList.add('done'));
  } else if (status === 'error') {
    dots.transcribe?.classList.add(step === 'transcribing' ? 'error' : 'done');
    if (step !== 'transcribing') dots.analyze?.classList.add('error');
  }
}

function fmtDate(iso) {
  if (!iso) return '';
  try {
    // upload_date is YYYY-MM-DD, display as "May 6"
    if (/^\d{4}-\d{2}-\d{2}$/.test(iso)) {
      const [y, mo, d] = iso.split('-');
      return new Date(+y, +mo - 1, +d).toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
    }
    return new Date(iso).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' });
  } catch { return iso; }
}

function renderJob(job) {
  const loading    = document.getElementById('loading-state');
  const analysisEl = document.getElementById('analysis-content');
  const metaBar    = document.getElementById('meta-bar');
  const pill       = document.getElementById('status-pill');
  const btn        = document.getElementById('trigger-btn');

  const url   = job.url || (job.vid ? `https://www.youtube.com/watch?v=${job.vid}` : '#');
  activeUrl   = url;

  document.getElementById('video-title').textContent = job.title || job.vid || '';
  document.getElementById('video-date').textContent  = job.upload_date ? fmtDate(job.upload_date) : '';
  document.getElementById('video-link').href         = url;
  metaBar.classList.remove('hidden');

  setPipeline(job.step, job.status);

  if (job.status === 'ready' && job.analysis) {
    loading.classList.add('hidden');
    analysisEl.classList.remove('hidden');
    btn.disabled = false; btn.textContent = '▶ Run Now';

    if (activeVid !== job.vid) {
      activeVid = job.vid;
      let html = marked.parse(job.analysis);
      html = linkifyTimestamps(html, url);
      analysisEl.innerHTML = html;
    }
    setVideoEmbed(job.vid);

    const pillText = job.status;
    pill.textContent = pillText;
    pill.className = `pill ${job.status}`;

  } else if (job.status === 'error') {
    loading.classList.add('hidden');
    analysisEl.classList.remove('hidden');
    analysisEl.innerHTML = `<div class="error-state"><strong>Error:</strong><br><br>${job.error || 'Unknown'}</div>`;
    setVideoEmbed(job.vid);
    btn.disabled = false; btn.textContent = '▶ Retry';
    pill.textContent = 'error'; pill.className = 'pill error';

  } else {
    // running
    loading.classList.remove('hidden');
    analysisEl.classList.add('hidden');
    setVideoEmbed(job.vid);
    document.getElementById('loading-msg').textContent =
      job.step === 'transcribing' ? 'Transcribing with Whisper...' : 'Analyzing with gemma4...';
    document.getElementById('loading-sub').textContent = '';
    btn.disabled = true; btn.textContent = 'Running...';
    pill.textContent = job.step || 'running'; pill.className = 'pill running';
  }
}

function showIdle() {
  document.getElementById('loading-state').classList.remove('hidden');
  document.getElementById('analysis-content').classList.add('hidden');
  document.getElementById('meta-bar').classList.add('hidden');
  document.getElementById('video-wrap').classList.add('hidden');
  document.getElementById('status-pill').textContent = 'idle';
  document.getElementById('status-pill').className = 'pill';
  document.getElementById('trigger-btn').disabled = false;
  document.getElementById('trigger-btn').textContent = '▶ Run Now';
  setPipeline('', 'idle');
}

function renderEpisodes(episodes) {
  const list  = document.getElementById('episodes-list');
  const count = document.getElementById('ep-count');
  if (!list) return;
  count.textContent = episodes.length;
  if (!episodes.length) { list.innerHTML = '<p class="empty-msg">No episodes yet</p>'; return; }
  list.innerHTML = episodes.map(ep => {
    const isActive = ep.vid === activeVid;
    const dotCls = ep.status === 'error' ? 'error' : ep.status === 'running' ? 'running' : '';
    return `<div class="ep-item${isActive ? ' active' : ''}" data-vid="${ep.vid}" onclick="loadEpisode('${ep.vid}')">
      <div class="ep-date">
        <span class="ep-dot ${dotCls}"></span>
        ${ep.upload_date ? fmtDate(ep.upload_date) : 'Unknown date'}
      </div>
      <div class="ep-title">${ep.title || ep.vid}</div>
    </div>`;
  }).join('');
}

async function loadEpisode(vid) {
  if (vid === activeVid) return;
  try {
    const r = await fetch(`/api/episode/${vid}`);
    if (!r.ok) return;
    const job = await r.json();
    activeVid = null; // force re-render
    renderJob(job);
    // refresh episode list to update active highlight
    fetchEpisodes();
  } catch (e) { console.warn('loadEpisode:', e); }
}

async function fetchStatus() {
  try {
    const r = await fetch('/api/status');
    if (!r.ok) return;
    const data = await r.json();
    const timeEl = document.getElementById('server-time');
    if (data.server_time) timeEl.textContent = new Date(data.server_time).toLocaleTimeString();

    if (data.running) {
      // something is running — show it
      const job = data.job;
      if (job) renderJob(job);
    } else if (data.job && activeVid === null) {
      // initial load — show latest
      renderJob(data.job);
    } else if (!data.job && activeVid === null) {
      showIdle();
    }

    // if a running job just finished and it matches our active view, refresh
    if (data.job && data.job.vid === activeVid && data.job.status !== 'running') {
      const prev = document.getElementById('status-pill').className;
      if (prev.includes('running')) renderJob(data.job);
    }
  } catch (e) { console.warn('fetchStatus:', e); }
}

async function fetchEpisodes() {
  try {
    const r = await fetch('/api/episodes');
    if (!r.ok) return;
    renderEpisodes(await r.json());
  } catch (e) {}
}

document.getElementById('trigger-btn').addEventListener('click', async () => {
  const btn = document.getElementById('trigger-btn');
  btn.disabled = true; btn.textContent = 'Starting...';
  try {
    const r = await fetch('/api/trigger', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });
    const d = await r.json();
    if (!r.ok) {
      alert(d.error || 'Failed to trigger');
      btn.disabled = false; btn.textContent = '▶ Run Now';
    } else {
      // switch active view to the new job
      activeVid = null;
    }
  } catch (e) {
    alert('Failed: ' + e.message);
    btn.disabled = false; btn.textContent = '▶ Run Now';
  }
  fetchStatus();
  fetchEpisodes();
});

// Boot
fetchStatus();
fetchEpisodes();
setInterval(fetchStatus, 5000);
setInterval(fetchEpisodes, 15000);
