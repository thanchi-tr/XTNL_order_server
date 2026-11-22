/* Live view page (docs/design/live-view.md v9 §1, §4.8; data shapes: docs/design/live-view-contract.md).
   Read-only: it fetches GET /api/snapshot, GET /api/story and listens on /api/stream. It has no write path.
   The DOM is built with createElement / createElementNS / textContent only. Every value shown comes from the
   contract fields; the page never computes or shows a distance, a size or a money figure. */
(function () {
  'use strict';

  var SVGNS = 'http://www.w3.org/2000/svg';
  var SEC = 1000, MIN = 60000, HOUR = 3600000, DAYMS = 86400000;
  var RING_MAX = 6000;
  var FEED_MAX = 300;
  var NOT_SHOWN = '(not shown)';

  /* ===================== helpers ===================== */
  function $(id) { return document.getElementById(id); }
  function el(tag, cls, text) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (text !== undefined && text !== null) n.textContent = String(text);
    return n;
  }
  function sv(tag, attrs, text) {
    var n = document.createElementNS(SVGNS, tag);
    if (attrs) {
      Object.keys(attrs).forEach(function (k) {
        if (attrs[k] !== undefined && attrs[k] !== null) n.setAttribute(k, String(attrs[k]));
      });
    }
    if (text !== undefined && text !== null) n.textContent = String(text);
    return n;
  }
  function txt(s) { return document.createTextNode(String(s)); }
  function clear(n) { while (n.firstChild) n.removeChild(n.firstChild); }
  function has(o, k) { return o !== null && typeof o === 'object' && Object.prototype.hasOwnProperty.call(o, k); }
  function isNum(x) { return typeof x === 'number' && isFinite(x); }
  function str(x) { return typeof x === 'string' ? x : (x === null || x === undefined ? '' : String(x)); }
  function pad(n) { return (n < 10 ? '0' : '') + n; }
  function plural(n, one, many) { return n + ' ' + (n === 1 ? one : (many || one + 's')); }
  function fmtLocal(ms, withSec) {
    var d = new Date(ms);
    return pad(d.getHours()) + ':' + pad(d.getMinutes()) + (withSec === false ? '' : ':' + pad(d.getSeconds()));
  }
  function fmtUtc(ms) {
    var d = new Date(ms);
    return d.getUTCFullYear() + '-' + pad(d.getUTCMonth() + 1) + '-' + pad(d.getUTCDate()) + ' ' +
      pad(d.getUTCHours()) + ':' + pad(d.getUTCMinutes()) + ':' + pad(d.getUTCSeconds()) + ' UTC';
  }
  /* a local time, with UTC in the title attribute */
  function timeEl(ms, cls, withSec, prefix) {
    var s = el('span', cls || 't5', (prefix || '') + (isNum(ms) ? fmtLocal(ms, withSec) : 'unknown'));
    if (isNum(ms)) s.title = fmtUtc(ms);
    return s;
  }
  function dur(ms) {
    var s = Math.max(0, Math.round(ms / SEC));
    if (s < 60) return s + ' s';
    var m = Math.floor(s / 60);
    if (m < 60) return m + ' min';
    var h = Math.floor(m / 60);
    return h + ' h ' + (m % 60) + ' min';
  }
  function ago(ms) { return isNum(ms) ? dur(Date.now() - ms) + ' ago' : 'never'; }
  function level(v) { return typeof v === 'string' && v !== '' ? v : null; }
  function levelNum(v) { var f = parseFloat(v); return isFinite(f) ? f : null; }

  /* running text with ids (6+ digits that are not part of a decimal level) set in mono */
  var ID_RE = /(^|[^\d.])(\d\d\d\d\d\d+)(?![\d.])/g;
  function richText(parent, s) {
    s = str(s);
    var last = 0, m;
    ID_RE.lastIndex = 0;
    while ((m = ID_RE.exec(s)) !== null) {
      var start = m.index + m[1].length;
      if (start > last) parent.appendChild(txt(s.slice(last, start)));
      parent.appendChild(el('span', 'idt', m[2]));
      last = start + m[2].length;
    }
    if (last < s.length) parent.appendChild(txt(s.slice(last)));
    return parent;
  }

  /* icons: static path data only */
  var ICONS = {
    halt: ['M5.5 1.5h5l4 4v5l-4 4h-5l-4-4v-5z', 'M5 8h6'],
    flag: ['M3.5 14.5v-13', 'M3.5 2h9l-2 3 2 3h-9'],
    engine: ['M1 8.5h3l2-5 3.5 9 2-6 1.2 2H15'],
    dlq: ['M1.5 9l2-6.5h9l2 6.5v5h-13z', 'M1.5 9h4l1 2h3l1-2h4'],
    streams: ['M8 12.5v.01', 'M5.2 9.8a4 4 0 0 1 5.6 0', 'M2.6 7.2a7.6 7.6 0 0 1 10.8 0'],
    trade: ['M1.5 12.5l4-4.5 3 3 6-7', 'M10.5 4h4v4'],
    orders: ['M3 2.5h10v11H3z', 'M5.5 5.5h5', 'M5.5 8h5', 'M5.5 10.5h3'],
    account: ['M2 4.5h12v8H2z', 'M2 7h12'],
    outage: ['M1.5 8h4', 'M10.5 8h4', 'M6.5 4.5l3 7'],
    power: ['M8 1.5v6', 'M4.6 4a5.2 5.2 0 1 0 6.8 0'],
    bell: ['M4 11V7a4 4 0 0 1 8 0v4l1.5 1.5h-11z', 'M6.5 14h3'],
    user: ['M8 2.5a2.5 2.5 0 1 0 0 5 2.5 2.5 0 1 0 0-5z', 'M3 14c.6-2.8 2.6-4.5 5-4.5s4.4 1.7 5 4.5'],
    refresh: ['M13 3v3.5H9.5', 'M13 6.5A5.5 5.5 0 1 0 13.5 9'],
    chevron: ['M4 6l4 4 4-4'],
    pause: ['M5 3v10', 'M11 3v10'],
    play: ['M4.5 2.5v11l9-5.5z'],
    search: ['M6.5 2a4.5 4.5 0 1 0 0 9 4.5 4.5 0 1 0 0-9z', 'M10 10l4 4'],
    link: ['M6.5 9.5l3-3', 'M7 4.5l1.5-1.5a2.8 2.8 0 0 1 4 4L11 8.5', 'M9 11.5l-1.5 1.5a2.8 2.8 0 0 1-4-4L5 7.5'],
    ok: ['M3 8.5l3 3 7-7'],
    x: ['M4.5 4.5l7 7', 'M11.5 4.5l-7 7'],
    tri: ['M8 2.5l6 11H2z'],
    ring: ['M8 3.5a4.5 4.5 0 1 0 0 9 4.5 4.5 0 1 0 0-9z']
  };
  function icon(name, size) {
    var s = sv('svg', { viewBox: '0 0 16 16', width: size || 14, height: size || 14, 'aria-hidden': 'true', 'class': 'ic',
      fill: 'none', stroke: 'currentColor', 'stroke-width': 1.6, 'stroke-linecap': 'round', 'stroke-linejoin': 'round' });
    (ICONS[name] || []).forEach(function (d) { s.appendChild(sv('path', { d: d })); });
    return s;
  }
  function btn(cls, child) {
    var b = el('button', cls);
    b.type = 'button';
    if (child) b.appendChild(typeof child === 'string' ? el('span', 't5', child) : child);
    return b;
  }
  function badge(text, tone, wrap) {
    return el('span', 'badge t5 s' + (tone ? ' ' + tone : '') + (wrap ? ' wrap' : '') + (tone ? ' tone-' + tone : ''), text);
  }
  function clickable(node, fn, label) {
    node.setAttribute('role', 'button');
    node.tabIndex = 0;
    if (label) node.setAttribute('aria-label', label);
    node.addEventListener('click', fn);
    node.addEventListener('keydown', function (ev) {
      if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); fn(ev); }
    });
  }

  /* ===================== taxonomy ===================== */
  var CATS = ['orders', 'positions', 'risk', 'halt', 'alerts', 'operator', 'dlq', 'engine', 'streams', 'catchup'];
  var CAT_ICON = { orders: 'orders', positions: 'trade', risk: 'flag', halt: 'halt', alerts: 'bell', operator: 'user',
    dlq: 'dlq', engine: 'engine', streams: 'streams', catchup: 'refresh' };
  var KIND = {
    trade: { label: 'Trade story', icon: 'trade', cat: 'positions' },
    account: { label: 'Account rule story', icon: 'account', cat: 'risk' },
    halt: { label: 'Halt story', icon: 'halt', cat: 'halt' },
    outage: { label: 'Outage story', icon: 'outage', cat: 'streams' },
    engine: { label: 'Engine start story', icon: 'power', cat: 'engine' },
    dlq: { label: 'DLQ story', icon: 'dlq', cat: 'dlq' }
  };
  var SEV_RANK = { debug: 0, info: 1, notice: 2, warn: 3, alert: 4 };
  var OPEN_STATES = { open: 1, active: 1, lost: 1, starting: 1, queued: 1, quarantined: 1 };
  var BADGE_TEXT = { delivered: ['delivered ✓', 'ok'], queued: ['queued', 'info'], retrying: ['retrying', 'warn'],
    suppressed: ['suppressed (duplicate)', ''], undeliverable: ['NOT delivered', 'alert'], escalated: ['escalated', 'warn'] };
  function levelTone(lv) {
    if (lv === 'HARD') return 'alert';
    if (lv === 'SOFT' || lv === 'FLAG') return 'warn';
    if (lv === 'NONE') return 'ok';
    return 'off';
  }
  function sevTone(sev) {
    return sev === 'alert' ? 'alert' : sev === 'warn' ? 'warn' : sev === 'notice' ? 'info' : '';
  }

  /* Glossary: one sentence per reason / rule code (src/oversee rules, sweep and halt; src/pubsub/dlq_rows.py).
     An unknown code is shown as the code itself. */
  var GLOSSARY = {
    'missing_stop_loss': 'An open position or working entry order has had no stop-loss for longer than the grace period.',
    'max_position_risk': 'The risk of one position or order, measured to its stop, is above the per-trade limit.',
    'max_aggregate_open_risk': 'The combined risk of every open position and working order is above the account limit.',
    'max_daily_loss': 'The loss on the account since the start of the trading day is beyond the daily limit.',
    'margin_usage': 'The share of the account used as margin is above the allowed band.',
    'stale_account_image': 'The stored account state may be behind the broker (a stream is fenced or an incident is open), so results are flagged.',
    'stale_mark': 'A symbol held or worked is valued at its last known mid: no fresh quote has arrived, or its market is closed.',
    'risk_unknown': 'Risk could not be worked out because an input is missing (for example the instrument, the units or the account row).',
    'mark_unknown': 'No usable mid was available to value the position, so its risk is unknown.',
    'live_unknown': 'Live account figures were missing or only sampled, so the rule could not run.',
    'stop_kind_inconsistent': 'A stop child sits on the wrong side of the market: touched and awaiting its fill, or a mislabelled take-profit.',
    'stop_kind_unknown': 'A linked child order has no stop or limit kind, so it may or may not be the stop.',
    'position_risk': 'The risk of the position or order to its stop, compared with the per-trade bands.',
    'aggregate_open_risk': 'The combined risk of every open position and working order, compared with the account bands.',
    'max_deliveries': 'An Oversee feed entry was delivered too many times without being handled, so it was set aside.',
    'malformed_entry': 'An Oversee feed entry could not be read, so it was set aside.',
    'no_stop': 'No stop-loss is set, so no risk to a stop can be measured; the missing-stop rule covers it.',
    'no_stop_needed': 'This order or position does not need a stop-loss (for example a closing order).',
    'stop_present': 'A stop-loss is in place.',
    'stop_grace': 'No stop-loss yet, but the grace period has not run out.',
    'account_image_current': 'The stored account state is current.',
    'marks_current': 'Every mark in use is current.',
    'daily_loss': 'The loss on the account since the start of the trading day is beyond the daily limit.',
    'daily_loss(partial_session)': 'The daily loss is beyond the limit, but the start-of-day record is partial, so the halt is capped at SOFT.',
    'daily_loss(stale_mark)': 'The daily loss is beyond the limit, but it was valued with stale marks, so the halt is capped at SOFT.',
    'daily_loss(balance_skew)': 'The daily loss is beyond the limit, but the balance may not yet include a recent fill, so the halt is capped at SOFT.',
    'partial_session': 'The start-of-day record is incomplete, so the daily figure may be off.',
    'sod_partial_session': 'The start-of-day record for this account is partial.',
    'balance_skew': 'A recent fill or change is newer than the last committed balance, so the balance may be behind.',
    'unexplained_balance_delta': 'The balance rose by an amount no recorded trade or transfer explains; it is left out of the equity mark.',
    'pnl_unverified': 'Today\'s result could not be checked against a second source.',
    'transfers_unverified': 'Today\'s deposits and withdrawals could not be checked against the broker.',
    'equity_divergence': 'The engine\'s equity mark and the broker\'s live figure disagree by more than the allowed share.',
    'evaluation': 'A synthetic rule held while the account cannot be evaluated.',
    'evaluation_failed': 'The risk sweep could not evaluate this account, so a capped SOFT state is held until it can.',
    'escalation': 'A SOFT breach stayed uncapped for too long and raised the halt to HARD.',
    'latch': 'The hard latch keeps the halt at HARD until an operator clears it.',
    'cool_down': 'Every breach stayed below SOFT for the cool-down, so the halt cleared itself.',
    'illegal_transition': 'The update would move the order or position through a state change that is not allowed, so it was parked.',
    'schema_invalid': 'The update did not have the expected shape, so it was parked.',
    'checksum': 'The update failed its checksum, so it was parked.',
    'future_timestamp': 'The update carries a time too far in the future, so it was parked.',
    'QueueFull': 'The event bus queue was full, so the update was parked for a retry.',
    'missing_client_order_id': 'The update has no client order id where one is required, so it was parked.'
  };
  var COMPOSITE_RE = /^([A-Za-z_]+)\(([A-Za-z_,]+)\)$/;
  function gloss(code) {
    code = str(code);
    if (!code) return '';
    if (has(GLOSSARY, code)) return GLOSSARY[code];
    var m = COMPOSITE_RE.exec(code);
    if (m && has(GLOSSARY, m[1])) {
      return GLOSSARY[m[1]] + ' Capped at SOFT because of: ' + m[2].split(',').join(', ') + '.';
    }
    return code;
  }

  /* ===================== model ===================== */
  var M = { instance: null, events: {}, ns: [], stories: {}, merges: {}, state: null, day: null, series: null, poll: null };
  var UI = {
    cats: {}, q: '', paused: false, pausedCount: 0, hk: false, expanded: {}, inactiveOpen: false,
    page: 'connecting', attempt: 0, slow: false, drawerKey: null, haveSnapshot: false
  };

  function resetModel(snap) {
    M.instance = str(snap.instance) || null;
    M.events = {};
    M.ns = [];
    M.stories = {};
    M.merges = {};
    (Array.isArray(snap.events) ? snap.events : []).forEach(addRingEntry);
    (Array.isArray(snap.stories) ? snap.stories : []).forEach(putStory);
    /* merges still in the ring, applied after the story headers, idempotently like live story_merge events */
    (Array.isArray(snap.merges) ? snap.merges : []).forEach(addMerge);
    M.state = snap.state || null;
    M.day = snap.day || null;
    M.series = snap.series || null;
    applyPoll(snap.poll || null);
  }
  function isMerge(o) { return o && typeof o === 'object' && has(o, 'from') && has(o, 'into') && !has(o, 'kind'); }
  function addRingEntry(o) {
    if (isMerge(o)) { addMerge(o); return; }
    addEvent(o);
  }
  function insertN(n) {
    var ns = M.ns;
    if (!ns.length || n > ns[ns.length - 1]) { ns.push(n); return; }
    var i = ns.length;
    while (i > 0 && ns[i - 1] > n) i--;
    ns.splice(i, 0, n);
  }
  function addEvent(ev) {
    if (!ev || !isNum(ev.n) || has(M.events, ev.n)) return false;
    M.events[ev.n] = ev;
    insertN(ev.n);
    while (M.ns.length > RING_MAX) { delete M.events[M.ns.shift()]; }
    return true;
  }
  /* story_merge is applied idempotently: a second copy of the same merge changes nothing */
  function addMerge(d) {
    if (!d || typeof d.from !== 'string' || typeof d.into !== 'string' || d.from === d.into) return;
    M.merges[d.from] = { into: d.into, held: d.held === true };
  }
  function putStory(h) {
    if (!h || typeof h.key !== 'string') return;
    M.stories[h.key] = h;
    if (typeof h.merged_into === 'string' && h.merged_into && h.merged_into !== h.key) {
      M.merges[h.key] = { into: h.merged_into, held: h.held === true };
    }
  }
  /* the story a key is displayed in, after merges; held = a "held for catch-up" fold was crossed */
  function resolve(key) {
    var held = false, seen = {}, k = key, steps = 0;
    while (has(M.merges, k) && !seen[k] && steps < 20) {
      seen[k] = 1;
      if (M.merges[k].held) held = true;
      k = M.merges[k].into;
      steps++;
    }
    return { key: k, held: held };
  }
  function applyPoll(p) {
    M.poll = p;
  }

  /* ===================== theme (per-browser convenience; the page works without storage) ===================== */
  function readTheme() {
    try {
      return window.localStorage.getItem('lv-theme') === 'light' ? 'light' : 'dark';
    } catch (e) {
      return 'dark';
    }
  }
  function saveTheme(v) {
    try {
      window.localStorage.setItem('lv-theme', v);
    } catch (e) {
      /* storage blocked: the choice lasts for this page view only */
    }
  }
  var theme = 'dark';
  function applyTheme(v) {
    theme = v === 'light' ? 'light' : 'dark';
    document.documentElement.setAttribute('data-theme', theme);
    var t = $('themeTxt');
    if (t) t.textContent = 'Theme: ' + theme;
  }

  /* ===================== render scheduling ===================== */
  var dirty = {}, flushPending = false;
  function mark() {
    for (var i = 0; i < arguments.length; i++) dirty[arguments[i]] = true;
    if (flushPending) return;
    flushPending = true;
    var raf = window.requestAnimationFrame || function (f) { return setTimeout(f, 16); };
    raf(flush);
  }
  function flush() {
    flushPending = false;
    var d = dirty;
    dirty = {};
    if (d.status) { renderBanner(); renderTicker(); renderTiles(); renderPath(); updateConn(); }
    if (d.day) renderDay();
    if (d.controls) renderControls();
    if (d.feed && !UI.paused) renderFeed();
    if (d.console) { renderOpenNow(); renderBreaches(); }
    if (d.health) renderHealth();
  }
  function markAll() { mark('status', 'day', 'controls', 'feed', 'console', 'health'); }

  /* ===================== snapshot and stream ===================== */
  var es = null, openedAt = 0, backoff = SEC, retryTimer = null, loading = false;

  /* after = snapshot.last_n (n of the newest ring entry, activity or story_merge; 0 when the ring is empty) */
  function lastRingN(snap) {
    if (isNum(snap.last_n)) return snap.last_n;
    var list = Array.isArray(snap.events) ? snap.events : [];
    var last = list.length ? list[list.length - 1] : null;
    return last && isNum(last.n) ? last.n : 0;
  }
  function retryLater() {
    if (retryTimer) return;
    if (openedAt && Date.now() - openedAt >= MIN) backoff = SEC;
    openedAt = 0;
    var wait = backoff;
    backoff = Math.min(backoff * 2, 30 * SEC);
    retryTimer = setTimeout(function () { retryTimer = null; loadSnapshot(); }, wait);
  }
  function loadSnapshot() {
    if (loading) return;
    loading = true;
    var slowTimer = setTimeout(function () { UI.slow = true; renderSlow(); }, 5 * SEC);
    fetch('/api/snapshot', { cache: 'no-store', credentials: 'same-origin' })
      .then(function (r) {
        if (!r.ok) throw new Error('snapshot ' + r.status);
        return r.json();
      })
      .then(function (snap) {
        clearTimeout(slowTimer);
        loading = false;
        UI.slow = false;
        renderSlow();
        if (!snap || typeof snap !== 'object' || typeof snap.instance !== 'string') throw new Error('snapshot shape');
        resetModel(snap);
        UI.haveSnapshot = true;
        markAll();
        openStream(snap);
      })
      .catch(function () {
        clearTimeout(slowTimer);
        loading = false;
        UI.slow = false;
        renderSlow();
        UI.page = 'down';
        UI.attempt++;
        mark('status');
        retryLater();
      });
  }
  function parseData(e) {
    try { return JSON.parse(e.data); } catch (err) { return undefined; }
  }
  /* opened only from loadSnapshot, with the instance and the last ring number of the snapshot just rendered */
  function openStream(snap) {
    if (es) { es.close(); es = null; }
    var url = '/api/stream?instance=' + encodeURIComponent(snap.instance) + '&after=' + encodeURIComponent(String(lastRingN(snap)));
    var src = new EventSource(url);
    es = src;
    openedAt = Date.now();
    UI.page = 'reconnecting';
    mark('status');
    function on(name, fn) {
      src.addEventListener(name, function (e) {
        if (es !== src) return;
        var d = parseData(e);
        if (d === undefined || d === null) return;
        fn(d);
      });
    }
    on('hello', function (d) {
      if (d.instance !== M.instance) {
        /* another server process answered: start again from a fresh snapshot */
        src.close();
        es = null;
        UI.page = 'reconnecting';
        mark('status');
        retryLater();
        return;
      }
      UI.page = 'live';
      UI.attempt = 0;
      mark('status');
    });
    on('activity', function (d) {
      if (addEvent(d)) {
        if (UI.paused) { UI.pausedCount++; mark('controls'); }
        mark('feed', 'controls');
      }
    });
    on('story_merge', function (d) { addMerge(d); mark('feed'); });
    on('state', function (d) { M.state = d; mark('status', 'console'); });
    on('story', function (d) { putStory(d); mark('feed', 'controls'); if (UI.drawerKey === d.key) refreshDrawer(); });
    on('day', function (d) { M.day = d; mark('day'); });
    on('series', function (d) { M.series = d; mark('health'); });
    on('poll', function (d) { applyPoll(d); mark('status'); });
    src.addEventListener('resync', function () {
      src.close();
      if (es !== src) return;
      es = null;
      UI.page = 'reconnecting';
      mark('status');
      retryLater();
    });
    src.onerror = function () {
      src.close();
      if (es !== src) return;
      es = null;
      UI.page = 'reconnecting';
      mark('status');
      retryLater();
    };
  }

  /* ===================== banners (one at a time, §1.1 order) ===================== */
  function bannerInfo() {
    if (UI.page === 'down') {
      return { tone: '', text: 'Live view server not reachable; retrying (attempt ' + UI.attempt + ').' };
    }
    var p = M.poll;
    if (!p) return null;
    var t = isNum(p.at_ms) ? fmtLocal(p.at_ms) : 'an unknown time';
    if (p.db === 'down' && p.engine === 'down') {
      return { tone: '', text: 'Database and engine both unreachable since ' + t + '. Showing the state as of ' + t + '.', at: p.at_ms };
    }
    if (p.db === 'down') {
      return { tone: '', text: 'Database unreachable since ' + t + '. The engine cannot record anything or queue alerts while it is down. Showing the state as of ' + t + '.', at: p.at_ms };
    }
    if (p.engine === 'down') {
      var e = M.state && M.state.engine;
      var lo = e && isNum(e.last_ok_ms) ? e.last_ok_ms : null;
      return { tone: 'warn', text: 'Engine not responding since ' + (lo ? fmtLocal(lo) : 'an unknown time') + ' (last proof of life).', at: lo };
    }
    return null;
  }
  function renderBanner() {
    var b = $('banner');
    var info = bannerInfo();
    clear(b);
    if (!info) { b.hidden = true; return; }
    b.className = 'banner' + (info.tone ? ' ' + info.tone : '');
    b.appendChild(el('span', 'sq ' + (info.tone || 'alert')));
    var t = el('span', 't4', info.text);
    if (isNum(info.at)) t.title = fmtUtc(info.at);
    b.appendChild(t);
    b.hidden = false;
  }
  function renderSlow() {
    var s = $('slow');
    clear(s);
    if (!UI.slow) { s.hidden = true; return; }
    s.appendChild(el('span', 't4 tone-warn', 'The snapshot is taking more than 5 s: too many live-view tabs open in this browser?'));
    s.hidden = false;
  }
  function engineDown() { return !!(M.poll && M.poll.engine === 'down'); }

  /* ===================== status ticker and tiles ===================== */
  function haltLevel() {
    var h = M.state && M.state.halt;
    if (!h) return null;
    return str(h.effective_level) || str(h.level) || null;
  }
  function pageWord() {
    if (UI.page === 'live') return ['live', 'ok'];
    if (UI.page === 'down') return ['server down', 'alert'];
    if (UI.page === 'reconnecting') return ['reconnecting', 'warn'];
    return ['connecting', 'warn'];
  }
  function streamWord(s) {
    switch (s.status) {
      case 'live': return ['live', 'ok'];
      case 'lost': return ['lost', 'alert'];
      case 'fenced': return ['fenced', 'warn'];
      case 'inactive': return ['inactive', 'off'];
      default: return ['unknown', 'off'];
    }
  }
  function renderTicker() {
    var host = $('ticker');
    var keepX = host.scrollLeft;
    clear(host);
    function item(label) {
      var s = el('span', 'tk');
      s.appendChild(el('span', 't5 s', label));
      host.appendChild(s);
      return s;
    }
    function val(parent, text, tone) { parent.appendChild(el('span', 't5 v' + (tone ? ' tone-' + tone : ''), text)); }
    var st = M.state || {};
    var lv = haltLevel();
    val(item('HALT'), lv || 'unknown', levelTone(lv));
    var ld = st.leader;
    val(item('LEADER'), ld ? str(ld.node) + ' / epoch ' + ld.epoch : 'none', ld ? '' : 'off');
    var eg = st.engine;
    val(item('ENGINE'), eg ? (str(eg.role) || str(eg.status) || 'unknown') : 'unknown', eg && eg.status === 'running' ? 'ok' : eg && (eg.status === 'down' || eg.status === 'stalled') ? 'alert' : 'off');
    var streams = Array.isArray(st.streams) ? st.streams.filter(function (s) { return s.status !== 'inactive'; }) : [];
    if (streams.length) {
      var sk = item('STREAMS');
      streams.forEach(function (s) {
        var w = streamWord(s);
        sk.appendChild(el('span', 't5', str(s.adapter) + ' ' + str(s.stream)));
        val(sk, w[0], w[1]);
      });
    }
    var p = M.poll;
    val(item('LAST POLL'), p && isNum(p.at_ms) ? ago(p.at_ms) : 'none', p && p.db === 'down' ? 'alert' : '');
    var pw = pageWord();
    val(item('PAGE'), pw[0], pw[1]);
    host.scrollLeft = keepX;
  }

  /* ===================== data path bar (the site's pipeline bar): streams -> engine -> database -> page ==== */
  var PATH_GLYPH = { done: 'ok', attention: 'tri', blocked: 'x', todo: 'ring' };
  function pathSteps() {
    var st = M.state || {}, p = M.poll || {};
    var steps = [];
    var streams = Array.isArray(st.streams) ? st.streams.filter(function (s) { return s.status !== 'inactive'; }) : [];
    var lost = streams.filter(function (s) { return s.status === 'lost'; }).length;
    var fenced = streams.filter(function (s) { return s.status === 'fenced'; }).length;
    if (!streams.length) steps.push(['Broker streams', 'todo', 'none read']);
    else if (lost) steps.push(['Broker streams', 'blocked', lost + ' lost']);
    else if (fenced) steps.push(['Broker streams', 'attention', fenced + ' fenced']);
    else steps.push(['Broker streams', 'done', streams.length + ' live']);
    var eg = st.engine || {};
    if (p.engine === 'off') steps.push(['Engine', 'todo', 'probe off']);
    else if (p.engine === 'down' || eg.status === 'down') steps.push(['Engine', 'blocked', 'not responding']);
    else if (eg.status === 'stalled') steps.push(['Engine', 'attention', 'RiskSweep stalled']);
    else if (eg.status === 'running') steps.push(['Engine', 'done', str(eg.role) || 'running']);
    else steps.push(['Engine', 'todo', 'unknown']);
    if (p.db === 'ok') steps.push(['Database', 'done', 'reading']);
    else if (p.db === 'down') steps.push(['Database', 'blocked', 'unreachable']);
    else steps.push(['Database', 'todo', 'not read yet']);
    var pw = pageWord();
    steps.push(['This page', pw[1] === 'ok' ? 'done' : pw[1] === 'alert' ? 'blocked' : 'attention', pw[0]]);
    return steps;
  }
  function renderPath() {
    var host = $('path');
    if (!host) return;
    clear(host);
    var steps = pathSteps();
    var worst = steps.some(function (x) { return x[1] === 'blocked'; }) ? 'alert'
      : steps.some(function (x) { return x[1] === 'attention'; }) ? 'warn' : 'ok';
    var lead = el('span', 'pb-lead');
    lead.appendChild(el('span', 'pb-dot ' + worst));
    lead.appendChild(el('span', 't5 s', 'DATA PATH'));
    host.appendChild(lead);
    var list = el('ol', 'pb-steps');
    steps.forEach(function (x, i) {
      var li = el('li', 'pstep ' + x[1]);
      li.appendChild(icon(PATH_GLYPH[x[1]], 11));
      li.appendChild(el('span', 't5 s pname', x[0]));
      li.appendChild(el('span', 't5 s pdet', x[2]));
      if (i) { var ar = el('li', 'parr t5 s', '→'); ar.setAttribute('aria-hidden', 'true'); list.appendChild(ar); }
      list.appendChild(li);
    });
    host.appendChild(list);
  }

  function tile(label, iconName, extraCls) {
    var t = el('div', 'tile' + (extraCls ? ' ' + extraCls : ''));
    var k = el('div', 't2');
    if (iconName) k.appendChild(icon(iconName, 12));
    k.appendChild(txt(label));
    t.appendChild(k);
    return t;
  }
  function metaLine(t, text, tone) {
    var m = el('span', 't5 meta' + (tone ? ' tone-' + tone : ''), text);
    t.appendChild(m);
    return m;
  }
  function metaTime(t, prefix, ms, tone) {
    var m = el('span', 't5 meta' + (tone ? ' tone-' + tone : ''));
    m.appendChild(txt(prefix));
    m.appendChild(timeEl(ms, 't5' + (tone ? ' tone-' + tone : '')));
    t.appendChild(m);
    return m;
  }
  function staleNote(t) {
    if (!engineDown()) return;
    t.className += ' stale';
    metaLine(t, 'last known before the engine stopped');
  }

  function renderTiles() {
    var host = $('tiles');
    clear(host);
    var st = M.state || {};
    if (!UI.haveSnapshot) {
      var w = tile('Status');
      w.appendChild(el('div', 't3 val', 'waiting for the first snapshot'));
      host.appendChild(w);
      return;
    }
    /* Halt */
    var h = st.halt;
    var lv = haltLevel();
    var ht = tile('Halt', 'halt', 'halt ' + levelTone(lv));
    ht.appendChild(el('div', 'hero lg val tone-' + levelTone(lv), lv || 'unknown'));
    if (h) {
      if (isNum(h.at_ms)) metaTime(ht, 'since ', h.at_ms);
      if (h.hard_latch) metaLine(ht, 'hard latch on', 'alert');
      if (h.level && h.effective_level && h.level !== h.effective_level) metaLine(ht, 'recorded ' + h.level + ', effective ' + h.effective_level);
      if (h.rule_id || h.reason_code) metaLine(ht, [h.rule_id, h.reason_code].filter(Boolean).join(' · '));
      if (h.by_whom) metaLine(ht, 'by ' + h.by_whom);
    } else {
      metaLine(ht, 'no halt row read yet');
    }
    staleNote(ht);
    host.appendChild(ht);

    /* Why / when */
    var why = st.halt_why || {};
    var wt = tile('Why / when', 'flag');
    var words = [];
    if (lv === 'SOFT') {
      if (why.capped) words.push(['capped: will not escalate', 'ok']);
      else if (isNum(why.escalate_min)) words.push(['HARD in about ' + plural(why.escalate_min, 'min', 'min') + ' unless fixed', 'warn']);
    }
    wt.appendChild(el('div', 't3 val', words.length ? words[0][0] : (lv === 'HARD' ? 'HARD until an operator clears it' : 'nothing pending')));
    if (isNum(why.cooldown_clear_ms)) metaTime(wt, 'cool-down clear about ', why.cooldown_clear_ms);
    if (why.acked) metaLine(wt, 'acked', 'ok');
    host.appendChild(wt);

    /* Engine */
    var eg = st.engine || {};
    var et = tile('Engine', 'engine');
    var role = str(eg.role);
    var roleTone = role === 'LEADER' ? 'ok' : role === 'SAFE_HOLD' ? 'alert' : role ? 'info' : 'off';
    et.appendChild(el('div', 'hero val tone-' + roleTone, role || str(eg.status) || 'unknown'));
    if (eg.status === 'running') metaLine(et, 'running', 'ok');
    else if (eg.status === 'stalled') metaLine(et, 'RiskSweep stalled', 'alert');
    else if (eg.status === 'down') metaTime(et, 'not responding since ', isNum(eg.last_ok_ms) ? eg.last_ok_ms : eg.since_ms, 'alert');
    else if (eg.status === 'off') metaLine(et, 'engine probe off');
    else metaLine(et, 'state unknown');
    if (isNum(eg.sweep_age_s)) metaLine(et, 'RiskSweep pass age ' + eg.sweep_age_s + ' s');
    /* W1: the dead-man receiver as the leader sees it (deadman-alert.md, receiver not acknowledging) */
    var dm = eg.deadman;
    if (dm) {
      if (dm.configured === false) metaLine(et, 'dead-man receiver: not configured', 'warn');
      else if (isNum(dm.failures) && dm.failures > 0) {
        metaLine(et, 'dead-man receiver: not acknowledging, ' + plural(dm.failures, 'failed beat'), dm.failures >= 4 ? 'alert' : 'warn');
      } else if (isNum(dm.ok_age_s) && (dm.stale === 'warn' || dm.stale === 'alert')) {
        /* no failed beat, but no beat sent or acknowledged lately either (task dead, circuit open, loop stuck) */
        metaLine(et, 'dead-man receiver: no beat sent or acknowledged for ' + dm.ok_age_s + ' s', dm.stale);
      } else if (isNum(dm.ok_age_s)) metaLine(et, 'dead-man receiver: OK, last ack ' + dm.ok_age_s + ' s ago', 'ok');
      else metaLine(et, 'dead-man receiver: no acknowledged beat yet in this term');
    }
    var sh = st.safe_hold;
    if (sh && sh.on) {
      metaTime(et, 'SAFE_HOLD since ', sh.since_ms, 'alert');
      if (sh.hold_code) metaLine(et, 'hold ' + sh.hold_code + (isNum(sh.hold_count) ? ' · ' + sh.hold_count + ' abnormal ends' : ''));
    } else if (sh && sh.resumed_by && isNum(sh.last_resume_ms)) {
      metaLine(et, 'last resumed by ' + sh.resumed_by + ' at ' + fmtLocal(sh.last_resume_ms));
    }
    staleNote(et);
    host.appendChild(et);

    /* Leader */
    var ld = st.leader;
    var lt = tile('Leader', 'flag');
    if (ld) {
      var lvv = el('div', 't3 val');
      lvv.appendChild(txt(str(ld.node) + ' · epoch '));
      lvv.appendChild(el('span', 'idt', String(ld.epoch)));
      lt.appendChild(lvv);
      if (isNum(ld.since_ms)) metaTime(lt, 'term ' + ld.term_id + ' since ', ld.since_ms);
      if (isNum(ld.reached_leader_ms)) metaTime(lt, 'LEADER at ', ld.reached_leader_ms);
      if (ld.draining) metaLine(lt, 'draining (' + ld.draining + ')', 'warn');
    } else {
      lt.appendChild(el('div', 't3 val', 'no leader term'));
    }
    staleNote(lt);
    host.appendChild(lt);

    /* Streams (+ auth) */
    var stt = tile('Streams', 'streams', 'wide');
    var byAd = {}, order = [];
    (Array.isArray(st.streams) ? st.streams : []).forEach(function (s) {
      var a = str(s.adapter);
      if (!byAd[a]) { byAd[a] = []; order.push(a); }
      byAd[a].push(s);
    });
    if (!order.length) stt.appendChild(el('div', 't3 val', 'no stream rows'));
    var inactive = [];
    order.forEach(function (a) {
      var rows = byAd[a];
      if (rows.every(function (s) { return s.status === 'inactive'; })) { inactive.push(a); return; }
      rows.forEach(function (s) {
        var r = el('div', 'srow');
        var w = streamWord(s);
        r.appendChild(el('span', 't5', a + ' ' + str(s.stream)));
        r.appendChild(el('span', 't5 tone-' + w[1], w[0]));
        if (s.status === 'lost' && isNum(s.since_ms)) r.appendChild(timeEl(s.since_ms, 't5', true, 'since '));
        if (s.adapter_state) r.appendChild(el('span', 't5', str(s.adapter_state)));
        if (s.role_note) r.appendChild(el('span', 't5', str(s.role_note)));
        stt.appendChild(r);
      });
    });
    if (inactive.length) {
      var ir = el('div', 'srow');
      ir.appendChild(el('span', 't5', inactive.join(', ') + ': inactive (not configured)'));
      stt.appendChild(ir);
    }
    (Array.isArray(st.auth) ? st.auth : []).forEach(function (au) {
      var r = el('div', 'srow');
      var a = str(au.adapter);
      if (au.shown === 'as_recorded') {
        r.appendChild(el('span', 't5 tone-warn', a + ': AUTH_FAILED (as recorded, since ' + (isNum(au.since_ms) ? fmtLocal(au.since_ms) : 'unknown') + ')'));
      } else if (au.shown === 'cleared') {
        r.appendChild(el('span', 't5 tone-ok', a + ': AUTH_FAILED cleared (credentials changed)'));
      } else if (au.shown === 'failed_now') {
        r.appendChild(el('span', 't5 tone-alert', a + ': AUTH_FAILED since ' + (isNum(au.since_ms) ? fmtLocal(au.since_ms) : 'unknown')));
      } else if (au.shown === 'rearmed') {
        r.appendChild(el('span', 't5 tone-ok', a + ': re-armed' + (isNum(au.since_ms) ? ' at ' + fmtLocal(au.since_ms) : '')));
      } else {
        return;
      }
      stt.appendChild(r);
    });
    staleNote(stt);
    host.appendChild(stt);

    /* Oversight */
    var ov = Array.isArray(st.oversight) ? st.oversight : [];
    var ot = tile('Oversight', 'account');
    if (!ov.length) {
      ot.appendChild(el('div', 't3 val tone-ok', 'all accounts overseen'));
    } else {
      ot.appendChild(el('div', 't3 val tone-alert', 'Oversight lapsed'));
      ov.forEach(function (o) {
        metaLine(ot, 'account ' + str(o.account_id) + ' (' + str(o.adapter) + ') since ' + (isNum(o.since_ms) ? fmtLocal(o.since_ms) : 'unknown'), 'alert');
      });
    }
    host.appendChild(ot);

    /* Alert delivery */
    var al = st.alerts || {};
    var at = tile('Alert delivery', 'bell');
    if (al.undeliverable > 0) at.appendChild(el('div', 't3 val tone-alert', 'failing'));
    else if (al.waiting > 0) at.appendChild(el('div', 't3 val tone-warn', al.waiting + ' waiting'));
    else at.appendChild(el('div', 't3 val tone-ok', 'OK'));
    if (al.waiting > 0 && isNum(al.oldest_ms)) metaLine(at, 'oldest ' + dur(Date.now() - al.oldest_ms));
    if (al.undeliverable > 0) metaLine(at, plural(al.undeliverable, 'alert') + ' NOT delivered', 'alert');
    if (isNum(al.escalated_today)) metaLine(at, al.escalated_today + ' escalated today', al.escalated_today ? 'warn' : '');
    host.appendChild(at);

    /* DLQ */
    var dq = st.dlq || {};
    var queued = (dq.QUEUED || 0) + (dq.LEASED || 0) + (dq.BACKOFF || 0);
    var dt = tile('DLQ', 'dlq');
    dt.appendChild(el('div', 't3 val' + (queued ? ' tone-warn' : ''), queued + ' queued'));
    metaLine(dt, (dq.QUARANTINED || 0) + ' quarantined', dq.QUARANTINED ? 'alert' : '');
    if (dq.LEASED || dq.BACKOFF) metaLine(dt, (dq.LEASED || 0) + ' leased · ' + (dq.BACKOFF || 0) + ' in backoff');
    host.appendChild(dt);

    /* Breaches */
    var br = st.breaches || {};
    var bt = tile('Breaches', 'flag');
    var parts = ['FLAG', 'SOFT', 'HARD'].filter(function (l) { return br[l] > 0; });
    var bv = el('div', 't3 val');
    if (!parts.length) bv.appendChild(el('span', 'tone-ok', 'none open'));
    parts.forEach(function (l) { bv.appendChild(el('span', 'tone-' + levelTone(l), br[l] + ' ' + l)); });
    bt.appendChild(bv);
    if (br.pending > 0) metaLine(bt, br.pending + ' pending');
    host.appendChild(bt);

    /* Page */
    var pw = pageWord();
    var pt = tile('Page', 'refresh');
    pt.appendChild(el('div', 't3 val tone-' + pw[1], pw[0]));
    var p = M.poll;
    if (p) {
      if (p.db === 'down') metaLine(pt, 'db: down', 'alert');
      if (isNum(p.at_ms)) metaLine(pt, 'last poll ' + fmtLocal(p.at_ms) + ' (' + ago(p.at_ms) + ')');
      if (p.backlog > 0) metaLine(pt, 'catching up ' + plural(p.backlog, 'row'), 'warn');
    }
    host.appendChild(pt);
  }

  /* ===================== day panel ===================== */
  var STREAM_CLS = { up: 'f-up', down: 'f-down', fenced: 'f-fenced', engine_off: 'f-off', unknown: 'f-unknown',
    not_observed: 'f-hatch', inactive: 'f-inactive' };
  var ENGINE_CLS = { running: 'f-up', unknown: 'f-unknown', engine_off: 'f-off' };
  var HALT_CLS = { NONE: 'f-none', SOFT: 'f-soft', HARD: 'f-hard' };
  var STREAM_ORDER = ['orders', 'account', 'prices'];

  function ensureDefs() {
    if ($('lvDefs')) return;
    var s = sv('svg', { id: 'lvDefs', width: 0, height: 0, 'aria-hidden': 'true', 'class': 'sr', focusable: 'false' });
    var defs = sv('defs');
    [['lvHatch', ''], ['lvHatchRed', ' red']].forEach(function (p) {
      var pat = sv('pattern', { id: p[0], patternUnits: 'userSpaceOnUse', width: 6, height: 6, patternTransform: 'rotate(45)' });
      pat.appendChild(sv('line', { x1: 0, y1: 0, x2: 0, y2: 6, 'class': 'hatch-line' + p[1] }));
      defs.appendChild(pat);
    });
    s.appendChild(defs);
    document.body.appendChild(s);
  }
  function svgTitle(node, text) { node.appendChild(sv('title', null, text)); return node; }

  function renderDay() {
    var host = $('day');
    clear(host);
    var day = M.day;
    var head = el('div', 'day-h');
    var left = el('span');
    left.appendChild(el('span', 't2', 'Day'));
    left.appendChild(el('span', 't2h', 'Today'));
    if (day && isNum(day.start_ms)) {
      var d0 = new Date(day.start_ms);
      left.appendChild(el('span', 't5', d0.toDateString()));
    }
    head.appendChild(left);
    head.appendChild(el('span', 't5 s', 'local time · 15-minute buckets · click a bar to jump to that time'));
    host.appendChild(head);
    if (!day || !isNum(day.start_ms)) {
      host.appendChild(el('p', 't4', 'No day model yet.'));
      return;
    }
    var box = el('div', 'dayhost');
    host.appendChild(box);
    var W = Math.max(280, Math.floor(host.clientWidth - 24) || 600);
    var L = W < 520 ? 66 : 92, R = 8, pw = W - L - R;
    var start = day.start_ms, end = start + DAYMS;
    var nowMs = Math.min(end, Math.max(start, Math.max(isNum(day.now_ms) ? day.now_ms : 0, Date.now())));
    function x(t) { return L + Math.max(0, Math.min(1, (t - start) / DAYMS)) * pw; }
    var adapters = [];
    var streams = day.streams && typeof day.streams === 'object' ? day.streams : {};
    var inactive = Array.isArray(day.inactive) ? day.inactive : [];
    Object.keys(streams).forEach(function (a) { adapters.push(a); });
    adapters.sort(function (a, b) { return (inactive.indexOf(a) >= 0) - (inactive.indexOf(b) >= 0) || (a < b ? -1 : a > b ? 1 : 0); });
    var shown = adapters.filter(function (a) { return UI.inactiveOpen || inactive.indexOf(a) < 0; });
    var hidden = adapters.length - shown.length;

    var y = 16, STRIP_H = 48, LANE = 10;
    var yStrip = y;
    y += STRIP_H + 8;
    var yHalt = y; y += LANE + 4;
    var yEng = y; y += LANE + 6;
    var rows = [];
    shown.forEach(function (a) {
      rows.push({ type: 'adapter', a: a, y: y });
      y += 13;
      var names = Object.keys(streams[a] || {}).sort(function (p, q) {
        var ip = STREAM_ORDER.indexOf(p), iq = STREAM_ORDER.indexOf(q);
        return (ip < 0 ? 9 : ip) - (iq < 0 ? 9 : iq);
      });
      names.forEach(function (s) { rows.push({ type: 'stream', a: a, s: s, y: y }); y += LANE + 3; });
      y += 3;
    });
    var yAxis = y + 2;
    var H = yAxis + 16;
    var svg = sv('svg', { width: W, height: H, viewBox: '0 0 ' + W + ' ' + H, role: 'group', 'aria-label': 'Today: events, halt level, engine and streams, local time' });

    /* labels */
    function label(t, yy, cls) { svg.appendChild(sv('text', { x: 0, y: yy, 'class': 't5' + (cls ? ' ' + cls : '') }, t)); }
    label('events', yStrip + STRIP_H / 2 + 4);
    label('halt', yHalt + 9);
    label('engine', yEng + 9);

    /* grid */
    for (var hh = 0; hh <= 24; hh += 6) {
      var gx = x(start + hh * HOUR);
      svg.appendChild(sv('line', { x1: gx, x2: gx, y1: yStrip, y2: yAxis, 'class': 's-grid' }));
    }

    /* day strip: 96 stacked buckets, counts only */
    var buckets = Array.isArray(day.buckets) ? day.buckets : [];
    var bw = pw / 96;
    var classes = ['activity', 'notice', 'warn', 'alert'];
    if (UI.hk) classes.push('housekeeping');
    var maxN = 4;
    buckets.forEach(function (b) {
      var tot = 0;
      classes.forEach(function (c) { tot += (b && isNum(b[c]) ? b[c] : 0); });
      maxN = Math.max(maxN, tot);
    });
    svg.appendChild(sv('line', { x1: L, x2: L + pw, y1: yStrip + STRIP_H, y2: yStrip + STRIP_H, 'class': 's-axis' }));
    buckets.forEach(function (b, i) {
      if (!b || i >= 96) return;
      var bx = L + i * bw;
      if (b.partial) svg.appendChild(sv('rect', { x: bx, y: yStrip, width: bw, height: STRIP_H, 'class': 'f-hatch' }));
      var yy = yStrip + STRIP_H;
      classes.forEach(function (c) {
        var n = isNum(b[c]) ? b[c] : 0;
        if (!n) return;
        var hgt = n / maxN * STRIP_H;
        yy -= hgt;
        svg.appendChild(sv('rect', { x: bx + 0.5, y: yy, width: Math.max(1, bw - 1), height: hgt, 'class': 'f-' + (c === 'housekeeping' ? 'hk' : c) }));
      });
      var t0 = start + i * 15 * MIN, t1 = t0 + 15 * MIN;
      var parts = [b.activity || 0, 'activity', b.notice || 0, 'notice', b.warn || 0, 'warn', b.alert || 0, 'alert'];
      var tip = fmtLocal(t0, false) + '–' + fmtLocal(t1, false) + ': ' + parts[0] + ' activity, ' + parts[2] + ' notice, ' +
        parts[4] + ' warn, ' + parts[6] + ' alert';
      if (UI.hk) tip += ', ' + (b.housekeeping || 0) + ' housekeeping';
      if (b.partial) tip += ' (partial: older rows were not loaded)';
      var hit = sv('rect', { x: bx, y: yStrip, width: bw, height: STRIP_H, 'class': 'hit', tabindex: 0, role: 'button', 'aria-label': tip });
      svgTitle(hit, tip);
      hit.addEventListener('click', function () { jumpTo(t1); });
      hit.addEventListener('keydown', function (ev) {
        if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); jumpTo(t1); }
      });
      svg.appendChild(hit);
    });

    /* lanes */
    function lane(yy) { svg.appendChild(sv('rect', { x: L, y: yy, width: pw, height: LANE, 'class': 'f-lane' })); }
    function seg(yy, from, to, cls, tip) {
      if (!isNum(from)) return;
      var a = x(from), b = x(isNum(to) ? to : nowMs);
      if (b <= a) b = a + 1;
      var r = sv('rect', { x: a, y: yy, width: Math.max(1.5, b - a), height: LANE, 'class': cls });
      svgTitle(r, tip + ' ' + fmtLocal(from, false) + '–' + (isNum(to) ? fmtLocal(to, false) : 'now'));
      svg.appendChild(r);
      return r;
    }
    lane(yHalt);
    (Array.isArray(day.halt) ? day.halt : []).forEach(function (h) {
      seg(yHalt, h.from_ms, h.to_ms, HALT_CLS[h.level] || 'f-unknown', 'halt ' + str(h.level));
      if (h.hard_latch) seg(yHalt, h.from_ms, h.to_ms, 'f-hatchred', 'hard latch');
    });
    lane(yEng);
    (Array.isArray(day.engine) ? day.engine : []).forEach(function (s) {
      seg(yEng, s.from_ms, s.to_ms, ENGINE_CLS[s.state] || 'f-unknown', 'engine ' + str(s.state).replace('_', ' '));
    });
    rows.forEach(function (r) {
      if (r.type === 'adapter') {
        label(r.a + (inactive.indexOf(r.a) >= 0 ? ' (inactive)' : ''), r.y + 10, 'ink1');
        return;
      }
      label(r.s, r.y + 9);
      lane(r.y);
      var segs = (streams[r.a] && streams[r.a][r.s]) || [];
      segs.forEach(function (s) {
        seg(r.y, s.from_ms, s.to_ms, STREAM_CLS[s.state] || 'f-unknown', r.a + ' ' + r.s + ' ' + str(s.state).replace('_', ' '));
      });
    });

    /* axis and now tick */
    for (var hx = 0; hx <= 24; hx += (W < 520 ? 6 : 3)) {
      var ax = x(start + hx * HOUR);
      svg.appendChild(sv('text', { x: ax, y: yAxis + 12, 'text-anchor': hx === 0 ? 'start' : hx === 24 ? 'end' : 'middle', 'class': 't5' }, pad(hx)));
    }
    var nx = x(nowMs);
    svg.appendChild(sv('line', { x1: nx, x2: nx, y1: yStrip - 4, y2: yAxis, 'class': 's-now' }));
    svg.appendChild(sv('text', { x: Math.min(nx, L + pw - 2), y: 11, 'text-anchor': nx > L + pw - 60 ? 'end' : 'middle', 'class': 't5 ink1 b' }, 'now ' + fmtLocal(nowMs, false)));
    box.appendChild(svg);

    if (hidden > 0 || (UI.inactiveOpen && inactive.length)) {
      var tb = btn('btn daybtn', UI.inactiveOpen ? 'hide inactive adapters' : 'show ' + plural(hidden, 'inactive adapter') + ' (' + inactive.join(', ') + ')');
      tb.setAttribute('aria-expanded', UI.inactiveOpen ? 'true' : 'false');
      tb.addEventListener('click', function () { UI.inactiveOpen = !UI.inactiveOpen; mark('day'); });
      host.appendChild(tb);
    }
    var lg = el('div', 'legend');
    [['activity', 'activity'], ['notice', 'notice'], ['warn', 'warn'], ['alert', 'alert']].concat(UI.hk ? [['hk', 'housekeeping']] : [])
      .concat([['up', 'up / NONE'], ['soft', 'SOFT / fenced'], ['hard', 'HARD / down'], ['off', 'engine off'], ['unknown', 'unknown'],
        ['notobs', 'not observed'], ['now', 'now']])
      .forEach(function (p) {
        var s = el('span');
        s.appendChild(el('i', 'sw ' + p[0]));
        s.appendChild(el('span', 't5 s', p[1]));
        lg.appendChild(s);
      });
    host.appendChild(lg);
  }
  /* scroll the stories to the newest item at or before the end of the clicked bucket */
  function jumpTo(tEnd) {
    var items = $('feed').children, target = null;
    for (var i = 0; i < items.length; i++) {
      var t = parseFloat(items[i].getAttribute('data-t'));
      if (isFinite(t) && t < tEnd) { target = items[i]; break; }
    }
    if (!target && items.length) target = items[items.length - 1];
    if (!target) return;
    target.scrollIntoView({ block: 'start' });
    var card = target.firstChild;
    if (card && card.classList) {
      card.classList.add('flash');
      setTimeout(function () { card.classList.remove('flash'); }, 1800);
    }
  }

  /* ===================== feed controls ===================== */
  var controlsBuilt = false, qInput = null, pauseBtn = null, hkBtn = null, chipBox = null, fstate = null;
  function buildControls() {
    var host = $('controls');
    var bar = el('div', 'fbar');
    var lab = el('label', 'search');
    lab.appendChild(el('span', 'sr', 'Filter by id or symbol'));
    lab.appendChild(icon('search', 14));
    qInput = el('input', 't4');
    qInput.type = 'search';
    qInput.autocomplete = 'off';
    qInput.spellcheck = false;
    qInput.placeholder = 'Filter: id or symbol';
    qInput.addEventListener('input', function () { UI.q = qInput.value.trim().toLowerCase(); mark('feed', 'controls'); });
    lab.appendChild(qInput);
    bar.appendChild(lab);
    pauseBtn = btn('btn');
    pauseBtn.setAttribute('aria-pressed', 'false');
    pauseBtn.addEventListener('click', function () {
      UI.paused = !UI.paused;
      if (!UI.paused) { UI.pausedCount = 0; mark('feed'); }
      mark('controls');
    });
    bar.appendChild(pauseBtn);
    hkBtn = btn('btn');
    hkBtn.setAttribute('aria-pressed', 'false');
    hkBtn.addEventListener('click', function () { UI.hk = !UI.hk; mark('feed', 'controls', 'day'); });
    bar.appendChild(hkBtn);
    host.appendChild(bar);
    chipBox = el('div', 'chips');
    chipBox.setAttribute('role', 'group');
    chipBox.setAttribute('aria-label', 'Categories');
    host.appendChild(chipBox);
    fstate = el('div', 'fstate t5');
    host.appendChild(fstate);
    controlsBuilt = true;
  }
  function renderControls() {
    if (!controlsBuilt) buildControls();
    clear(pauseBtn);
    pauseBtn.appendChild(icon(UI.paused ? 'play' : 'pause', 14));
    pauseBtn.appendChild(el('span', 't5', UI.paused ? 'Resume' : 'Pause'));
    if (UI.paused && UI.pausedCount > 0) {
      var c = el('span', 'cnt');
      c.appendChild(el('span', 't5 s', String(UI.pausedCount)));
      pauseBtn.appendChild(c);
    }
    pauseBtn.className = 'btn' + (UI.paused ? ' paused' : '');
    pauseBtn.setAttribute('aria-pressed', UI.paused ? 'true' : 'false');
    var hkN = 0;
    M.ns.forEach(function (n) { if (isHk(M.events[n])) hkN++; });
    clear(hkBtn);
    hkBtn.appendChild(el('span', 't5', 'Show housekeeping (' + hkN + ')'));
    hkBtn.setAttribute('aria-pressed', UI.hk ? 'true' : 'false');

    clear(chipBox);
    var counts = {};
    M.ns.forEach(function (n) { var e = M.events[n]; if (e && (UI.hk || !isHk(e))) counts[e.category] = (counts[e.category] || 0) + 1; });
    var any = Object.keys(UI.cats).length > 0;
    var all = btn('chip');
    all.appendChild(el('span', 't5', 'all'));
    all.setAttribute('aria-pressed', any ? 'false' : 'true');
    all.addEventListener('click', function () { UI.cats = {}; mark('feed', 'controls'); });
    chipBox.appendChild(all);
    CATS.forEach(function (c) {
      var b = btn('chip');
      b.appendChild(icon(CAT_ICON[c], 12));
      b.appendChild(el('span', 't5', c));
      b.appendChild(el('span', 't5 s', String(counts[c] || 0)));
      b.setAttribute('aria-pressed', UI.cats[c] ? 'true' : 'false');
      b.addEventListener('click', function () {
        if (UI.cats[c]) delete UI.cats[c]; else UI.cats[c] = true;
        mark('feed', 'controls');
      });
      chipBox.appendChild(b);
    });
    var bits = [];
    if (any) bits.push('categories: ' + Object.keys(UI.cats).join(', '));
    if (UI.q) bits.push('filter: "' + UI.q + '"');
    if (UI.paused) bits.push('paused: ' + plural(UI.pausedCount, 'new line') + ' waiting');
    fstate.textContent = bits.join(' · ');
    fstate.hidden = !bits.length;
  }

  /* ===================== stories feed ===================== */
  function isHk(e) { return !!e && (e.housekeeping === true || e.severity === 'debug'); }
  function lineTime(e) {
    if (e.late && isNum(e.detected_ms)) return e.detected_ms;
    return isNum(e.at_ms) ? e.at_ms : (isNum(e.detected_ms) ? e.detected_ms : 0);
  }
  function buildGroups() {
    var groups = {}, standalone = [];
    Object.keys(M.stories).forEach(function (k) {
      if (resolve(k).key === k) groups[k] = { h: M.stories[k], lines: [], seen: {}, missing: 0 };
    });
    function place(gk, ev, held) {
      var g = groups[gk];
      if (!g || g.seen[ev.n]) return !!g;
      g.seen[ev.n] = 1;
      g.lines.push({ ev: ev, held: held });
      return true;
    }
    Object.keys(M.stories).forEach(function (k) {
      var r = resolve(k);
      var g = groups[r.key];
      if (!g) return;
      (Array.isArray(M.stories[k].events) ? M.stories[k].events : []).forEach(function (n) {
        var ev = M.events[n];
        if (ev) place(r.key, ev, r.held || (ev.story && ev.story !== r.key && resolve(ev.story).held));
        else if (!g.seen['m' + n]) { g.seen['m' + n] = 1; g.missing++; }
      });
    });
    M.ns.forEach(function (n) {
      var ev = M.events[n];
      var sk = (typeof ev.story === 'string' && ev.story) || (typeof ev.parent === 'string' && ev.parent) || null;
      if (!sk) { standalone.push(ev); return; }
      var r = resolve(sk);
      if (!place(r.key, ev, r.held)) standalone.push(ev);
    });
    Object.keys(groups).forEach(function (k) { groups[k].lines.sort(function (a, b) { return a.ev.n - b.ev.n; }); });
    return { groups: groups, standalone: standalone };
  }
  function evHay(e) {
    var h = str(e.title);
    if (e.ids && typeof e.ids === 'object') Object.keys(e.ids).forEach(function (k) { h += ' ' + str(e.ids[k]); });
    return h.toLowerCase();
  }
  function storyHay(g) {
    var h = g.h, s = str(h.key) + ' ' + str(h.symbol) + ' ' + str(h.header);
    if (h.ids && typeof h.ids === 'object') Object.keys(h.ids).forEach(function (k) { s += ' ' + str(h.ids[k]); });
    s = s.toLowerCase();
    g.lines.forEach(function (l) { s += ' ' + evHay(l.ev); });
    return s;
  }
  function matchQ(hay) {
    if (!UI.q) return true;
    return UI.q.split(/\s+/).every(function (w) { return !w || hay.indexOf(w) !== -1; });
  }
  function storyPassesCats(g) {
    var cats = UI.cats;
    if (!Object.keys(cats).length) return true;
    var k = KIND[g.h.kind];
    if (k && cats[k.cat]) return true;
    var c = g.h.counts || {};
    for (var key in c) { if (has(c, key) && c[key] > 0 && cats[key]) return true; }
    return g.lines.some(function (l) { return cats[l.ev.category]; });
  }
  function lateText(e) {
    if (!e.late) return '';
    var base;
    if (e.late === 'history' || e.late === 'while_offline') base = 'found while the engine was off';
    else if (e.late === 'snapshot') base = 'reported late (found on reconnect)';
    else if (e.late === 'dlq') base = 'reported late (replayed from the DLQ)';
    else base = 'reported late';
    if (isNum(e.at_ms)) {
      base += ' · action ' + fmtLocal(e.at_ms);
      if (isNum(e.detected_ms) && e.detected_ms - e.at_ms >= MIN && (e.late === 'delayed' || e.late === 'snapshot' || e.late === 'dlq')) {
        base += ' (reported ' + dur(e.detected_ms - e.at_ms) + ' after it happened)';
      }
    }
    return base;
  }
  function catChip(c) {
    var s = el('span', 'cchip');
    if (CAT_ICON[c]) s.appendChild(icon(CAT_ICON[c], 11));
    s.appendChild(el('span', 't5 s', str(c)));
    return s;
  }
  function lineEl(ev, held, badges) {
    var sev = held ? 'info' : str(ev.severity) || 'info';
    var li = el('li', 'line sev-' + sev);
    li.setAttribute('data-n', String(ev.n));
    var tm = lineTime(ev);
    var t = timeEl(tm, 't5 lt');
    if (isNum(tm)) t.title = fmtUtc(tm) + (ev.clock ? ' · ' + ev.clock + ' clock' : '');
    li.appendChild(t);
    var x = el('div', 'lx');
    x.appendChild(richText(el('div', 't4 c'), ev.title));
    var m = el('div', 'lm');
    m.appendChild(catChip(ev.category));
    if (sev === 'alert' || sev === 'warn') m.appendChild(badge(sev, sevTone(sev)));
    if (held) m.appendChild(badge('held for catch-up', 'info'));
    if (isHk(ev)) m.appendChild(badge('housekeeping', ''));
    var lt = lateText(ev);
    if (lt) {
      var ls = el('span', 't5 s tone-warn', lt);
      if (isNum(ev.at_ms)) ls.title = 'action ' + fmtUtc(ev.at_ms);
      m.appendChild(ls);
    }
    (badges && Array.isArray(badges[ev.sid]) ? badges[ev.sid] : []).forEach(function (b) {
      var bt = BADGE_TEXT[b] || [str(b), ''];
      m.appendChild(badge(bt[0], bt[1]));
    });
    if (ev.ids && typeof ev.ids === 'object') {
      Object.keys(ev.ids).forEach(function (k) {
        var ic = el('span', 'idc t5 s', k + ' ' + str(ev.ids[k]));
        ic.title = k;
        m.appendChild(ic);
      });
    }
    x.appendChild(m);
    li.appendChild(x);
    return li;
  }
  function spanOf(h, nowMs) {
    var open = has(OPEN_STATES, h.state);
    if (h.kind === 'outage') {
      var ms = isNum(h.gap_ms) ? h.gap_ms : (nowMs - h.first_ms);
      return { ms: ms, max: MIN, word: isNum(h.gap_ms) ? 'gap' : 'lost for', tone: 'alert' };
    }
    var end = open ? nowMs : h.last_ms;
    return { ms: end - h.first_ms, max: HOUR, word: open ? 'open' : 'lasted', tone: h.worst === 'alert' ? 'alert' : h.worst === 'warn' ? 'warn' : '' };
  }
  function durWrap(h) {
    if (!isNum(h.first_ms) || !isNum(h.last_ms)) return null;
    var sp = spanOf(h, Date.now());
    var w = el('span', 'durw');
    var BW = 60;
    var s = sv('svg', { width: BW, height: 6, viewBox: '0 0 ' + BW + ' 6', 'aria-hidden': 'true' });
    s.appendChild(sv('rect', { x: 0, y: 0, width: BW, height: 6, 'class': 'f-durbg' }));
    s.appendChild(sv('rect', { x: 0, y: 0, width: Math.max(1, Math.min(1, sp.ms / sp.max) * BW), height: 6, 'class': 'f-dur' + (sp.tone ? ' ' + sp.tone : '') }));
    w.appendChild(s);
    w.appendChild(el('span', 't5 s', sp.word + ' ' + dur(sp.ms) + (sp.ms > sp.max ? ' (scale ' + (sp.max === MIN ? '60 s' : '1 h') + ')' : '')));
    return w;
  }
  function relChip(key) {
    var other = M.stories[resolve(key).key] || M.stories[key];
    var kd = other ? KIND[other.kind] : null;
    var b = btn('rel');
    b.appendChild(icon(kd ? kd.icon : 'link', 12));
    b.appendChild(el('span', 't5 s', (kd ? kd.label + ': ' : '') + (other && other.symbol ? other.symbol + ' ' : '') + key));
    b.title = 'Open the linked story ' + key;
    b.addEventListener('click', function (ev) { ev.stopPropagation(); openDrawer(key); });
    return b;
  }
  function badgeSummary(h) {
    var out = {}, order = [];
    var b = h.badges && typeof h.badges === 'object' ? h.badges : {};
    Object.keys(b).forEach(function (sid) {
      (Array.isArray(b[sid]) ? b[sid] : []).forEach(function (s) {
        if (!has(out, s)) { out[s] = 0; order.push(s); }
        out[s]++;
      });
    });
    return order.map(function (s) { return [s, out[s]]; });
  }
  function storyCard(g) {
    var h = g.h;
    var kd = KIND[h.kind] || { label: str(h.kind) + ' story', icon: 'link', cat: '' };
    var li = el('li');
    li.setAttribute('data-t', String(isNum(h.last_ms) ? h.last_ms : 0));
    li.setAttribute('data-key', h.key);
    var worst = str(h.worst) || 'info';
    var card = el('div', 'scard sev-' + worst);
    var top = el('div', 'sc-top');
    var kn = el('span', 'kind');
    kn.appendChild(icon(kd.icon, 13));
    kn.appendChild(el('span', 't5 s', kd.label));
    top.appendChild(kn);
    if (kd.cat) top.appendChild(catChip(kd.cat));
    if (h.state) top.appendChild(badge(str(h.state), has(OPEN_STATES, h.state) ? (h.state === 'lost' || h.state === 'quarantined' ? 'alert' : 'info') : 'ok'));
    if (worst === 'alert' || worst === 'warn' || worst === 'notice') top.appendChild(badge(worst, sevTone(worst)));
    if (h.kind === 'dlq' && h.dlq_status) top.appendChild(badge(str(h.dlq_status), h.dlq_status === 'QUARANTINED' ? 'alert' : ''));
    if (isNum(h.last_ms)) top.appendChild(timeEl(h.last_ms, 't5 when', true, 'last '));
    card.appendChild(top);
    var title = btn('sc-title t3');
    richText(title, str(h.header) || h.key);
    title.title = 'Open the story details';
    title.addEventListener('click', function () { openDrawer(h.key); });
    card.appendChild(title);

    var meta = el('div', 'sc-meta');
    var dw = durWrap(h);
    if (dw) meta.appendChild(dw);
    var counts = h.counts && typeof h.counts === 'object' ? h.counts : {};
    var cparts = Object.keys(counts).filter(function (k) { return counts[k] > 0; }).map(function (k) { return k + ' ' + counts[k]; });
    if (cparts.length) meta.appendChild(el('span', 't5 s', cparts.join(' · ')));
    (Array.isArray(h.episodes) ? h.episodes : []).slice(-3).forEach(function (ep) {
      if (ep.resolved) meta.appendChild(badge(str(ep.rule_id) + ' resolved', 'ok'));
      else meta.appendChild(badge((ep.level ? ep.level + ' ' : '') + str(ep.rule_id), levelTone(ep.level)));
      if (ep.repages > 0) meta.appendChild(badge('re-paged ' + ep.repages + '×' + (isNum(ep.last_repage_ms) ? ', last ' + fmtLocal(ep.last_repage_ms, false) : ''), 'warn'));
    });
    badgeSummary(h).forEach(function (p) {
      var bt = BADGE_TEXT[p[0]] || [str(p[0]), ''];
      meta.appendChild(badge(bt[0] + (p[1] > 1 ? ' ×' + p[1] : ''), bt[1]));
    });
    (Array.isArray(h.related) ? h.related : []).forEach(function (rk) { if (typeof rk === 'string') meta.appendChild(relChip(rk)); });
    var visible = g.lines.filter(function (l) { return UI.hk || !isHk(l.ev); });
    var open = !!UI.expanded[h.key];
    var tg = btn('sc-toggle');
    tg.appendChild(el('span', 't5 s', plural(visible.length, 'line')));
    tg.appendChild(icon('chevron', 12));
    tg.setAttribute('aria-expanded', open ? 'true' : 'false');
    tg.addEventListener('click', function () {
      if (UI.expanded[h.key]) delete UI.expanded[h.key]; else UI.expanded[h.key] = true;
      mark('feed');
    });
    meta.appendChild(tg);
    card.appendChild(meta);

    if (open) {
      var ol = el('ol', 'lines');
      visible.forEach(function (l) { ol.appendChild(lineEl(l.ev, l.held, h.badges)); });
      if (!visible.length) ol.appendChild(el('li', 't5 more', 'No lines held in this browser.'));
      if (g.missing > 0) {
        var mb = btn('more linkish', plural(g.missing, 'earlier line') + ' are in the story details');
        mb.addEventListener('click', function () { openDrawer(h.key); });
        var li2 = el('li');
        li2.appendChild(mb);
        ol.appendChild(li2);
      }
      card.appendChild(ol);
    }
    li.appendChild(card);
    return li;
  }
  function lineCard(ev) {
    var li = el('li');
    li.setAttribute('data-t', String(lineTime(ev)));
    var c = el('div', 'lcard sev-' + (str(ev.severity) || 'info'));
    var ol = el('ol', 'lines bare');
    ol.appendChild(lineEl(ev, false, null));
    c.appendChild(ol);
    li.appendChild(c);
    return li;
  }
  function renderFeed() {
    var host = $('feed');
    clear(host);
    if (!UI.haveSnapshot) {
      host.appendChild(el('li', 'empty t4', 'Waiting for the first snapshot.'));
      return;
    }
    var gs = buildGroups();
    var items = [];
    var nStories = 0, nLines = 0;
    Object.keys(gs.groups).forEach(function (k) {
      var g = gs.groups[k];
      if (!storyPassesCats(g) || !matchQ(storyHay(g))) return;
      nStories++;
      items.push({ t: isNum(g.h.last_ms) ? g.h.last_ms : 0, tie: k, g: g });
    });
    gs.standalone.forEach(function (ev) {
      if (!UI.hk && isHk(ev)) return;
      if (Object.keys(UI.cats).length && !UI.cats[ev.category]) return;
      if (!matchQ(evHay(ev))) return;
      nLines++;
      items.push({ t: lineTime(ev), tie: 'n' + ev.n, ev: ev });
    });
    items.sort(function (a, b) { return b.t - a.t || (a.tie < b.tie ? 1 : a.tie > b.tie ? -1 : 0); });
    items.slice(0, FEED_MAX).forEach(function (it) { host.appendChild(it.g ? storyCard(it.g) : lineCard(it.ev)); });
    if (!items.length) host.appendChild(el('li', 'empty t4', Object.keys(UI.cats).length || UI.q ? 'Nothing matches the filter.' : 'No activity today yet.'));
    if (items.length > FEED_MAX) host.appendChild(el('li', 'empty t5', (items.length - FEED_MAX) + ' older items hidden: narrow the filter to see them.'));
    $('feedCount').textContent = plural(nStories, 'story', 'stories') + ' · ' + plural(nLines, 'single line');
  }

  /* ===================== ladder and sparklines ===================== */
  function fitWidth(host, fallback) {
    var w = host && host.clientWidth ? host.clientWidth : 0;
    return Math.max(200, w || fallback);
  }
  /* entry / stop / take-profit on one line, evenly spaced in level order (owner decision 8.3: B);
     no axis numbers, no distances, no current price */
  function ladderSvg(entry, stop, tp, W) {
    var H = 62, padX = 44, y = 30;
    var marks = [];
    [['E', entry, 'm-entry', 'tone-info'], ['SL', stop, 'm-stop', 'tone-alert'], ['TP', tp, 'm-tp', 'tone-ok']].forEach(function (p) {
      var v = level(p[1]);
      if (v === null || v === NOT_SHOWN || levelNum(v) === null) return;
      var same = marks.filter(function (m) { return m.v === v; })[0];
      if (same) { same.k += ' = ' + p[0]; return; }
      marks.push({ k: p[0], v: v, cls: p[2], tone: p[3] });
    });
    marks.sort(function (a, b) { return levelNum(a.v) - levelNum(b.v); });
    function x(v) {
      var i = -1;
      marks.forEach(function (m, j) { if (m.v === v) i = j; });
      return marks.length < 2 ? W / 2 : padX + i / (marks.length - 1) * (W - 2 * padX);
    }
    var desc = marks.map(function (m) { return m.k + ' ' + m.v; }).join(', ') + '; in level order, not to scale';
    var svg = sv('svg', { width: W, height: H, viewBox: '0 0 ' + W + ' ' + H, role: 'img', 'aria-label': desc });
    svg.appendChild(sv('line', { x1: 4, x2: W - 4, y1: y, y2: y, 'class': 'lad-base' }));
    var e = level(entry);
    if (e !== null && levelNum(e) !== null) {
      [[stop, 'lad-stop'], [tp, 'lad-tp']].forEach(function (p) {
        var v = level(p[0]);
        if (v === null || levelNum(v) === null || v === e) return;
        svg.appendChild(sv('line', { x1: x(e), x2: x(v), y1: y, y2: y, 'class': p[1] }));
      });
    }
    marks.forEach(function (m) {
      var mx = x(m.v);
      svg.appendChild(sv('rect', { x: mx - 1.5, y: y - 8, width: 3, height: 16, 'class': m.cls }));
      svg.appendChild(sv('text', { x: mx, y: y - 13, 'text-anchor': 'middle', 'class': 't5 b ' + m.tone }, m.k));
      svg.appendChild(sv('text', { x: mx, y: y + 24, 'text-anchor': 'middle', 'class': 't5 ink1' }, m.v));
    });
    return svg;
  }
  /* stepped stop / take-profit levels since 00:00: time to scale, levels as evenly spaced rows in level order */
  function levelSpark(history, entry, W, big) {
    var pts = (Array.isArray(history) ? history : []).filter(function (p) { return p && isNum(p.at_ms); })
      .slice().sort(function (a, b) { return a.at_ms - b.at_ms; });
    var H = big ? 160 : 104, Lp = 6, Rp = 96, TOP = 12, B = 20;
    var dayStart = M.day && isNum(M.day.start_ms) ? M.day.start_ms : null;
    var nowMs = Date.now();
    var t0 = pts.length ? pts[0].at_ms : (dayStart || nowMs - HOUR);
    if (dayStart !== null && t0 < dayStart) t0 = dayStart;
    var t1 = Math.max(nowMs, t0 + MIN);
    function ok(v) { v = level(v); return v !== null && v !== NOT_SHOWN && levelNum(v) !== null ? v : null; }
    var vals = [];
    pts.forEach(function (p) { [p.stop, p.take_profit].forEach(function (v) { v = ok(v); if (v !== null && vals.indexOf(v) < 0) vals.push(v); }); });
    var ent = ok(entry);
    if (ent !== null && vals.indexOf(ent) < 0) vals.push(ent);
    vals.sort(function (a, b) { return levelNum(b) - levelNum(a); });
    function x(t) { return Lp + Math.max(0, Math.min(1, (t - t0) / (t1 - t0))) * (W - Lp - Rp); }
    function y(v) { var i = vals.indexOf(v); return vals.length < 2 ? (TOP + H - B) / 2 : TOP + i / (vals.length - 1) * (H - TOP - B); }
    var svg = sv('svg', { width: W, height: H, viewBox: '0 0 ' + W + ' ' + H, role: 'img',
      'aria-label': 'Stop and take-profit levels since ' + fmtLocal(t0, false) + ', in level order, not to scale' });
    var xr = W - Rp;
    vals.forEach(function (v) { svg.appendChild(sv('line', { x1: Lp, x2: xr, y1: y(v), y2: y(v), 'class': 'sp-row' })); });
    svg.appendChild(sv('line', { x1: Lp, x2: xr, y1: H - B + 4, y2: H - B + 4, 'class': 'sp-base' }));
    if (ent !== null) svg.appendChild(sv('line', { x1: Lp, x2: xr, y1: y(ent), y2: y(ent), 'class': 'sp-entry' }));
    var lastV = {};
    [['stop', 'sp-stop', 'SL', 'tone-alert'], ['take_profit', 'sp-tp', 'TP', 'tone-ok']].forEach(function (f) {
      var d = '', prev = null;
      pts.forEach(function (p) {
        var v = ok(p[f[0]]);
        if (v === null) { if (prev !== null) { d += 'H' + x(p.at_ms).toFixed(1); prev = null; } return; }
        if (prev === null) d += 'M' + x(p.at_ms).toFixed(1) + ' ' + y(v).toFixed(1);
        else d += 'H' + x(p.at_ms).toFixed(1) + 'V' + y(v).toFixed(1);
        prev = v;
      });
      if (prev !== null) d += 'H' + xr.toFixed(1);
      if (d) svg.appendChild(sv('path', { d: d, 'class': f[1] }));
      lastV[f[2]] = { v: prev, tone: f[3] };
    });
    var ys = [];
    ['SL', 'TP'].forEach(function (k) {
      var lv = lastV[k];
      if (!lv || lv.v === null) return;
      var yy = y(lv.v) + 4;
      ys.forEach(function (o) { if (Math.abs(o - yy) < 13) yy = o + 13; });
      ys.push(yy);
      svg.appendChild(sv('text', { x: xr + 6, y: yy, 'class': 't5 b ' + lv.tone }, k + ' ' + lv.v));
    });
    if (ent !== null) svg.appendChild(sv('text', { x: Lp + 2, y: Math.max(10, y(ent) - 4), 'class': 't5 tone-info' }, 'entry ' + ent));
    svg.appendChild(sv('text', { x: Lp, y: H - 3, 'class': 't5' }, fmtLocal(t0, false)));
    svg.appendChild(sv('text', { x: xr, y: H - 3, 'text-anchor': 'end', 'class': 't5' }, 'now'));
    if (!pts.length) svg.appendChild(sv('text', { x: Lp + 4, y: TOP + 12, 'class': 't5' }, 'no level history today'));
    return svg;
  }

  /* ===================== console: open now, open breaches, health ===================== */
  function renderOpenNow() {
    var host = $('openNow');
    clear(host);
    var rows = M.state && Array.isArray(M.state.open_now) ? M.state.open_now : [];
    $('openCount').textContent = rows.length ? plural(rows.length, 'item') + ' · levels in order, not to scale' : '';
    if (!rows.length) { host.appendChild(el('p', 't4 c none', 'Nothing open.')); return; }
    var W = fitWidth(host, 340) - 26;
    rows.forEach(function (r) {
      var c = el('div', 'card');
      var head = el('div', 'ch');
      head.appendChild(badge(r.kind === 'order' ? 'ORD' : 'POS', r.kind === 'order' ? '' : 'ok'));
      var idn = el('span', 't5', str(r.id));
      idn.title = str(r.adapter) + ' ' + (r.kind === 'order' ? 'order id' : 'position id');
      head.appendChild(idn);
      head.appendChild(el('span', 't5 s', str(r.adapter)));
      var st = el('span', 'st');
      st.appendChild(badge(str(r.status), r.status === 'OPEN' || r.status === 'WORKING' ? 'ok' : ''));
      head.appendChild(st);
      c.appendChild(head);
      var b = el('div', 'cb');
      var what = el('div', 'what');
      what.appendChild(el('span', 't3', str(r.symbol) + ' ' + str(r.side)));
      b.appendChild(what);
      var notes = [];
      if (level(r.stop) === null) notes.push('no stop');
      [['entry', r.entry], ['stop', r.stop], ['take-profit', r.take_profit]].forEach(function (p) { if (p[1] === NOT_SHOWN) notes.push(p[0] + ' (not shown)'); });
      var lf = el('div', 'fit');
      lf.appendChild(ladderSvg(r.entry, r.stop, r.take_profit, W));
      b.appendChild(lf);
      if (notes.length) b.appendChild(el('div', 't5 s', notes.join(' · ')));
      if (Array.isArray(r.history) && r.history.length) {
        b.appendChild(el('div', 't5 s', 'stop / take-profit since 00:00'));
        var sf = el('div', 'fit');
        sf.appendChild(levelSpark(r.history, r.entry, W, false));
        b.appendChild(sf);
      } else {
        b.appendChild(el('div', 't5 s', 'no level changes since 00:00'));
      }
      c.appendChild(b);
      if (typeof r.story === 'string' && r.story) clickable(c, function () { openDrawer(r.story); }, 'Open the story of ' + str(r.symbol) + ' ' + str(r.side) + ' ' + str(r.id));
      else c.className += ' nolink';
      host.appendChild(c);
    });
  }
  function escalationText(b) {
    if (b.capped || b.cap === 'SOFT') return ['capped: will not escalate', 'ok'];
    if (isNum(b.escalate_min)) return ['HARD in about ' + b.escalate_min + ' min unless fixed', 'warn'];
    return null;
  }
  function renderBreaches() {
    var host = $('breaches');
    clear(host);
    var rows = M.state && Array.isArray(M.state.open_breaches) ? M.state.open_breaches : [];
    $('breachCount').textContent = rows.length ? String(rows.length) : '';
    if (!rows.length) { host.appendChild(el('p', 't4 c none', 'No open breaches.')); return; }
    rows.forEach(function (b) {
      var c = el('div', 'card breach ' + str(b.level));
      var head = el('div', 'ch');
      var rt = el('span', 't3');
      rt.appendChild(el('span', 'idt', str(b.rule_id)));
      head.appendChild(rt);
      var st = el('span', 'st');
      st.appendChild(badge(str(b.level), levelTone(b.level)));
      if (b.cap && b.cap !== 'NONE') st.appendChild(badge('cap ' + b.cap, ''));
      if (b.acked) st.appendChild(badge('acked', 'ok'));
      if (b.repages > 0) st.appendChild(badge('re-paged ' + b.repages + '×', 'warn'));
      head.appendChild(st);
      c.appendChild(head);
      var kv = el('dl', 'kv');
      function row(k, node) { kv.appendChild(el('dt', 't5 s', k)); var dd = el('dd'); dd.appendChild(node); kv.appendChild(dd); }
      var subj = (b.subject_type === 'POSITION' || b.subject_type === 'ORDER')
        ? [str(b.symbol), str(b.side), str(b.subject_id)].filter(Boolean).join(' ')
        : 'account ' + str(b.subject_id);
      row('subject', richText(el('span', 't4 c'), subj));
      var code = str(b.reason_code) || str(b.rule_id);
      var why = el('span', 't4 c', gloss(code));
      row('reason', why);
      if (b.reason_code) row('code', el('span', 't5', str(b.reason_code)));
      if (isNum(b.since_ms)) row('since', timeEl(b.since_ms, 't5'));
      var esc = escalationText(b);
      if (esc) row('escalate', el('span', 't4 c tone-' + esc[1], esc[0]));
      c.appendChild(kv);
      if (typeof b.story === 'string' && b.story) clickable(c, function () { openDrawer(b.story); }, 'Open the story of breach ' + str(b.rule_id));
      else c.className += ' nolink';
      host.appendChild(c);
    });
  }
  var HEALTH = [['sweep_pass_age_s', 'RiskSweep pass age', ' s'], ['alerts_waiting', 'Alerts waiting', ''],
    ['dlq_queued', 'DLQ queued', ''], ['open_breaches', 'Open breaches', '']];
  function renderHealth() {
    var host = $('health');
    clear(host);
    var s = M.series || {};
    var W = Math.max(130, Math.floor((fitWidth(host, 340) - 8) / 2) - 22);
    HEALTH.forEach(function (hd) {
      var pts = Array.isArray(s[hd[0]]) ? s[hd[0]] : [];
      var c = el('div', 'hcard');
      var r = el('div', 'row');
      r.appendChild(el('span', 't5 s', hd[1]));
      var lastV = null;
      pts.forEach(function (p) { if (Array.isArray(p) && isNum(p[1])) lastV = p[1]; });
      r.appendChild(el('span', 't5', lastV === null ? 'no data' : 'now ' + lastV + hd[2]));
      c.appendChild(r);
      c.appendChild(healthSpark(pts, W, hd[1]));
      host.appendChild(c);
    });
  }
  function healthSpark(pts, W, label) {
    var H = 34;
    var vals = pts.filter(function (p) { return Array.isArray(p) && isNum(p[0]); });
    var maxV = 1;
    vals.forEach(function (p) { if (isNum(p[1])) maxV = Math.max(maxV, p[1]); });
    var t1 = Date.now(), t0 = t1 - 60 * MIN;
    if (vals.length) { t0 = Math.min(t0, vals[0][0]); t1 = Math.max(t1, vals[vals.length - 1][0] + MIN); }
    function x(t) { return 1 + (t - t0) / Math.max(1, t1 - t0) * (W - 2); }
    function y(v) { return H - 2 - v / maxV * (H - 6); }
    var svg = sv('svg', { width: W, height: H, viewBox: '0 0 ' + W + ' ' + H, role: 'img', 'aria-label': label + ', last 60 minutes' });
    svg.appendChild(sv('line', { x1: 0, x2: W, y1: H - 1.5, y2: H - 1.5, 'class': 'sp-base' }));
    var d = '', open = false;
    vals.forEach(function (p) {
      if (!isNum(p[1])) { open = false; return; }
      d += (open ? 'L' : 'M') + x(p[0]).toFixed(1) + ' ' + y(p[1]).toFixed(1);
      open = true;
    });
    if (d) svg.appendChild(sv('path', { d: d, 'class': 'sp-health' }));
    return svg;
  }

  /* ===================== drawer ===================== */
  var drawerSeq = 0, drawerData = null, lastFocus = null;
  function openDrawer(key) {
    if (typeof key !== 'string' || !key) return;
    if (UI.drawerKey === null) lastFocus = document.activeElement;
    UI.drawerKey = key;
    $('drawer').hidden = false;
    $('scrim').hidden = false;
    drawerData = null;
    $('drawerBody').scrollTop = 0;
    renderDrawerShell(M.stories[key] || null, 'Loading…');
    loadDrawer(key);
  }
  function refreshDrawer() { if (UI.drawerKey) loadDrawer(UI.drawerKey); }
  function loadDrawer(key) {
    var seq = ++drawerSeq;
    fetch('/api/story?key=' + encodeURIComponent(key), { cache: 'no-store', credentials: 'same-origin' })
      .then(function (r) {
        if (r.status === 404) throw new Error('This story is no longer held by the live view.');
        if (r.status === 400) throw new Error('This story key is not valid.');
        if (!r.ok) throw new Error('The story could not be loaded (status ' + r.status + ').');
        return r.json();
      })
      .then(function (data) {
        if (seq !== drawerSeq || UI.drawerKey !== key) return;
        if (!data || !data.story) throw new Error('The story could not be read.');
        drawerData = data;
        renderDrawer(data);
      })
      .catch(function (err) {
        if (seq !== drawerSeq || UI.drawerKey !== key) return;
        if (drawerData) return;
        var msg = err && err.message && err.message.indexOf(' ') > 0 ? err.message : 'The live view server could not be reached.';
        renderDrawerShell(M.stories[key] || null, msg);
      });
  }
  function closeDrawer() {
    UI.drawerKey = null;
    drawerSeq++;
    $('drawer').hidden = true;
    $('scrim').hidden = true;
    if (lastFocus && lastFocus.focus) { try { lastFocus.focus(); } catch (e) { /* element gone */ } }
    lastFocus = null;
  }
  function drawerHead(h, key) {
    var head = $('drawerHead');
    clear(head);
    var box = el('div');
    var eb = el('div', 'eyebrow');
    var kd = h ? KIND[h.kind] : null;
    eb.appendChild(icon(kd ? kd.icon : 'link', 13));
    eb.appendChild(el('span', 't2', kd ? kd.label : 'Story'));
    if (h && h.state) eb.appendChild(badge(str(h.state), has(OPEN_STATES, h.state) ? 'info' : 'ok'));
    if (h && (h.worst === 'alert' || h.worst === 'warn')) eb.appendChild(badge(h.worst, sevTone(h.worst)));
    box.appendChild(eb);
    var t = richText(el('h2', 't3'), h ? (str(h.header) || key) : key);
    t.id = 'drawerTitle';
    box.appendChild(t);
    if (h && isNum(h.first_ms)) {
      var tm = el('div', 't5 s');
      tm.appendChild(timeEl(h.first_ms, 't5 s', true, 'first '));
      if (isNum(h.last_ms)) { tm.appendChild(txt(' · ')); tm.appendChild(timeEl(h.last_ms, 't5 s', true, 'last ')); }
      box.appendChild(tm);
    }
    head.appendChild(box);
    var x = btn('dr-x', el('span', 't4', '×'));
    x.setAttribute('aria-label', 'Close details');
    x.addEventListener('click', closeDrawer);
    head.appendChild(x);
    return x;
  }
  function renderDrawerShell(h, msg) {
    var x = drawerHead(h, UI.drawerKey);
    var body = $('drawerBody');
    clear(body);
    body.appendChild(el('p', 't4 dr-sec', msg));
    try { x.focus(); } catch (e) { /* ignore */ }
  }
  function sec(title) {
    var s = el('section', 'dr-sec');
    s.appendChild(el('h3', 't2', title));
    $('drawerBody').appendChild(s);
    return s;
  }
  function copyText(value) {
    try {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(value).then(function () { toast('Copied ' + value); }, function () { toast('Copy was refused: select the id instead.'); });
      } else {
        toast('Copy is not available here: select the id instead.');
      }
    } catch (e) {
      toast('Copy is not available here: select the id instead.');
    }
  }
  var toastTimer = null;
  function toast(msg) {
    var t = $('toast');
    clear(t);
    t.appendChild(el('span', 't4 c', msg));
    t.hidden = false;
    clearTimeout(toastTimer);
    toastTimer = setTimeout(function () { t.hidden = true; }, 2600);
  }
  function findBreach(ep) {
    var rows = M.state && Array.isArray(M.state.open_breaches) ? M.state.open_breaches : [];
    var parts = str(ep.episode).split('|');
    return rows.filter(function (b) {
      return b.rule_id === ep.rule_id && (parts.length < 3 || (b.subject_type === parts[1] && b.subject_id === parts[2]));
    })[0] || null;
  }
  function renderDrawer(data) {
    var h = data.story;
    var key = UI.drawerKey;
    var x = drawerHead(h, key);
    var body = $('drawerBody');
    clear(body);
    var events = Array.isArray(data.events) ? data.events.slice() : [];
    events.sort(function (a, b) { return (a.n || 0) - (b.n || 0); });

    /* related stories */
    var related = Array.isArray(h.related) ? h.related.filter(function (k) { return typeof k === 'string'; }) : [];
    if (related.length || h.merged_into) {
      var rs = sec('Linked stories');
      var rr = el('div', 'dr-row');
      related.forEach(function (k) { rr.appendChild(relChip(k)); });
      if (typeof h.merged_into === 'string' && h.merged_into) {
        rr.appendChild(el('span', 't5 s', h.held ? 'held for catch-up in' : 'merged into'));
        rr.appendChild(relChip(h.merged_into));
      }
      rs.appendChild(rr);
    }

    /* lines, oldest first */
    var ls = sec('Lines · ' + events.length);
    var ol = el('ol', 'lines');
    events.forEach(function (ev) {
      var held = typeof ev.story === 'string' && ev.story !== h.key && resolve(ev.story).held;
      ol.appendChild(lineEl(ev, held, h.badges));
    });
    if (!events.length) ol.appendChild(el('li', 't4 c', 'The server holds no lines of this story.'));
    ls.appendChild(ol);

    /* levels */
    var lv = h.levels || {};
    var hist = Array.isArray(h.history) ? h.history : [];
    if (h.kind === 'trade' || hist.length || lv.entry || lv.stop || lv.take_profit) {
      var vs = sec('Levels');
      var cur = el('div', 'dr-row');
      [['entry', lv.entry, 'tone-info'], ['stop', lv.stop, 'tone-alert'], ['take-profit', lv.take_profit, 'tone-ok']].forEach(function (p) {
        cur.appendChild(el('span', 't5 ' + p[2], p[0] + ' ' + (level(p[1]) || 'none')));
      });
      vs.appendChild(cur);
      var W = fitWidth(vs, 460) - 2;
      var lf = el('div', 'fit');
      lf.appendChild(ladderSvg(lv.entry, lv.stop, lv.take_profit, W));
      vs.appendChild(lf);
      vs.appendChild(el('div', 't5 s', 'level history since 00:00 (in level order, not to scale)'));
      var sf = el('div', 'fit');
      sf.appendChild(levelSpark(hist, lv.entry, W, true));
      vs.appendChild(sf);
      if (hist.length) {
        var tw = el('div', 'tabwrap');
        var tab = el('table', 'lvtab');
        var thead = el('thead');
        var hr = el('tr');
        ['time', 'entry', 'stop', 'take-profit'].forEach(function (c) { hr.appendChild(el('th', 't5 s', c)); });
        thead.appendChild(hr);
        tab.appendChild(thead);
        var tb = el('tbody');
        hist.slice().sort(function (a, b) { return a.at_ms - b.at_ms; }).forEach(function (p) {
          var tr = el('tr');
          var td0 = el('td');
          td0.appendChild(timeEl(p.at_ms, 't5'));
          tr.appendChild(td0);
          [p.entry, p.stop, p.take_profit].forEach(function (v) { tr.appendChild(el('td', 't5', level(v) || '—')); });
          tb.appendChild(tr);
        });
        tab.appendChild(tb);
        tw.appendChild(tab);
        vs.appendChild(tw);
      }
    }

    /* rule episodes */
    var eps = Array.isArray(h.episodes) ? h.episodes : [];
    if (eps.length) {
      var es2 = sec('Rule episodes');
      var timing = M.state && M.state.timing ? M.state.timing : {};
      eps.forEach(function (ep) {
        var box = el('div', 'epbox');
        var r1 = el('div', 'dr-row');
        var rt = el('span', 't3');
        rt.appendChild(el('span', 'idt', str(ep.rule_id)));
        r1.appendChild(rt);
        if (ep.level) r1.appendChild(badge(ep.level, levelTone(ep.level)));
        if (ep.cap && ep.cap !== 'NONE') r1.appendChild(badge('cap ' + ep.cap, ''));
        r1.appendChild(badge(ep.resolved ? 'resolved' : 'active', ep.resolved ? 'ok' : levelTone(ep.level)));
        if (ep.acked) r1.appendChild(badge('acked', 'ok'));
        box.appendChild(r1);
        var br = findBreach(ep);
        var code = br && br.reason_code ? br.reason_code : ep.rule_id;
        box.appendChild(el('p', 't4 c', gloss(code)));
        var r2 = el('div', 'dr-row');
        if (br && br.reason_code) r2.appendChild(el('span', 't5 s', 'code ' + br.reason_code));
        if (isNum(ep.since_ms)) r2.appendChild(timeEl(ep.since_ms, 't5 s', true, 'active since '));
        if (!ep.resolved) {
          if (ep.cap === 'SOFT') r2.appendChild(el('span', 't5 s tone-ok', 'capped: will not escalate'));
          else if (isNum(ep.escalate_min)) r2.appendChild(el('span', 't5 s tone-warn', 'HARD in about ' + ep.escalate_min + ' min unless fixed'));
        }
        if (isNum(timing.escalate_after_ms) && ep.level === 'SOFT') r2.appendChild(el('span', 't5 s', 'escalates after ' + dur(timing.escalate_after_ms) + ' uncapped'));
        if (isNum(timing.cool_down_ms)) r2.appendChild(el('span', 't5 s', 'cool-down ' + dur(timing.cool_down_ms)));
        if (ep.repages > 0) r2.appendChild(el('span', 't5 s tone-warn', 're-paged ' + ep.repages + '×' + (isNum(ep.last_repage_ms) ? ', last ' + fmtLocal(ep.last_repage_ms, false) : '')));
        box.appendChild(r2);
        es2.appendChild(box);
      });
    }

    /* alert delivery history */
    var badges = h.badges && typeof h.badges === 'object' ? h.badges : {};
    var sids = Object.keys(badges);
    if (sids.length) {
      var as = sec('Alert delivery');
      var bySid = {};
      events.forEach(function (ev) { bySid[ev.sid] = ev; });
      sids.forEach(function (sid) {
        var row = el('div', 'epbox');
        var ev = bySid[sid];
        var r1 = el('div', 'dr-row');
        if (ev) r1.appendChild(timeEl(lineTime(ev), 't5 s'));
        (Array.isArray(badges[sid]) ? badges[sid] : []).forEach(function (b, i) {
          if (i) r1.appendChild(el('span', 't5 s', '→'));
          var bt = BADGE_TEXT[b] || [str(b), ''];
          r1.appendChild(badge(bt[0], bt[1]));
        });
        row.appendChild(r1);
        row.appendChild(richText(el('p', 't4 c'), ev ? ev.title : 'line ' + sid + ' (no longer held)'));
        as.appendChild(row);
      });
    }

    /* outage / dlq facts */
    if (h.kind === 'outage' && isNum(h.gap_ms)) sec('Outage').appendChild(el('p', 't4 c', 'Gap ' + dur(h.gap_ms) + '.'));
    if (h.kind === 'dlq' && h.dlq_status) sec('DLQ').appendChild(el('p', 't4 c', 'Status ' + h.dlq_status + '.'));

    /* ids, each copyable */
    var ids = [], seen = {};
    function addIds(o) {
      if (!o || typeof o !== 'object') return;
      Object.keys(o).forEach(function (k) {
        var v = str(o[k]);
        if (!v || seen[k + '=' + v]) return;
        seen[k + '=' + v] = 1;
        ids.push([k, v]);
      });
    }
    addIds(h.ids);
    events.forEach(function (ev) { addIds(ev.ids); });
    var is = sec('Ids');
    ids.push(['story', h.key]);
    ids.forEach(function (p) {
      var row = el('div', 'idrow');
      row.appendChild(el('span', 't5 s', p[0]));
      row.appendChild(el('span', 'idc t5', p[1]));
      var cb = btn('copy', el('span', 't5 s', 'copy'));
      cb.setAttribute('aria-label', 'Copy ' + p[0] + ' ' + p[1]);
      cb.addEventListener('click', function () { copyText(p[1]); });
      row.appendChild(cb);
      is.appendChild(row);
    });
    try { if (document.activeElement === document.body || !document.activeElement) x.focus(); } catch (e) { /* ignore */ }
  }

  /* ===================== start ===================== */
  function init() {
    applyTheme(readTheme());
    $('themeBtn').addEventListener('click', function () {
      var next = theme === 'dark' ? 'light' : 'dark';
      applyTheme(next);
      saveTheme(next);
      mark('day');
    });
    ensureDefs();
    $('scrim').addEventListener('click', closeDrawer);
    document.addEventListener('keydown', function (ev) {
      if (ev.key === 'Escape' && UI.drawerKey) closeDrawer();
    });
    var rt = null;
    window.addEventListener('resize', function () {
      clearTimeout(rt);
      rt = setTimeout(function () { mark('day', 'console', 'health'); }, 150);
    });
    setInterval(function () { mark('status'); updateConn(); }, SEC);
    setInterval(function () { mark('day', 'health'); }, 30 * SEC);
    markAll();
    updateConn();
    loadSnapshot();
  }
  function updateConn() {
    var pw = pageWord();
    $('connSq').className = 'sq ' + pw[1];
    $('connTxt').textContent = pw[0].toUpperCase();
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
