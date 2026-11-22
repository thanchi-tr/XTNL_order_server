/* Edge (shell) page: docs/design/live-view.md §1.8. Phase 2 placeholder, not connected to anything that trades,
   decides or advises. It reads only GET /api/snapshot (state.halt and poll) for the status strip, at load and
   every 5 s, backing off from 1 s doubling to 30 s while the read fails. The gate tile is derived here and is
   never an input to the engine: it has one fixed value, DENY, and no code path yields another. */
(function () {
  'use strict';

  var GATE = 'DENY';
  var PERIOD_MS = 5000;
  var FIRST_BACKOFF_MS = 1000;
  var MAX_BACKOFF_MS = 30000;
  var STALE_MS = 10000;
  var SEP = ' · ';

  var backoff = FIRST_BACKOFF_MS;
  var failing = false;
  var last = null;

  function byId(id) { return document.getElementById(id); }
  function pad(n) { return (n < 10 ? '0' : '') + n; }
  function fmtLocal(ms) {
    var d = new Date(ms);
    return pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
  }
  function fmtUtc(ms) {
    var d = new Date(ms);
    return d.getUTCFullYear() + '-' + pad(d.getUTCMonth() + 1) + '-' + pad(d.getUTCDate()) + ' ' +
      pad(d.getUTCHours()) + ':' + pad(d.getUTCMinutes()) + ':' + pad(d.getUTCSeconds()) + ' UTC';
  }
  function isNum(x) { return typeof x === 'number' && isFinite(x); }

  /* the theme the live page stored; the page renders the same without storage */
  function applyStoredTheme() {
    var v = 'dark';
    try {
      v = window.localStorage.getItem('lv-theme') === 'light' ? 'light' : 'dark';
    } catch (e) {
      v = 'dark';
    }
    document.documentElement.setAttribute('data-theme', v);
  }

  function isCurrent() {
    return !failing && last !== null && last.poll !== null && isNum(last.poll.at_ms) &&
      Date.now() - last.poll.at_ms <= STALE_MS;
  }
  function haltLevel() {
    if (last === null || last.halt === null) return null;
    var h = last.halt;
    var lv = typeof h.effective_level === 'string' && h.effective_level ? h.effective_level : h.level;
    return lv === 'NONE' || lv === 'SOFT' || lv === 'HARD' ? lv : null;
  }
  function tone(lv) {
    return lv === 'HARD' ? 'alert' : lv === 'SOFT' ? 'warn' : lv === 'NONE' ? 'ok' : 'off';
  }

  /* ---- chrome shared with the live page: status strip, connection pill, data path bar (halt and poll only) ---- */
  var NS = 'http://www.w3.org/2000/svg';
  var GLYPH = { done: 'M3 8.5l3 3 7-7', attention: 'M8 2.5l6 11H2z', blocked: 'M4.5 4.5l7 7M11.5 4.5l-7 7',
                todo: 'M8 3.5a4.5 4.5 0 1 0 0 9 4.5 4.5 0 1 0 0-9z' };
  function el(tag, cls, text) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (text !== undefined) n.textContent = text;
    return n;
  }
  function glyph(state) {
    var s = document.createElementNS(NS, 'svg');
    s.setAttribute('viewBox', '0 0 16 16'); s.setAttribute('width', '11'); s.setAttribute('height', '11');
    s.setAttribute('aria-hidden', 'true'); s.setAttribute('class', 'ic'); s.setAttribute('fill', 'none');
    s.setAttribute('stroke', 'currentColor'); s.setAttribute('stroke-width', '1.6');
    s.setAttribute('stroke-linecap', 'round'); s.setAttribute('stroke-linejoin', 'round');
    var p = document.createElementNS(NS, 'path');
    p.setAttribute('d', GLYPH[state]);
    s.appendChild(p);
    return s;
  }
  function clearNode(n) { while (n.firstChild) n.removeChild(n.firstChild); }
  function ago(ms) {
    var s = Math.max(0, Math.round((Date.now() - ms) / 1000));
    return s < 60 ? s + ' s ago' : Math.round(s / 60) + ' min ago';
  }
  function renderChrome(current, lv) {
    var poll = last !== null ? last.poll : null;
    var page = failing ? ['server down', 'alert'] : current ? ['live', 'ok'] : ['connecting', 'warn'];
    var sq = byId('connSq');
    if (sq) sq.className = 'sq ' + page[1];
    var ct = byId('connTxt');
    if (ct) ct.textContent = page[0].toUpperCase();
    var tk = byId('ticker');
    if (tk) {
      clearNode(tk);
      var items = [['HALT', lv !== null && current ? lv : 'unknown', lv !== null && current ? tone(lv) : 'off'],
                   ['LAST POLL', poll !== null && isNum(poll.at_ms) ? ago(poll.at_ms) : 'none',
                    poll !== null && poll.db === 'down' ? 'alert' : ''],
                   ['PAGE', page[0], page[1]]];
      items.forEach(function (it) {
        var s = el('span', 'tk');
        s.appendChild(el('span', 't5 s', it[0]));
        s.appendChild(el('span', 't5 v' + (it[2] ? ' tone-' + it[2] : ''), it[1]));
        tk.appendChild(s);
      });
    }
    var path = byId('path');
    if (path) {
      clearNode(path);
      var eng = poll === null ? 'todo' : poll.engine === 'ok' ? 'done' : poll.engine === 'down' ? 'blocked' : 'todo';
      var db = poll === null ? 'todo' : poll.db === 'ok' ? 'done' : poll.db === 'down' ? 'blocked' : 'todo';
      var steps = [['Engine', eng, eng === 'done' ? 'answering' : eng === 'blocked' ? 'not responding' : 'not read'],
                   ['Database', db, db === 'done' ? 'reading' : db === 'blocked' ? 'unreachable' : 'not read yet'],
                   ['This page', page[1] === 'ok' ? 'done' : page[1] === 'alert' ? 'blocked' : 'attention', page[0]]];
      var worst = steps.some(function (x) { return x[1] === 'blocked'; }) ? 'alert'
        : steps.some(function (x) { return x[1] !== 'done'; }) ? 'warn' : 'ok';
      var lead = el('span', 'pb-lead');
      lead.appendChild(el('span', 'pb-dot ' + worst));
      lead.appendChild(el('span', 't5 s', 'DATA PATH'));
      path.appendChild(lead);
      var list = el('ol', 'pb-steps');
      steps.forEach(function (x, i) {
        if (i) { var ar = el('li', 'parr t5 s', '→'); ar.setAttribute('aria-hidden', 'true'); list.appendChild(ar); }
        var li = el('li', 'pstep ' + x[1]);
        li.appendChild(glyph(x[1]));
        li.appendChild(el('span', 't5 s pname', x[0]));
        li.appendChild(el('span', 't5 s pdet', x[2]));
        list.appendChild(li);
      });
      path.appendChild(list);
    }
  }

  function render() {
    var current = isCurrent();
    var lv = haltLevel();

    /* ActionGate: always the one fixed value, with the reasons the real gate also fails closed on */
    var reasons = ['Phase 1: no order sender exists'];
    if (lv === 'SOFT' || lv === 'HARD') reasons.push('halt ' + lv);
    if (last !== null && last.poll !== null && last.poll.db === 'down') reasons.push('database unreachable');
    if (!current) reasons.push('status not current');
    byId('gateValue').textContent = GATE;
    byId('gateWhy').textContent = reasons.join(SEP);

    /* Halt: the latest lv_halt_events row from the snapshot; unknown when missing or stale */
    var hv = byId('haltValue');
    var hm = byId('haltMeta');
    var tile = byId('haltTile');
    if (current && lv !== null) {
      var h = last.halt;
      hv.textContent = lv;
      hv.className = 'hero val tone-' + tone(lv);
      tile.className = 'tile halt ' + tone(lv);
      var meta = [];
      if (isNum(h.at_ms)) meta.push('since ' + fmtLocal(h.at_ms));
      if (h.hard_latch === true) meta.push('hard latch on');
      hm.textContent = meta.length ? meta.join(SEP) : 'from the latest halt row';
      hm.title = isNum(h.at_ms) ? fmtUtc(h.at_ms) : '';
    } else {
      hv.textContent = 'unknown';
      hv.className = 'hero val tone-off';
      tile.className = 'tile halt off';
      hm.textContent = current ? 'no halt row read yet' : 'status not current';
      hm.title = '';
    }
    renderChrome(current, lv);
  }

  function readSnapshot() {
    fetch('/api/snapshot')
      .then(function (r) {
        if (!r.ok) throw new Error('status ' + r.status);
        return r.json();
      })
      .then(function (snap) {
        var st = snap && typeof snap === 'object' && snap.state ? snap.state : null;
        last = {
          halt: st && st.halt && typeof st.halt === 'object' ? st.halt : null,
          poll: snap && snap.poll && typeof snap.poll === 'object' ? snap.poll : null
        };
        failing = false;
        backoff = FIRST_BACKOFF_MS;
        render();
        setTimeout(readSnapshot, PERIOD_MS);
      })
      .catch(function () {
        failing = true;
        render();
        var wait = backoff;
        backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
        setTimeout(readSnapshot, wait);
      });
  }

  function start() {
    applyStoredTheme();
    render();
    setInterval(render, 1000);
    readSnapshot();
  }
  /* loaded with defer, so the document is parsed */
  start();
})();
