/* Live stop adjustment prototypes — shared chrome and simulated behaviour.
   Design only: mock data (design 3, scenario D at the 11:00 bar), no network,
   nothing stored. Times are UTC bar times as in docs/design/arm-sl-sample/live-adjust.md. */
(function () {
  'use strict';
  var PAGES = [
    ['approve', 'approve.html', 'Approvals'],
    ['ownership', 'ownership.html', 'Stop ownership'],
    ['live', 'live-mode.html', 'Live mode'],
    ['alerts', 'alerts.html', 'Alerts'],
    ['notices', 'notices.html', 'Notices']
  ];
  var LOGO = '<svg class="brand-mark" viewBox="0 0 80 80" fill="none" aria-hidden="true"><defs><linearGradient id="lg" x1="24" y1="6" x2="58" y2="74" gradientUnits="userSpaceOnUse"><stop offset="0%" stop-color="#3dffb0"/><stop offset="55%" stop-color="#00cc7a"/><stop offset="100%" stop-color="#009e60"/></linearGradient></defs>' +
    '<path d="M40 29 L63 52 M40 29 L17 52" stroke="url(#lg)" stroke-width="2.2" stroke-opacity="0.55" stroke-linecap="round"/><path d="M63 52 L40 74 M17 52 L40 74" stroke="#00cc7a" stroke-width="2" stroke-opacity="0.24" stroke-linecap="round"/>' +
    '<line x1="18" y1="52" x2="62" y2="52" stroke="#00cc7a" stroke-opacity="0.34" stroke-width="1.3" stroke-dasharray="4 3"/><line x1="27" y1="8" x2="63" y2="52" stroke="url(#lg)" stroke-width="3.4" stroke-linecap="round"/>' +
    '<line x1="51" y1="13" x2="17" y2="52" stroke="url(#lg)" stroke-width="2.6" stroke-opacity="0.72" stroke-linecap="round"/><circle cx="63" cy="52" r="2" fill="#2fd0ff"/>' +
    '<circle cx="27" cy="8" r="4.4" fill="#00f090"/><circle cx="27" cy="8" r="2.4" fill="#e6fff4"/><circle cx="51" cy="13" r="3.7" fill="#00e688"/><circle cx="51" cy="13" r="2" fill="#e6fff4"/></svg>';
  var SERVER_ICON = '<svg viewBox="0 0 12 12" aria-hidden="true"><rect x="1.5" y="1.5" width="9" height="4" rx="1"/><rect x="1.5" y="6.5" width="9" height="4" rx="1"/></svg>';

  function $(sel, root) { return (root || document).querySelector(sel); }
  function $all(sel, root) { return Array.prototype.slice.call((root || document).querySelectorAll(sel)); }

  /* mock clock: offset so the page opens one second after the 11:00:11 decision */
  var offset = (11 * 3600 + 12) - Math.floor(Date.now() / 1000);
  function nowSec() { return Math.floor(Date.now() / 1000) + offset; }
  function setNow(s) { offset = s - Math.floor(Date.now() / 1000); }
  function hms(s) {
    s = ((s % 86400) + 86400) % 86400;
    var h = Math.floor(s / 3600), m = Math.floor(s % 3600 / 60), x = s % 60;
    return (h < 10 ? '0' : '') + h + ':' + (m < 10 ? '0' : '') + m + ':' + (x < 10 ? '0' : '') + x;
  }

  function chrome() {
    var page = document.body.getAttribute('data-page');
    var links = PAGES.map(function (p) {
      return '<a class="nav-link" href="' + p[1] + '"' + (p[0] === page ? ' aria-current="page"' : '') + '>' + p[2] + '</a>';
    }).join('');
    var nav = document.createElement('header');
    nav.className = 'nav';
    nav.innerHTML = '<a class="skip" href="#main">Skip to content</a><div class="wrap"><a class="brand" href="approve.html" aria-label="XTNL order-server live adjust">' + LOGO +
      '<span class="brand-word" aria-hidden="true">XTNL</span><span class="brand-sub">LIVE ADJUST</span></a>' +
      '<nav class="nav-links" aria-label="Pages">' + links + '</nav><span class="spacer"></span>' +
      '<span class="nav-pill hide-sm" title="Local only (D5). Phone access later through a tunnel.">LOCAL · 127.0.0.1</span>' +
      '<span class="nav-pill nav-pill--user hide-sm">operator · 28 min left</span></div>';
    var bar = document.createElement('div');
    bar.className = 'status-bar';
    bar.setAttribute('role', 'region');
    bar.setAttribute('aria-label', 'Engine status');
    /* safety values first, so they stay visible on narrow screens */
    bar.innerHTML = '<div class="wrap"><span class="sb-dot on" id="sbDot"></span><span class="sb-state" id="sbState">PHASE 3 · APPROVE</span>' +
      '<span class="sb-sep"></span><span class="sb-clock num" id="sbClock"></span><span class="sb-sep"></span><div class="sb-items">' +
      '<span class="sb-item"><span class="sb-label">Halt</span><span class="sb-value ok" id="sbHalt">NONE</span></span>' +
      '<span class="sb-item"><span class="sb-label">Dead-man</span><span class="sb-value ok" id="sbDead">ACK 4 s</span></span>' +
      '<span class="sb-item"><span class="sb-label">Open moves</span><span class="sb-value warn" id="sbOpen">1 awaiting you</span></span>' +
      '<span class="sb-item"><span class="sb-label">Gate</span><span class="sb-value ok" id="sbGate">OPEN · TIGHTEN</span></span>' +
      '<span class="sb-item"><span class="sb-label">Broker</span><span class="sb-value ok">STONEX · STREAMING</span></span></div></div>';
    var live = document.createElement('div');
    live.className = 'sr-only';
    live.id = 'sbLive';
    live.setAttribute('aria-live', 'polite');
    document.body.insertBefore(live, document.body.firstChild);
    document.body.insertBefore(bar, document.body.firstChild);
    document.body.insertBefore(nav, document.body.firstChild);
    var foot = document.createElement('footer');
    foot.className = 'wrap';
    foot.innerHTML = '<div class="foot"><span>XTNL · order-server · live adjust prototype (design 3, Phases 3–5)</span><span>Design only — nothing here sends anything. Phase 1 stays read-only.</span></div>';
    document.body.appendChild(foot);
    var cur = $('.nav-link[aria-current=page]');
    if (cur && cur.scrollIntoView) { cur.scrollIntoView({ inline: 'center', block: 'nearest' }); }
    function tick() { var c = $('#sbClock'); if (c) { c.textContent = hms(nowSec()) + ' UTC'; } }
    tick(); setInterval(tick, 1000);
  }

  /* status strip: update values; announce only safety-relevant changes */
  function setStrip(o, announce) {
    function set(id, text, tone) {
      var e = $(id); if (!e) { return; }
      e.textContent = text; e.classList.remove('ok', 'warn', 'bad'); if (tone) { e.classList.add(tone); }
    }
    if (o.state) { set('#sbState', o.state[0], o.state[1]); var d = $('#sbDot'); if (d) { d.className = 'sb-dot ' + (o.state[1] || 'on'); } }
    if (o.gate) { set('#sbGate', o.gate[0], o.gate[1]); }
    if (o.halt) { set('#sbHalt', o.halt[0], o.halt[1]); }
    if (o.dead) { set('#sbDead', o.dead[0], o.dead[1]); }
    if (o.open) { set('#sbOpen', o.open[0], o.open[1]); }
    if (announce) { var l = $('#sbLive'); if (l) { l.textContent = announce; } }
  }

  /* ------------------------------------------------------------ approvals */
  function approvals() {
    var card = $('#req'), ring = $('#ringArc'), secs = $('#ringSecs'), chip = $('#reqState');
    var steps = $all('#steps .step'), note = $('#reqNote'), acts = $('#reqActions');
    var srTime = $('#ringSr'), srAlert = $('#ringAlert'), srState = $('#reqLive');
    var C = 2 * Math.PI * 56;
    ring.setAttribute('stroke-dasharray', C.toFixed(1));
    var DECIDED = 11 * 3600 + 11, WINDOW = 60, EXPIRY = DECIDED + WINDOW;
    var timer = null, seqTimer = null, gen = 0, phase = null, lastSr = -1;
    var STEP_NAMES = ['Proposed', 'Awaiting you', 'Approved', 'Checking', 'Sending', 'Acked', 'Confirmed'];
    var GLYPH = { done: ['✓', 'done'], now: ['●', 'current'], fail: ['✕', 'failed'], todo: ['○', 'not reached'] };
    var STATE = {
      awaiting: ['▲ AWAITING YOU', 'chip chip-warn pulse'], approved: ['APPROVED', 'chip chip-info'], checking: ['CHECKING', 'chip chip-info'],
      sending: ['SENDING', 'chip chip-info'], acked: ['ACKED', 'chip chip-info'], confirmed: ['✓ CONFIRMED', 'chip chip-ok'],
      expired: ['○ EXPIRED', 'chip chip-muted'], rejected: ['✕ REJECTED BY YOU', 'chip chip-muted'], refused: ['✕ REFUSED', 'chip chip-bad']
    };
    function left() { return EXPIRY - nowSec(); }
    function cancelAll() { clearInterval(timer); clearTimeout(seqTimer); gen += 1; }
    function setSteps(n, failAt) {
      steps.forEach(function (s, i) {
        var st = failAt === i ? 'fail' : (i < n ? 'done' : (i === n ? 'now' : 'todo'));
        s.setAttribute('data-s', st);
        if (st === 'now') { s.setAttribute('aria-current', 'step'); } else { s.removeAttribute('aria-current'); }
        s.innerHTML = '<span aria-hidden="true">' + GLYPH[st][0] + '</span>' + STEP_NAMES[i] + '<span class="sr-only">, ' + GLYPH[st][1] + '</span>';
      });
    }
    function setState(s, html, say) {
      phase = s;
      card.setAttribute('data-state', s);
      chip.textContent = STATE[s][0];
      chip.className = STATE[s][1];
      if (html) { note.innerHTML = html; }
      if (s !== 'awaiting' && acts.contains(document.activeElement)) { var t = $('#reqTitle'); t.setAttribute('tabindex', '-1'); t.focus(); }
      acts.hidden = s !== 'awaiting';
      if (say) { srState.textContent = say; }
      var done = ['confirmed', 'expired', 'rejected', 'refused'].indexOf(s) > -1;
      var OPEN = { approved: '1 approved', checking: '1 checking', sending: '1 sending', acked: '1 acked · confirming' };
      setStrip({ open: done ? ['none', ''] : (s === 'awaiting' ? ['1 awaiting you', 'warn'] : [OPEN[s], '']) });
    }
    function draw() {
      var l = Math.max(left(), 0);
      secs.textContent = String(l);
      ring.setAttribute('stroke-dashoffset', (C * (1 - l / WINDOW)).toFixed(1));
      ring.classList.toggle('low', l <= 15);
      if (l % 10 === 0 && l !== lastSr) { lastSr = l; srTime.textContent = l + ' seconds left. Expires ' + hms(EXPIRY) + ' UTC.'; }
      if ([30, 15, 5].indexOf(l) > -1) { srAlert.textContent = l + ' seconds left to approve'; }
    }
    function start(secondsLeft, announce) {
      cancelAll();
      $all('#histBody tr.demo-row').forEach(function (r) { r.remove(); });
      setNow(EXPIRY - secondsLeft);
      $('#curStop').textContent = '1.09800';
      setSteps(1);
      setState('awaiting', 'Expires <b>' + hms(EXPIRY) + ' UTC</b>, 60 s after the bar-end decision. If you do nothing, the stop stays at 1.09800 and the next bar end (11:15:11) proposes again. Just before sending, the gate, the broker’s stop and the live price are checked again.', announce ? 'New approval request: move Order 1 stop to 1.10000.' : '');
      draw();
      var my = gen;
      timer = setInterval(function () {
        if (my !== gen) { return; }
        draw();
        if (left() <= 0) {
          cancelAll(); setSteps(1, 1);
          setState('expired', '<b>Expired at ' + hms(EXPIRY) + ' UTC.</b> Nothing was sent; the stop stays at 1.09800 at the broker. The 11:15 bar end will propose the move again, as a new request.', 'Expired. Nothing was sent.');
          addHistory(hms(EXPIRY), '○ EXPIRED', 'chip chip-muted', 'no answer in 60 s');
        }
      }, 1000);
    }
    function run(seq, done) {
      var i = 0, my = gen;
      (function next() {
        if (my !== gen) { return; }
        if (i >= seq.length) { done(); return; }
        var s = seq[i++];
        if (s[1] === 'checking' && left() <= 0) {
          setSteps(3, 3);
          setState('expired', '<b>Expired during the checks</b> (' + hms(EXPIRY) + ' UTC). Nothing was sent; the stop stays at 1.09800.', 'Expired. Nothing was sent.');
          addHistory(hms(EXPIRY), '○ EXPIRED', 'chip chip-muted', 'expired after approval, before send');
          return;
        }
        setSteps(s[0]); setState(s[1], s[2], s[3]); seqTimer = setTimeout(next, s[4]);
      })();
    }
    $('#btnApprove').addEventListener('click', function () {
      if (phase !== 'awaiting' || left() <= 0) { return; }
      cancelAll();
      var at = hms(nowSec());
      run([
        [2, 'approved', 'Approved by you at ' + at + ' UTC (audit #4418).', 'Approved.', 500],
        [3, 'checking', 'Checking: gate (stop tighten, full freshness checks); the broker’s current stop is still 1.09800; the bid is at least the minimum gap above 1.10000 (the larger of min_gap_r·R, the broker’s minimum stop distance and one increment); still the newest decision.', 'Checking.', 900],
        [4, 'sending', 'Checks passed. The move is recorded first, then one in-place amend of the stop is sent (price only, never a quantity).', 'Sending.', 900],
        [5, 'acked', 'StoneX accepted. Waiting for the stream to show the stop at 1.10000 (confirm window 30 s).', 'Broker accepted.', 1100]
      ], function () {
        setSteps(7);
        setState('confirmed', '<b>Moved.</b> The broker shows the stop at 1.10000 (break-even); take profit: none, unchanged. The 11:15:11 bar end should see it at target, and the position is released at about 11:16:11, after the verification re-read.', 'Confirmed: stop at 1.10000.');
        $('#curStop').textContent = '1.10000';
        addHistory(hms(nowSec()), '✓ CONFIRMED', 'chip chip-ok', 'approved ' + at + ' by you');
      });
    });
    $('#btnReject').addEventListener('click', function () {
      if (phase !== 'awaiting') { return; }
      cancelAll(); setSteps(1, 1);
      setState('rejected', '<b>Rejected by you.</b> Nothing was sent. The server keeps advising; the next bar end may propose again.', 'Rejected. Nothing was sent.');
      addHistory(hms(nowSec()), '✕ REJECTED', 'chip chip-muted', 'by you');
    });
    function addHistory(t, text, cls, why) {
      var tb = $('#histBody'), tr = document.createElement('tr');
      tr.className = 'demo-row';
      tr.innerHTML = '<td data-label="Time">' + t + '</td><td data-label="Intent">7f3a91c2</td><td data-label="Order">Order 1 · EURUSD long</td><td data-label="Stop">1.09800 → 1.10000</td><td data-label="Outcome"><span class="' + cls + '">' + text + '</span></td><td class="wrap-cell" data-label="Detail">' + why + '</td>';
      tb.insertBefore(tr, tb.firstChild);
    }
    $all('[data-demo]').forEach(function (b) {
      b.addEventListener('click', function () {
        var d = b.getAttribute('data-demo');
        if (d === 'restart') { start(59, true); }
        if (d === 'low') { start(12, true); }
        if (d === 'refuse') {
          if (phase !== 'awaiting') { return; }
          cancelAll(); var at = hms(nowSec());
          setSteps(3, 3);
          setState('refused', '<b>Refused at check: price through level.</b> You approved at ' + at + ' UTC, but the bid had already fallen to within the minimum gap of 1.10000, so moving the stop there could close the position at once. Nothing was sent; the stop stays at 1.09800.', 'Refused at check. Nothing was sent.');
          addHistory(hms(nowSec()), '✕ REFUSED', 'chip chip-bad', 'price through level (checked after your approval)');
        }
      });
    });
    start(59, false);
  }

  /* ------------------------------------------------------------ ownership */
  function ownership() {
    var drawer = $('#drawer'), back = $('#backdrop'), reason = $('#reason'), err = $('#reasonErr'), opener = null;
    var page = [$('main'), $('header.nav'), $('.status-bar'), $('footer')];
    function setInert(on) { page.forEach(function (el) { if (el) { el.inert = on; if (on) { el.setAttribute('aria-hidden', 'true'); } else { el.removeAttribute('aria-hidden'); } } }); }
    function open(btn) {
      opener = btn;
      $('#hbOrder').textContent = btn.getAttribute('data-order');
      reason.value = ''; err.textContent = ''; reason.removeAttribute('aria-invalid');
      drawer.hidden = false; void drawer.offsetWidth;
      drawer.classList.add('open'); back.classList.add('open'); setInert(true);
      setTimeout(function () { reason.focus(); }, 60);
    }
    function close(focusEl) {
      drawer.classList.remove('open'); back.classList.remove('open'); setInert(false);
      setTimeout(function () { if (!drawer.classList.contains('open')) { drawer.hidden = true; } }, 300);
      var f = focusEl || (opener && document.body.contains(opener) ? opener : $('#ownH'));
      if (f && !f.matches('button, a, textarea, input')) { f.setAttribute('tabindex', '-1'); }
      if (f) { f.focus(); }
      opener = null;
    }
    $all('[data-handback]').forEach(function (b) { b.addEventListener('click', function () { open(b); }); });
    $('#hbCancel').addEventListener('click', function () { close(); });
    back.addEventListener('click', function () { close(); });
    drawer.addEventListener('keydown', function (e) {
      if (e.key === 'Escape') { close(); return; }
      if (e.key !== 'Tab') { return; }
      var f = $all('textarea, button', drawer), first = f[0], last = f[f.length - 1];
      if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
      else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
    });
    reason.addEventListener('input', function () { err.textContent = ''; reason.removeAttribute('aria-invalid'); });
    $('#hbSubmit').addEventListener('click', function () {
      if (!opener || !drawer.classList.contains('open')) { return; }
      if (reason.value.trim().length < 4) {
        reason.setAttribute('aria-invalid', 'true');
        err.textContent = ''; err.textContent = '✕ Enter a short reason first (it is kept in the audit).';
        reason.focus(); return;
      }
      var row = $('#row-' + opener.getAttribute('data-row'));
      row.classList.remove('human');
      $('.owner', row).innerHTML = '<span class="who who--server">' + SERVER_ICON + 'Server</span>';
      $('.since', row).textContent = 'handed back ' + hms(nowSec()) + ' UTC';
      $('.will', row).textContent = 'propose moves from the next bar end (only tighter)';
      var act = $('.act', row);
      act.innerHTML = '<span class="chip chip-ok">✓ request applied</span>';
      $('#ownMeta').textContent = '3 positions · ' + $all('tr.human').length + ' yours';
      $('#sbLive').textContent = 'Stop handed back to the server.';
      close(act);
    });
  }

  /* ------------------------------------------------------------ live mode */
  function liveMode() {
    var PHASES = { 1: 'PHASE 1 · ADVICE', 3: 'PHASE 3 · APPROVE', 4: 'PHASE 4 · AUTO', 5: 'PHASE 5 · AUTONOMOUS' };
    var STATES = {
      normal: { banner: null, phase: 3, live: true,
        strip: { gate: ['OPEN · TIGHTEN', 'ok'], halt: ['NONE', 'ok'], dead: ['ACK 4 s', 'ok'], open: ['1 awaiting you', 'warn'] },
        tiles: [['OPEN', 'ok', 'stop tighten passes all freshness checks'], ['NONE', 'ok', 'Oversee level'], ['ACK 4 s', 'ok', 'off-PC receiver']],
        moves: [1, 0, 0, 0, 0], say: 'Normal. Gate open.' },
      hard: { banner: ['banner banner--bad', '✕ HARD HALT — nothing is sent (D3). An approval is still recorded, but the move is held until it expires; you are paged. Stops at the broker are unchanged.'],
        phase: 3, live: true,
        strip: { gate: ['DENIED · HARD_HALT', 'bad'], halt: ['HARD', 'bad'], dead: ['ACK 3 s', 'ok'], open: ['1 awaiting you · would be held', 'bad'] },
        tiles: [['DENIED', 'bad', 'hard_halt: every action'], ['HARD', 'bad', 'latched until an operator clears it'], ['ACK 3 s', 'ok', 'off-PC receiver']],
        moves: [1, 0, 0, 0, 0], say: 'HARD halt. Nothing is sent.', label: 'HARD HALT · NOTHING SENT' },
      resume: { banner: ['banner banner--warn', '▲ SAFE_HOLD auto-resumed 11:42:05 after a crash loop (2 of 6 in 24 h). Live moves resume with the engine (D6) — but only if ARM is not FAILED(unclean_restarts); if it is, moves stay blocked until you clear it.'],
        phase: 3, live: true,
        strip: { gate: ['SETTLING', 'warn'], halt: ['NONE', 'ok'], dead: ['ACK 2 s', 'ok'], open: ['none', ''] },
        tiles: [['SETTLING', 'warn', 'opens after SM8 READY, term start + 22 s, reconcile and catch-up'], ['NONE', 'ok', 'Oversee level'], ['ACK 2 s', 'ok', 'off-PC receiver']],
        moves: [0, 0, 0, 0, 0], say: 'SAFE_HOLD auto-resumed. Gate settling.', label: 'RESUMED · SETTLING' },
      dead: { banner: ['banner banner--bad', '✕ Dead-man receiver has not acknowledged for 2 min — live moves dropped to Phase 1 advice and unsent moves were cancelled (dead_man_lost). Raise the phase again once it acknowledges.'],
        phase: 1, live: true, label: 'ADVICE ONLY · DEAD-MAN LOST',
        strip: { gate: ['OPEN · TIGHTEN', 'ok'], halt: ['NONE', 'ok'], dead: ['NO ACK 124 s', 'bad'], open: ['none', ''] },
        tiles: [['OPEN', 'ok', 'but live moves are off'], ['NONE', 'ok', 'Oversee level'], ['NO ACK', 'bad', '124 s since the last ack']],
        moves: [0, 0, 0, 0, 0], say: 'Dead-man receiver lost. Advice only.' },
      unknown: { banner: ['banner banner--bad', '✕ 1 stop move outcome UNKNOWN after a leader change — operator needed. It is never re-sent; that stop is blocked until resolved.'],
        phase: 3, live: true,
        strip: { gate: ['OPEN · TIGHTEN', 'ok'], halt: ['NONE', 'ok'], dead: ['ACK 5 s', 'ok'], open: ['1 operator needed', 'bad'] },
        tiles: [['OPEN', 'ok', 'stop tighten passes all freshness checks'], ['NONE', 'ok', 'Oversee level'], ['ACK 5 s', 'ok', 'off-PC receiver']],
        moves: [0, 0, 0, 0, 1], say: 'Operator needed: send outcome unknown.', label: 'OPERATOR NEEDED' }
    };
    var banners = $('#banners'), sw = $('#liveSwitch'), current = 3, liveOn = true, curKey = 'normal';
    function cancelUnsent() {
      var t = $all('#moveTiles .tile');
      [0, 1].forEach(function (i) { $('.tile-value', t[i]).textContent = '0'; t[i].removeAttribute('data-tone'); });
      var op = Number($('.tile-value', t[3]).textContent) + Number($('.tile-value', t[4]).textContent);
      setStrip({ open: op ? [op + ' operator needed', 'bad'] : ['none', ''] });
    }
    function render(stateTone, stateLabel) {
      var st = STATES[curKey];
      stateLabel = stateLabel || st.label;
      stateTone = stateTone || (curKey === 'hard' || curKey === 'unknown' || curKey === 'dead' ? 'bad' : (curKey === 'resume' ? 'warn' : ''));
      var t4 = $all('#gateTiles .tile')[3];
      $('#liveSub').textContent = !liveOn ? 'Off: all unsent moves were cancelled (mode_off). The server keeps advising.' : (curKey === 'dead' && current === 1 ? 'Dropped to advice: unsent moves were cancelled (dead_man_lost).' : 'Switching off cancels every move not yet sent, in one transaction.');
      sw.setAttribute('aria-checked', String(liveOn));
      t4.setAttribute('data-tone', liveOn && current !== 1 ? 'ok' : 'bad');
      $('.tile-value', t4).textContent = liveOn && current !== 1 ? 'ON' : 'OFF';
      $('.tile-sub', t4).textContent = liveOn && current !== 1 ? PHASES[current].toLowerCase() : 'advice only';
      $all('[data-phase]').forEach(function (b) { var on = b.getAttribute('data-phase') === String(current); b.setAttribute('aria-checked', String(on)); b.tabIndex = on ? 0 : -1; });
      var label = curKey === 'dead' && current === 1 ? stateLabel : (!liveOn || current === 1 ? 'ADVICE ONLY' : (stateLabel || PHASES[current]));
      setStrip({ state: [label, stateTone || (current === 1 || !liveOn ? 'warn' : '')] });
    }
    function apply(k, quiet) {
      var s = STATES[k];
      curKey = k;
      banners.innerHTML = '';
      if (s.banner) { var b = document.createElement('div'); b.className = s.banner[0]; if (!quiet) { b.setAttribute('role', 'alert'); } b.textContent = s.banner[1]; banners.appendChild(b); }
      current = s.phase; liveOn = s.live;
      $('#phaseNote').textContent = $('[data-phase="' + current + '"]').getAttribute('data-note');
      setStrip(s.strip, quiet || s.banner ? '' : s.say);
      $all('#gateTiles .tile').slice(0, 3).forEach(function (tile, i) {
        tile.setAttribute('data-tone', s.tiles[i][1]);
        $('.tile-value', tile).textContent = s.tiles[i][0];
        $('.tile-sub', tile).textContent = s.tiles[i][2];
      });
      $all('#moveTiles .tile').forEach(function (tile, i) {
        var n = s.moves[i]; $('.tile-value', tile).textContent = String(n);
        if (n) { tile.setAttribute('data-tone', i >= 3 ? 'bad' : 'warn'); } else { tile.removeAttribute('data-tone'); }
      });
      $all('[data-state-btn]').forEach(function (b) { b.setAttribute('aria-pressed', String(b.getAttribute('data-state-btn') === k)); });
      render(k === 'hard' || k === 'unknown' || k === 'dead' ? 'bad' : (k === 'resume' ? 'warn' : ''), s.label);
    }
    $all('[data-state-btn]').forEach(function (b) { b.addEventListener('click', function () { apply(b.getAttribute('data-state-btn')); }); });
    function choose(b) {
      var prev = current;
      current = Number(b.getAttribute('data-phase'));
      $('#phaseNote').textContent = b.getAttribute('data-note');
      if (current < prev) { cancelUnsent(); }
      render(); b.focus();
      $('#sbLive').textContent = 'Phase set to ' + PHASES[current].toLowerCase() + '.';
    }
    $all('[data-phase]').forEach(function (b) {
      b.addEventListener('click', function () { choose(b); });
      b.addEventListener('keydown', function (e) {
        var all = $all('[data-phase]'), i = all.indexOf(b);
        /* arrow keys only move focus; Space or Enter (a click) selects, so a phase never changes by accident */
        var j = -1;
        if (e.key === 'ArrowRight' || e.key === 'ArrowDown') { j = (i + 1) % all.length; }
        if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') { j = (i - 1 + all.length) % all.length; }
        if (j > -1) { e.preventDefault(); all.forEach(function (x, n) { x.tabIndex = n === j ? 0 : -1; }); all[j].focus(); }
      });
    });
    sw.addEventListener('click', function () {
      liveOn = !liveOn;
      if (liveOn && current === 1) { current = 3; $('#phaseNote').textContent = $('[data-phase="3"]').getAttribute('data-note'); }
      if (!liveOn) { cancelUnsent(); }
      render();
      $('#sbLive').textContent = liveOn ? 'Live moves on.' : 'Live moves off. Unsent moves cancelled.';
    });
    apply('normal', true);
  }

  /* ------------------------------------------------------------ alerts */
  function alerts() {
    var master = $('#routineMaster'), rows = $all('.routine .switch');
    function phase() { return document.body.getAttribute('data-phase'); }
    function sync() {
      var p5 = phase() === '5';
      if (!p5) {
        if (!master.hasAttribute('data-p5')) { master.setAttribute('data-p5', master.getAttribute('aria-checked')); }
        master.setAttribute('aria-checked', 'true'); master.disabled = true;
        rows.forEach(function (s) { if (!s.hasAttribute('data-p5')) { s.setAttribute('data-p5', s.getAttribute('aria-checked')); } s.setAttribute('aria-checked', 'true'); s.disabled = true; });
        $('#routineState').textContent = 'Phase 3–4: all routine notices are sent';
        return;
      }
      if (master.hasAttribute('data-p5')) {
        master.setAttribute('aria-checked', master.getAttribute('data-p5')); master.removeAttribute('data-p5');
        rows.forEach(function (s) { if (s.hasAttribute('data-p5')) { s.setAttribute('aria-checked', s.getAttribute('data-p5')); s.removeAttribute('data-p5'); s.disabled = false; } });
      }
      var on = master.getAttribute('aria-checked') === 'true';
      master.disabled = false;
      rows.forEach(function (s) {
        var enable = p5 && on;
        if (!on) { if (!s.disabled) { s.setAttribute('data-prev', s.getAttribute('aria-checked')); } s.setAttribute('aria-checked', 'false'); }
        else if (s.disabled && s.hasAttribute('data-prev')) { s.setAttribute('aria-checked', s.getAttribute('data-prev')); s.removeAttribute('data-prev'); }
        if (!p5) { s.setAttribute('aria-checked', 'true'); }
        s.disabled = !enable;
      });
      $('#routineState').textContent = !p5 ? 'Phase 3–4: all routine notices are sent' : (on ? 'Routine notices on' : 'Routine notices off — safety alerts still reach you');
    }
    master.addEventListener('click', function () { if (master.disabled) { return; } master.setAttribute('aria-checked', String(master.getAttribute('aria-checked') !== 'true')); sync(); });
    rows.forEach(function (s) { s.addEventListener('click', function () { if (!s.disabled) { s.setAttribute('aria-checked', String(s.getAttribute('aria-checked') !== 'true')); } }); });
    $all('[data-show-phase]').forEach(function (b) {
      b.addEventListener('click', function () {
        document.body.setAttribute('data-phase', b.getAttribute('data-show-phase'));
        $all('[data-show-phase]').forEach(function (x) { x.setAttribute('aria-pressed', String(x === b)); });
        setStrip({ state: [b.getAttribute('data-show-phase') === '5' ? 'PHASE 5 · AUTONOMOUS' : 'PHASE 3 · APPROVE', ''] });
        sync();
      });
    });
    sync();
  }

  document.addEventListener('DOMContentLoaded', function () {
    chrome();
    var page = document.body.getAttribute('data-page');
    if (page === 'approve') { approvals(); }
    if (page === 'ownership') { ownership(); }
    if (page === 'live') { liveMode(); }
    if (page === 'alerts') { alerts(); }
  });
})();
