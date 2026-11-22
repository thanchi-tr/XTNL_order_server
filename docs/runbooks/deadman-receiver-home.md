# Dead-man receiver on a home VPS (Tailscale)

Phase 1 never sends orders. The receiver only listens for the engine's heartbeat and pages; it never touches
PostgreSQL, Redis or a broker. A page from it means oversight may be blind, never that an order is at risk.

This is the single-PC (home server) layout of the dead-man's switch, from `docs/design/home-server-plan.md` §3.1
and work item W6. The pages themselves and what to do about them are in [deadman-alert.md](deadman-alert.md).

```
home PC engine (leader) --15 s POST + Bearer--> Tailscale --> VPS: tailscale serve :443 -> 127.0.0.1:8443 receiver --> Pushover + SMTP
                        --15 s copy (XTNL_HEARTBEAT_COPY_URL)--> Healthchecks.io check A (silence backstop)
VPS 1-min timer: curl 127.0.0.1:8443/healthz, then ping Healthchecks.io check B (receiver or VPS down)
```

**Unverified:** these steps were written from the plan and the repo, not yet run on a real VPS. In particular
`systemd-analyze verify` of the two units, the `tailscale serve` command form, the first-request certificate
delay and the tag policy in step 2 (its grant and `ssh` syntax, tagging the home PC from the admin console while
it runs unattended, and the refusal checks of step 3) were not tried. Tick each step when you have run it, and fix this page where reality differs.

## Trigger

- Building the home receiver for the first time (a P1 gate in the plan, before relying on the home PC).
- Rebuilding it: the VPS was lost, reinstalled or moved to another provider.
- Healthchecks.io **check B** is down: the receiver, the VPS or its internet stopped answering.
- Rotating the bearer token (see "Token rotation" below).

## Prerequisites

- A 1 GB Linux VPS **outside the house**, in Sydney or Melbourne (Vultr or DigitalOcean), running
  **Ubuntu 24.04 or Debian 13**. The repo needs **Python 3.12** or later (`pyproject.toml`): Ubuntu 24.04 ships
  3.12, Debian 13 ships 3.13. Avoid Oracle Always Free, which reclaims idle VMs.
- A Tailscale account (free Personal plan) with the home PC already in the tailnet (plan §4.10).
- A Pushover account and application token (pages repeat until acknowledged), and an SMTP relay account. Both are
  sent **from the VPS**, so they work while the house is dark. Avoid public ntfy.sh topics.
- The receiver's Pushover application token and SMTP relay login must be **its own**, never the engine's
  (`XTNL_PUSH_TOKEN`, `XTNL_SMTP_USERNAME` / `XTNL_SMTP_PASSWORD`). The engine's sit in the home PC's Windows User
  environment; if the PC is stolen they leak, and revoking them must not silence the off-site alarm
  ([home-backup-restore.md](home-backup-restore.md) Rotation, "Theft of the PC"). Create a second Pushover
  application and a second relay login for the VPS.
- Three Healthchecks.io checks: A (heartbeat copy, period 1 min, grace 2 min), B (this receiver, period 1 min,
  grace 3 min) and C (backups). Their ping URLs are secrets: anyone holding one can keep that check green.
- Never copy `deploy/env/local.env` or any other env file to the VPS: get the code with `git clone` (env files are
  ignored by git), never by copying the working tree.

## Install

### 1. Base system, chrony and Python

The receiver rejects beats more than 300 s off its clock, so the VPS clock must be synced. A clock step from
chrony (for example after the VPS is migrated or resumed) does not cause false pages: the receiver times silence
and every "for > N s" condition on its monotonic clock.

```bash
sudo apt update && sudo apt full-upgrade -y
sudo apt install -y chrony curl git python3 python3-venv python3-pip
sudo systemctl enable --now chrony
chronyc tracking                     # "Leap status : Normal", system time within a few ms
python3 --version                    # 3.12 or later
# optional: make time-sync.target wait for chrony (the unit starts After=time-sync.target)
sudo systemctl enable chrony-wait.service 2>/dev/null || true
```

### 2. Tailscale, MagicDNS, HTTPS certificates and a neutral name

The home PC and the VPS are **tagged** (review round 1). The home PC is signed in under your own login, so
without a tag it counts as one of "your own devices": every rule for your user (the default `*` rule, an `ssh`
rule for your user, Tailscale's default SSH rule with `root`) would also let it reach every port of the VPS and
log in there. A compromised home PC could then stop the receiver and the check B timer, or read their env files:
the two alarms meant to catch exactly that PC. A tagged device is no longer a member device, so rules for your
own devices leave it out.

First, in the Tailscale admin console:

- DNS: turn on **MagicDNS** and **HTTPS certificates**;
- Access controls: **replace the whole policy** (the default allows everything, and its SSH rule allows `root`)
  with the policy below. Keep only rules you added on purpose for other devices; none may name
  `tag:xtnl-receiver` as a source, or `tag:xtnl-home` / `tag:xtnl-receiver` with `*`. Put your VPS login name in
  place of `<user>`:

```json
{
  "tagOwners": {
    "tag:xtnl-home":     ["autogroup:admin"],
    "tag:xtnl-receiver": ["autogroup:admin"]
  },
  "grants": [
    // the home PC: the receiver's HTTPS port only
    {"src": ["tag:xtnl-home"], "dst": ["tag:xtnl-receiver"], "ip": ["tcp:443"]},
    // your own (untagged) devices: HTTPS, and port 22 for Tailscale SSH (it also needs the ssh rule below)
    {"src": ["autogroup:member"], "dst": ["tag:xtnl-receiver"], "ip": ["tcp:443", "tcp:22"]}
  ],
  "ssh": [
    // your own devices only (the tagged home PC is not a member), never root, a fresh login each time
    {"action": "check", "src": ["autogroup:member"], "dst": ["tag:xtnl-receiver"], "users": ["<user>"]}
  ]
}
```

  Nothing goes from `tag:xtnl-receiver` to the home PC. If you reach the home PC from your laptop or phone, add
  that as its own grant with `"dst": ["tag:xtnl-home"]` and only the ports you need.
- Machines: on the home PC, **Edit ACL tags** and set `tag:xtnl-home` (a tagged device needs no key expiry
  setting: tags turn key expiry off).

Then on the VPS:

```bash
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up --ssh --hostname=vps-a --advertise-tags=tag:xtnl-receiver
tailscale status                     # vps-a, tagged tag:xtnl-receiver
```

- **Use a neutral machine name** such as `vps-a`. Certificate names are published in public
  Certificate Transparency logs, so the name must not say what the machine does or whose it is.
- The receiver's address becomes `https://vps-a.<tailnet>.ts.net`, with a Let's Encrypt certificate that httpx
  (the engine's client) trusts. No public port and no domain are needed.

### 3. Close public SSH (use Tailscale SSH)

First prove Tailscale SSH works **from another tailnet device**, keeping the provider's web console as the
break-glass path:

```bash
ssh <user>@vps-a                     # from your laptop on the tailnet: must work before you go on
ssh root@vps-a                       # from your laptop: must be refused (the policy never allows root)
```

And **from the home PC** (PowerShell), SSH must be refused and only 443 must answer:

```powershell
ssh <user>@vps-a                                                      # must be refused
(Test-NetConnection vps-a -Port 22 -WarningAction SilentlyContinue).TcpTestSucceeded     # False
(Test-NetConnection vps-a -Port 8443 -WarningAction SilentlyContinue).TcpTestSucceeded   # False
```

(Port 443 answers only after step 5; check it there.) If the home PC gets in, its tag is missing or a rule still
names your user or `*` for it: fix the policy before you go on.

Then, on the VPS:

```bash
sudo systemctl disable --now ssh.socket ssh.service 2>/dev/null || sudo systemctl disable --now ssh.service
```

and in the provider's cloud firewall allow **no inbound** traffic at all (Tailscale needs outbound only).

### 4. The code and the receiver

```bash
sudo mkdir -p /opt/xtnl && sudo chown "$USER" /opt/xtnl
git clone <your repo remote> /opt/xtnl/order-server
cd /opt/xtnl/order-server
python3 -m venv .venv
.venv/bin/pip install -e .
sudo chmod -R o+rX /opt/xtnl/order-server     # the unit runs as a dynamic user, which must read the code
```

Env file, from the "Home VPS" block of `deploy/env/deadman-receiver.env.example`:

```bash
sudo install -d -m 0755 /etc/xtnl
sudo install -m 0600 -o root -g root deploy/env/deadman-receiver.env.example /etc/xtnl/deadman-receiver.env
sudoedit /etc/xtnl/deadman-receiver.env
```

Set:

- `XTNL_RECEIVER_LISTEN=127.0.0.1:8443`;
- `XTNL_RECEIVER_TOKEN`: a new random token of at least 16 characters, for example from
  `python3 -c "import secrets; print(secrets.token_urlsafe(32))"` run on the VPS (never paste it into a chat, a
  ticket or a log). The same value becomes the home PC's `XTNL_DEADMAN_TOKEN`;
- `XTNL_RECEIVER_TLS_CERT=` and `XTNL_RECEIVER_TLS_KEY=` empty (blank counts as unset), `XTNL_RECEIVER_CERTS=`
  empty, `XTNL_SECRETS_SOURCE=env`;
- Pushover: `XTNL_RECEIVER_PUSH_PROVIDER=pushover`, `XTNL_RECEIVER_PUSH_TOKEN`, `XTNL_RECEIVER_PUSH_USER`;
- SMTP: `XTNL_RECEIVER_ALERT_EMAIL_TO` and the `XTNL_RECEIVER_SMTP_*` lines; **delete or comment out every
  `XTNL_RECEIVER_SES_*` line** (SES wins over SMTP when its region is set);
- keep `XTNL_RECEIVER_TEST_PAGE_UTC=22:00` (08:00 or 09:00 Melbourne), `XTNL_RECEIVER_REPAGE_S=900` and
  `XTNL_RECEIVER_TWO_ENGINES_WINDOW_S=60`.

Unit (a plain copy of the Melbourne unit without `LoadCredential=` and the TLS lines, installed under the usual
name so every command in `deadman-alert.md` works):

```bash
sudo install -m 0644 deploy/systemd/xtnl-deadman-receiver.home.service /etc/systemd/system/xtnl-deadman-receiver.service
sudo systemd-analyze verify /etc/systemd/system/xtnl-deadman-receiver.service   # no output = fine
sudo systemctl daemon-reload
sudo systemctl enable --now xtnl-deadman-receiver
sudo journalctl -u xtnl-deadman-receiver -n 20 --no-pager | grep -E "config ok|refused"
curl -s http://127.0.0.1:8443/healthz       # ok
```

The unit runs `--check-config` as `ExecStartPre`; its line names only the listen address, `http (loopback)`,
which channels are on and the token source, never a value. Expect `email=on, push=on, tokens from env`. A
`configuration refused` line names the variables to fix. The check is not fatal by itself (`ExecStartPre=-`):
`RestartPreventExitStatus=78` covers only the main process, so a failing check would be retried every 5 s for
ever. The main process then refuses the same configuration with exit 78 (a second `configuration refused` line)
and the unit stays failed, not restarted, until you fix the env file and run `sudo systemctl restart
xtnl-deadman-receiver`. A `receiver started` page arrives on both channels within seconds.

### 5. tailscale serve 443 -> 127.0.0.1:8443

```bash
sudo tailscale serve --bg --https=443 http://127.0.0.1:8443
tailscale serve status                       # https://vps-a.<tailnet>.ts.net (tailnet only) -> http://127.0.0.1:8443
```

`--bg` keeps the configuration across reboots. Never use `tailscale funnel`: that would publish the receiver on
the internet. The first HTTPS request can take several seconds while the certificate is issued.

### 6. The healthz timer (Healthchecks.io check B)

```bash
sudo install -m 0600 -o root -g root deploy/env/deadman-healthz.env.example /etc/xtnl/deadman-healthz.env
sudoedit /etc/xtnl/deadman-healthz.env        # XTNL_HEALTHZ_PING_URL = check B's ping URL
sudo install -m 0644 deploy/systemd/xtnl-deadman-healthz.service deploy/systemd/xtnl-deadman-healthz.timer /etc/systemd/system/
sudo systemd-analyze verify /etc/systemd/system/xtnl-deadman-healthz.service /etc/systemd/system/xtnl-deadman-healthz.timer
sudo systemctl daemon-reload
sudo systemctl enable --now xtnl-deadman-healthz.timer
sudo systemctl start xtnl-deadman-healthz.service && sudo journalctl -u xtnl-deadman-healthz -n 5 --no-pager
```

Every minute it GETs `http://127.0.0.1:8443/healthz`; on 200 it pings check B, otherwise check B's `/fail` URL
(Healthchecks.io then pages at once) and the run exits 1. When the VPS or its internet is down nothing pings, and
check B pages after its grace time. The ping URL is never logged: the journal only says which step failed.

### 7. The home PC

- `deploy/env/local.env`: `XTNL_DEADMAN_URL=https://vps-a.<tailnet>.ts.net/xtnl/heartbeat`.
- Windows User environment (as `xtnl`): `XTNL_DEADMAN_TOKEN` (the receiver's token) and `XTNL_HEARTBEAT_COPY_URL`
  (check A's ping URL). Type secrets at a prompt so they stay out of the PowerShell history:

```powershell
$t = Read-Host 'XTNL_DEADMAN_TOKEN' -AsSecureString
[Environment]::SetEnvironmentVariable('XTNL_DEADMAN_TOKEN', [Runtime.InteropServices.Marshal]::PtrToStringBSTR([Runtime.InteropServices.Marshal]::SecureStringToBSTR($t)), 'User')
Remove-Variable t
```

- Leave `XTNL_HEARTBEAT_COPY_TOKEN` unset: it would be sent to Healthchecks.io. The start-up warning about it is
  harmless here.
- Restart the engine to read them: `deploy\local\stop-xtnl.ps1`, then `Start-ScheduledTask 'XTNL engine'`.

## Checks

The engine also pages when this receiver stops acknowledging its beats: `PAGE[high] dead-man receiver not
acknowledging` after `XTNL_DEADMAN_ALERT_AFTER_S` (default 300 s), and `PAGE[info] dead-man receiver acknowledging
again` on recovery. What to check for that page: [deadman-alert.md](deadman-alert.md) "Receiver not acknowledging
(engine page)".

On the VPS:

```bash
systemctl status xtnl-deadman-receiver xtnl-deadman-healthz.timer
sudo journalctl -u xtnl-deadman-receiver --since "-30 min" --no-pager | grep -E 'WARNING|ERROR|CRITICAL'   # 'wrong bearer token' = token mismatch, 'rejected:' = fields or clock, 'page via' = paging failure
sudo journalctl -u xtnl-deadman-receiver --since "-5 min" | grep -c 'wrong bearer token'   # 0 (the receiver logs no status codes)
sudo journalctl -u xtnl-deadman-healthz --since "-10 min" --no-pager
chronyc tracking
tailscale status && tailscale serve status
curl -s http://127.0.0.1:8443/healthz
```

Do not filter the receiver's journal with `-p warning`. The receiver writes its log lines to stderr with no
syslog priority prefix, so journald stores every line, warnings included, at priority info, and `-p warning`
shows none of them. Grep for the level word instead, as above (JSON lines carry `"level":"WARNING"`).

Every `journalctl` in this runbook runs with `sudo` (review round 6). The receiver and the healthz timer are
system units, and `<user>` is usually not in the `adm` or `systemd-journal` group, so without `sudo` journalctl
prints `-- No entries --`. That means **no access, not a clean journal**: `grep -c` then prints a false 0 and the
WARNING grep shows nothing. If you see `-- No entries --` for a unit that has run, add `sudo`.

From the home PC:

```powershell
curl.exe -s -o NUL -w "%{http_code}`n" https://vps-a.<tailnet>.ts.net/healthz                 # 200
curl.exe -s -o NUL -w "%{http_code}`n" -X POST https://vps-a.<tailnet>.ts.net/xtnl/heartbeat  # 401 (no token)
```

The 401 probe itself leaves one `missing or wrong bearer token refused` line in the receiver journal (review
round 3). So a `grep -c 'wrong bearer token'` over a window that holds the probe prints 1, not 0: that is the
probe, not a token mismatch. The token checks below count only from a time noted after any probe.

## End-to-end test

Run all of it once after the install, and again after any rebuild.

1. **Beats:** with the engine running, on the VPS note the time (after any 401 probe from "Checks"), wait 1
   minute (four beats), then count the refusals since then:

   ```bash
   t=$(date '+%Y-%m-%d %H:%M:%S'); sleep 60
   sudo journalctl -u xtnl-deadman-receiver --since "$t" | grep -c 'wrong bearer token'     # 0
   ```

   It prints 0, and the journal shows `heartbeat authenticated with the current token` (logged once, at the first
   good beat after a receiver start or a token switch). That line and a clean engine log (below) are the proof;
   the 0 only shows that no beat was refused in that minute. The receiver never logs an HTTP status code, so do not
   look for `401` there; on the home PC, the engine log has no `dead-man heartbeat to ... failed` or
   `refused the heartbeat bearer token` line (that is where `HTTP 401` shows). Healthchecks.io check A's pings do
   not prove the token: the engine sends that copy whatever the receiver answers. The status line in any page
   shows `last beat N s ago from <node> epoch <e>`.
2. **Silence page:** run `deploy\local\stop-xtnl.ps1`. Pushover and email both page `no heartbeat for N s`
   within about 60-75 s; Healthchecks.io check A follows after about 3-4 minutes. Start the task again
   (`Start-ScheduledTask 'XTNL engine'`): a `resolved:` notice follows the first beat.
3. **House dark:** unplug the home internet link (or the PC). The VPS still pages on both channels: that proves
   the channels do not depend on the house.
4. **Receiver down:** `sudo systemctl stop xtnl-deadman-receiver`. Within a minute the timer sends check B's
   `/fail` ping and Healthchecks.io pages. `sudo systemctl start xtnl-deadman-receiver`; check B turns green at
   the next minute (a `receiver started` page also arrives).
5. **Daily test page:** the next 22:00 UTC brings `daily test page` on both channels. A missing one is an incident.
6. **Two engines (W19):** nothing to provoke. On the single home PC a `beating node changed` notice
   (`node_changed`, high) is unexpected; `two engines beating` (`two_engines`, critical) means a second engine
   runs (an old desktop, a restored spare): follow "Two engines" in `deadman-alert.md` at once. When you move to
   a new box, expect `node_changed` once only if its `XTNL_NODE_ID` differs from the old one. When you change
   `XTNL_NODE_ID` on the same box (for example `pc1 -> home1`, as `start-xtnl.ps1` advises) and restart, one
   `node_changed` notice is also expected (review round 6): it clears after 5 min and needs no action. Only a
   change you did not make points to a second machine.

## Token rotation

The receiver here reads its tokens from the env file (`XTNL_SECRETS_SOURCE=env`) and the engine reads
`XTNL_DEADMAN_TOKEN` from the Windows User environment, both only at start, so each step takes a restart. Always
in this order (the receiver first; never change the engine first, or every beat is refused and the VPS pages
"no heartbeat"):

1. VPS: add `XTNL_RECEIVER_TOKEN_NEXT=<new>` to `/etc/xtnl/deadman-receiver.env` (keep `XTNL_RECEIVER_TOKEN=<old>`),
   then `sudo systemctl restart xtnl-deadman-receiver`. Both tokens are now accepted.
2. Home PC: change the Windows User variable `XTNL_DEADMAN_TOKEN` to `<new>` (the prompt above).
3. Home PC: run `deploy\local\stop-xtnl.ps1`, then `Start-ScheduledTask 'XTNL engine'`. The receiver journal
   shows `authenticated with the next token`. Then, as in End-to-end step 1, note the time on the VPS, wait 1
   minute and count from it (a 401 probe run shortly before would otherwise count):
   `sudo journalctl -u xtnl-deadman-receiver --since "$t" | grep -c 'wrong bearer token'` prints 0.
4. VPS: Promote the new token: set `XTNL_RECEIVER_TOKEN=<new>`, delete the `XTNL_RECEIVER_TOKEN_NEXT` line, then
   `sudo systemctl restart xtnl-deadman-receiver`. A restart forgets the overlap, so the old token is refused at
   once, which is fine: the engine already uses the new one.

Each receiver restart takes about 5 s, so at most one beat fails; the receiver pages only after 60 s of silence,
so a rotation done in order pages nothing (step 3's engine restart may page `no heartbeat` if the engine takes
longer than 60 s to lead again: expected, it resolves at the first beat).

## Rollback

- **Back to the stop-gap (P1 only):** set `XTNL_DEADMAN_URL` on the home PC back to the placeholder in
  `deploy/env/local-live.env.example` and keep the Healthchecks.io copy (check A) as the only silence alarm. Since
  W1 the cost is more than the error line every 15 s in the engine log (plan §3.1 "Stop-gap" predates W1): after
  `XTNL_DEADMAN_ALERT_AFTER_S` (default 300 s) the leader sends a PAGE[high] "dead-man receiver not
  acknowledging". Expect **one such page per engine start that comes more than 6 h after the previous page** (a
  weekly Windows Update restart, a power cut): a new term inherits the open episode from `notification_dedupe`
  only while its last page is under 6 h old. The live view and `xtnl_deadman_consecutive_failures` show the
  failing beats in every term. Episodes are kept per receiver host, so when you later point `XTNL_DEADMAN_URL` at
  a real receiver, no "acknowledging again" PAGE[info] comes for the placeholder's episode (its row stays in
  `notification_dedupe`; harmless). With the token removed, every start also logs the ERROR "XTNL_DEADMAN_URL is
  set but XTNL_DEADMAN_TOKEN is empty". To have no beat, no page and no heartbeat copy at all, leave `XTNL_DEADMAN_URL`
  empty instead (`xtnl_deadman_configured` 0): then check A goes red, because the copy is sent only with a
  dead-man URL. Build the VPS again before relying on the box.
- **Rebuild:** a new VPS from this page; the receiver keeps no state, so nothing is lost but the current
  conditions (it pages them again after start-up). Remove the old machine from the tailnet.
- **Undo the install on a VPS:**

```bash
sudo systemctl disable --now xtnl-deadman-healthz.timer xtnl-deadman-receiver
sudo tailscale serve reset
sudo rm /etc/systemd/system/xtnl-deadman-receiver.service /etc/systemd/system/xtnl-deadman-healthz.service /etc/systemd/system/xtnl-deadman-healthz.timer
sudo systemctl daemon-reload
```

Pause check B in Healthchecks.io first, or it pages when the timer stops.
