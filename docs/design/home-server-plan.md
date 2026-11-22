# Home server plan

Running the order-server 24/7 on a dedicated PC at home in Melbourne.

**Plan only.** Nothing in this document has been built or changed yet. Phase 1 stays read-only. All prices are **USD**. "(est.)" marks an estimate or a price that is not confirmed. Prices moved a lot in 2026 (SSD and RAM shortage), so check every line before buying. No account values appear here.

How this plan was made: four research passes (repo needs, hardware, off-site pieces, Windows set-up), one draft, two adversarial reviews (repo facts and operations / cost). Every review finding is folded in below.

---

## 1. Summary

**What.** Move the order-server from the everyday desktop to a small, quiet PC that does nothing else and stays on.

Today's single-PC design stays as it is:
- Windows 11 Pro and Docker Desktop;
- `deploy/local/start-xtnl.ps1`, started by the `AtLogOn` scheduled task from `install-autostart.ps1`;
- `XTNL_SINGLE_PC=1`, which gives SAFE_HOLD auto-resume (D6) and downtime catch-up.

No engine code changes are needed to run it.

Two pieces live **outside** the house, because nothing inside a dark house can report that the house is dark:
1. **A dead-man receiver** on a small VPS (the existing `src/deadman_receiver`), with a free Healthchecks.io backstop.
2. **Encrypted, locked backups** in Backblaze B2.

**Why.**
- An everyday desktop gets slept, rebooted and used. A dedicated box doesn't.
- About USD 12–20 a month to run, against about USD 750–900 for the designed cloud setup.
- Latency doesn't matter for 15-minute bar decisions: Melbourne to London is about 250–300 ms.

**Cost** (details in §8):

| | One-off | Monthly |
|---|---|---|
| Recommended | about 600–1,225 (est.) | about 12–20 (est.) |
| Recommended + cold spare PC | about 930–1,705 (est.) | about 12–20 |
| Budget | about 475–890 (est.) | about 12–20 |

**Which phases it fits:**

| Phase | Fit |
|---|---|
| **P1 read-only (now)** | **Yes**, once the §6 "before relying on it" items are done: off-site receiver, nightly off-site backup, safe secret handling, the etcd rebuild fix, and the old desktop switched off. |
| **P3 (you approve each move, 60 s)** | **Yes, with conditions.** It needs the STOP_TIGHTEN gate, WAL-level backups and the approval page. On a box with no screen, the approval page is only usable through the phone tunnel (D5 part 2), or an RDP session kept open through every trading window. Pick one before P3. |
| **P4 (automatic + notify)** | **Only conditionally**: it needs M10 `dead_man_lost` (drop to advice when the receiver stops acknowledging). Read §7 first. |
| **P5 (autonomous)** | **No.** One box in one house has no failover. A second machine or the cloud comes first. |

---

## 2. Hardware

### 2.1 The PC

| | **Recommended: refurbished business "Tiny", Intel vPro** | **Budget: Intel N150 mini PC** |
|---|---|---|
| Examples | Lenovo ThinkCentre M90q Gen 2/3, Dell OptiPlex 7090 Micro (vPro i5/i7). An AMD Lenovo M75q Gen 2 also works, but has no vPro (see "Why vPro") | Beelink EQ14, ASUS NUC 14 Essential N150 |
| CPU | 6+ cores | 4 cores (enough for this load) |
| RAM | **32 GB already fitted.** Adding memory later costs more than the price gap between units | 16 GB (the N150's official maximum; workable with WSL capped at about 6 GB) |
| SSD | **1 TB TLC NVMe with DRAM, 600 TBW or more.** Most refurbished units ship with 256 GB, so expect to buy one | 500 GB–1 TB TLC NVMe, often included |
| Network | Wired 1 GbE. Never Wi-Fi | 2.5 GbE |
| Power on after AC loss | Dependable BIOS option | Present, but owners report it failing or resetting. **Pull the plug to test it** |
| Idle at the wall | about 9–12 W | about 6–8 W |
| Price | **about 330–480 (est.)**: refurbished 16 GB / 256 GB units list at about 313–330 | **about 190–260 (est.)** |

**Why vPro.**
- If Windows hangs, or BitLocker stops at its recovery screen after a firmware or Secure Boot update, neither Tailscale nor RDP is running.
- Intel AMT (vPro) gives remote keyboard and screen at the BIOS and BitLocker screens. Reach it through a Tailscale subnet router, either on the 4G router (RutOS has a Tailscale package) or on a Raspberry Pi.
- **Without vPro:** add a smart plug with local control (§2.4) to power-cycle the box remotely, and accept that a BitLocker recovery screen needs someone at the box.

**Skip:**
- **ECC RAM:** rare in mini PCs. PostgreSQL data checksums, the integrity checks and off-site backups cover the risk.
- **A NAS as main storage:** it would burn along with the server.

**What the repo needs** (measured on today's desktop):
- **Today (MD and ARM off):** about 2 GB. postgres about 82 MiB, redis 11 MiB, etcd 67 MiB, engine about 106 MB, WSL VM about 950 MB.
- **Worst case (MD and ARM on):** about 10–12 GB. Engine up to 3–4 GB (it runs on Windows Python, not inside WSL), redis-md capped at 2 GB, PostgreSQL 0.5–1 GB, Windows 3–4 GB.
- **Disk:** PostgreSQL grew 72.5 MB in 8 days, roughly 1–2 GB a year (extrapolated). The redis-md AOF can reach 1–2 GB.
- **SSD wear:** writes are light. Even 20 GB a day with 3× write amplification is about 22 TB a year, decades on a 600 TBW drive.

### 2.2 UPS

| Item | Choice |
|---|---|
| **Pick** | A CyberPower line-interactive UPS of about 1000 VA with USB that **PowerPanel Personal** supports (for example the VP1000ELCD). About **110–170 (est.)** |
| **Outlets** | **4 battery-backed outlets** if the internet is FTTN/FTTB (PC + router + NBN box + VDSL modem). Otherwise 3 is enough, or add a small switch |
| **Load** | About 35–40 W for PC + router + NBN box. Runtime probably 60 min or more. **Unverified: test it once** |
| **Battery** | The VP series battery is listed as not user-replaceable. If you want to swap batteries every 3–5 years, pick a CyberPower PR or APC Back-UPS Pro model |

**One shutdown path, configured once.** Use PowerPanel Personal, not Windows' built-in battery shutdown:
1. **Shut down at a runtime threshold** (for example 10 minutes left), not a battery %, because the battery % a UPS reports is often non-linear.
2. **"Run command before shutdown"** points at `deploy\local\stop-xtnl.ps1`, so the engine drains cleanly first. Check the model supports this option before buying.
3. **"Turn off UPS output after shutdown": on.** Otherwise, if mains returns after Windows has shut down but before the battery runs flat, the PC never sees power drop and never powers back on.

Why not Windows' own shutdown: it can't tell the UPS to cut its output, so step 3 isn't possible. If you use it anyway, W12 becomes a small watcher script (§6).

**Backstop if the box stays off anyway:** BIOS RTC wake (§4.1) powers it on once a day, and Wake-on-LAN from the router works too.

### 2.3 Internet failover

**Which NBN types survive a blackout:**
- **FTTP:** the NBN box can run on its optional battery (up to about 5 h). Your router needs the UPS.
- **HFC, FTTN/FTTB and fixed wireless:** these die when the street equipment loses power, whatever your UPS does.
- **FTTN/FTTB** also needs a separate VDSL modem in bridge mode in front of a 4G router, which takes the 4th UPS outlet.

**4G failover router.** The server needs very little bandwidth.

| Router | Price | Notes |
|---|---|---|
| **TP-Link Archer MR600** | about 155–175 (est.) | NBN as primary, 4G SIM as failover (about 10 s to switch in one review). Turn failover on |
| **Teltonika RUT241** | about 199 (confirmed at US sellers) | Industrial build. Only 2 Ethernet ports: put it in front of the server only, or use the MR600 |
| Telstra Smart Modem backup | | Plan-dependent, 25/5 cap; Telstra says it may not work in a power outage |

- **SIM:** use a monthly **recurring** data plan. Prepaid SIMs that expire fail silently. About 5–10 a month (est.).
- **Measure first:** watch a week of server traffic before choosing the allowance. It is probably well under 1 GB a day with market data off, but that is unmeasured.
- **Not a phone hotspot:** it leaves the house with you.
- **Limit:** in a wide blackout the towers also run on limited batteries. 4G covers hours, not days.
- **Clock on 4G:** the engine's clock check is fragile on 4G until W11 is done (§6).

### 2.4 Small extras

- **Smart plug with local control** (if the PC has no vPro): about 15–25 (est.). Cycling the plug forces a reboot, because the BIOS powers on after AC loss. Keep it off any cloud account and control it locally through Tailscale.
- **Paper and a password manager** for the BitLocker recovery key and the backup decryption key (§3.2).

---

## 3. Off-site pieces

### 3.1 Dead-man receiver

**What the engine already does.**
- The leader only, while its lease is valid, POSTs JSON to `XTNL_DEADMAN_URL` every 15 s (`src/runtime/deadman.py`).
- The receiver hears nothing in every failure case: SAFE_HOLD, the clock wait, Docker down, the PC off, no internet. That is correct.
- **Single-PC mode doesn't require the URL** (`src/runtime/config.py:57`), but it sends whenever the URL is set. Today `local-live.env.example:38` points at a dead placeholder.
- **`XTNL_DEADMAN_TOKEN` must be set but is not checked at start.** It is not in `$RequiredUserVars` (`start-xtnl.ps1:121`). Without it no Authorization header is sent, and the receiver refuses every beat. The first sign is 401 lines in the log.

**Layout.**

```
home PC engine (leader) --15 s POST + Bearer--> Tailscale --> VPS: tailscale serve :443 -> 127.0.0.1:8443 receiver --> Pushover + SMTP
                        --15 s copy (XTNL_HEARTBEAT_COPY_URL)--> Healthchecks.io check A (silence backstop)    --> Pushover + email
VPS 1-min timer: curl 127.0.0.1:8443/healthz, then ping Healthchecks.io check B (receiver or VPS down)
backup job ---------------------------------------------------> Healthchecks.io check C (backup freshness)
```

| Item | Choice | Cost |
|---|---|---|
| **Where** | A 1 GB Linux VPS outside the house: Vultr or DigitalOcean, Sydney or Melbourne. **Ubuntu 24.04 or Debian 13**, because the repo needs Python 3.12 or later. Install chrony: the receiver rejects beats more than 300 s off. Close public SSH and use Tailscale SSH. Avoid Oracle Always Free, which reclaims idle VMs | about 5–6/month (est.) |
| **Receiver** | `python -m src.deadman_receiver`, installed with `pip install -e .`. Use a **plain local copy** of `deploy/systemd/xtnl-deadman-receiver.service` without the `LoadCredential=` and TLS lines, rather than a drop-in that tries to clear them. Check it with `systemd-analyze verify`. `MemoryMax=256M` | – |
| **How it is reached** | Tailscale (free Personal plan). In the admin console, turn on **MagicDNS and HTTPS certificates**. `tailscale serve` then gives `https://<name>.<tailnet>.ts.net` with a Let's Encrypt certificate that httpx trusts. No public port and no domain. The receiver listens on plain `127.0.0.1:8443`, which `settings.py:124` allows without TLS files. Turn off key expiry on both machines. **Use a neutral machine name** (for example `vps-a`): certificate names are published in public Certificate Transparency logs | 0 |
| **Paging** | Pushover (repeats until acknowledged) plus an SMTP relay, both sent from the VPS so they work while the house is dark. Avoid public ntfy.sh topics | Pushover 4.99 once per platform |
| **Backstop** | Healthchecks.io free plan as `XTNL_HEARTBEAT_COPY_URL`. Check A: period 1 min, grace 2 min. 15 s beats are 4 pings a minute, just under its 5-a-minute limit per check, so don't shorten the interval and never add retries. Its URL is a secret: anyone holding it can keep check A green | 0 |
| **Timings** | The receiver pages about 60–75 s after the last beat, re-pages every 900 s, and sends a daily test page at 22:00 UTC (08:00 or 09:00 Melbourne). Healthchecks.io follows after about 3–4 min | – |

**Config only (no code):**
- **VPS env file**, started from `deploy/env/deadman-receiver.env.example`:
  - `XTNL_RECEIVER_LISTEN=127.0.0.1:8443`;
  - `XTNL_RECEIVER_TOKEN`, at least 16 characters;
  - empty `XTNL_RECEIVER_CERTS` and TLS variables (blank counts as unset, `settings.py:158-160`);
  - Pushover through `XTNL_RECEIVER_PUSH_*` and SMTP through `XTNL_RECEIVER_SMTP_*`;
  - `XTNL_SECRETS_SOURCE=env`.
- **Home PC:**
  - `XTNL_DEADMAN_URL=https://<name>.<tailnet>.ts.net/xtnl/heartbeat` in `deploy/env/local.env`;
  - `XTNL_DEADMAN_TOKEN` and `XTNL_HEARTBEAT_COPY_URL` in the Windows User environment (after W2).
- **Leave `XTNL_HEARTBEAT_COPY_TOKEN` unset.** Never put a real secret in it, because it would go to Healthchecks.io. The start-up warning about it (`wiring.py:289-294`) is harmless noise here.
- **Token rotation.** Both ends read the token only at start, so the `_NEXT` steps in `deadman-alert.md`, which are written for `aws-sm`, need restarts here:
  1. Add `XTNL_RECEIVER_TOKEN_NEXT` on the VPS and restart the receiver.
  2. Change the Windows User variable.
  3. Run `stop-xtnl.ps1`, then `Start-ScheduledTask 'XTNL engine'`.
  4. Promote the new token on the receiver and restart it again.

**Healthchecks.io can never be the "receiver acknowledges" signal P4 needs.** It answers 200 without evaluating anything.

**Stop-gap while the VPS isn't ready (P1 only).** The heartbeat copy is sent even when the main beat fails, but only while `XTNL_DEADMAN_URL` is set (`session.py:1649-1652`, `deadman.py:352-355`). So you can keep the placeholder URL plus the Healthchecks.io copy. The cost is an error line every 15 s. Build the VPS before relying on the box.

### 3.2 Off-site backups

**Today the local stack has no backup at all.** `deploy/jobs/xtnl-pg-backup.sh` and `deploy/patroni/pgbackrest.conf` are for AWS only.

| | **A. Nightly `pg_dump` → age → rclone → B2 Object Lock** (P1 now) | **B. pgBackRest + WAL archiving → B2** (before P3) |
|---|---|---|
| RPO | 24 h, plus a manual dump before migrations | about 60 s (`archive_timeout=60`) |
| Code | **W3 (M):** a job service under a compose profile, a script and a scheduled task. The postgres container is unchanged | **W4 (M):** a custom postgres image, `archive_mode=on`, config from outside the repo |
| Keys on the box | **Encrypt only:** an age *public* key. The private key is in a password manager plus paper, so the box can't read its own backups | The repo passphrase and B2 key must be on the box, in a config file, never an environment variable |
| Object Lock | Clean. Compliance retention 30 days. The upload key has **no** `deleteFiles`. rclone's b2 backend uses the native API | Workable but **untested**. Try it on a throwaway bucket first |
| Retention | 30 daily + 12 monthly, each under its own prefix. **B2 lifecycle rules** enforce it: `daily/` hidden after 31 days and deleted 1 day later, `monthly/` after 366 days. The lock stops anything being deleted early | 14 days of WAL + weekly full backups |
| Cost | about 0–0.50 a month (first 10 GB free, then 6.95 per TB-month) | under 0.30 a month extra (est.) |

**Option A details:**
- **Run in a container**, as a profile service in `deploy/local/docker-compose.local.yml`, so it joins the stack's network (`xtnl-local_default`) and reaches `postgres:5432`. PowerShell 5.1 corrupts binary pipes, so the pipeline `pg_dump -Fc | age -r <pubkey> | rclone rcat` runs inside the container.
- **Also dump the roles:** `pg_dumpall --globals-only --no-role-passwords`.
- **Check every dump** with `pg_restore -l`, and store a `.sha256` file next to it.
- **Timing:** run it away from start-up, because the migration uses `lock_timeout 2s`.
- **Healthchecks.io:** ping check C on success, and C's `/fail` URL on failure.
- **Run it under a separate Windows account** (for example `xtnl-backup`, in `docker-users`), with its rclone config in that profile. **The B2 key never enters the `xtnl` account's environment.** That way neither the engine, the API nor the live view can ever see it. This does **not** fully separate the two accounts while the job runs on Docker Desktop: every `docker-users` member controls the engine's Docker, and Docker Desktop serves bind mounts with the rights of `xtnl`, who runs it. So `xtnl` can read the key from the job's container, and `xtnl-backup` can change or read the engine's checkout (code, TLS key, `local.env`) with `xtnl`'s rights, whatever the checkout's folder rights say. Both directions are an accepted deviation (`docs/runbooks/home-backup-restore.md` Set-up 3) until the native backup runtime, a follow-up before P3, drops `xtnl-backup` from `docker-users`.

**Partition archive (W10).**
- Without `XTNL_ARCHIVE_DIR`, partitions are never detached. Ones older than 90 days raise an **info** notice, "awaiting archive". With the database created on 2026-11-14, that starts around **2027-02-12**.
- Set the variable to a folder outside OneDrive.
- **The archive files are plain gzip JSONL of `event_log` and `account_snapshots`, which hold account data.** W3 must age-encrypt them before uploading, never copy them as they are.

**Do not back up:**
- the `xtnl_local_redis_md` volume (market data stays only in redis-md);
- Docker's `docker_data.vhdx`, which contains it;
- the engine Redis or `redis-api`, which are disposable.

**Decision for you:** `last_clean_marks` and `sod_marks` are kept in PostgreSQL by design (operations.md §11). If you read the market-data rule strictly, add `--exclude-table-data=last_clean_marks`. The cost is a `partial_session` FLAG on restore day.

**Avoid** restic with B2 Object Lock: its lock files and `prune` fight the retention locks.

**Restore (option A):**
1. **Make sure no other engine is running anywhere** (§4.13). On the target PC, with the scheduled task not installed, run `docker compose -f deploy/local/docker-compose.local.yml up -d --wait`.
2. `rclone copy` the dump, check the sha256, then `age -d -i <identity>`.
3. **Roles:** don't replay the globals file as it is. It contains the `xtnl` superuser, so it fails, and the API and live-view roles come back without passwords. Follow the role statements in `docs/runbooks/single-pc-autostart.md` ("Carry over": 0011, 0014's `xtnl_api`, 0021). Then:
   - `pg_restore --exit-on-error`;
   - `.venv\Scripts\python.exe -m src.db.migrate`;
   - compare row counts.
4. **etcd and the epoch fence.** This step fails if done naively:
   - A fresh etcd volume from the same compose file almost certainly gets the **same** cluster_id (it comes from `--initial-cluster-token=xtnl-local` plus the peer URL, compose `:141-144`), and its revision restarts near 1.
   - The epoch bump (`src/db/epoch.py:46-48`, `... AND revision < $2`) then changes 0 rows, so the engine is **fenced**.
   - `epoch adopt` refuses with `already_adopted` when the cluster_id hasn't changed (`src/ops/service.py:450-451`).
   - **Fix (W18):** rebuild etcd with a **new** `--initial-cluster-token` (for example `xtnl-local-r1`). Check that `python -m src.ops epoch show` reports an etcd cluster_id different from the stored one, then run `src.ops epoch adopt <new> --from-cluster-id <old> --operator <you> --approver <second person> --reason ...`.
   - Or copy the etcd volume together with PostgreSQL data from the same moment.
   - `docs/runbooks/single-pc-autostart.md:303-304` ("a new etcd volume means a new cluster_id") has the same likely error; W18 fixes it.
5. **The approver.** The code refuses an approver name equal to the operator name (`src/ops/model.py:53`), but it only compares the strings and authenticates nobody. The second-person check is a procedure you follow, not a control the code enforces. **Name that person now**, someone who can approve by phone.
6. **Start the task.** The `startup` catch-up re-reads broker history from `adapter_coverage.last_streaming_ms − 900 s`. Check that `python -m src.ops catchup show` reports complete.
7. **Credentials:** run `new-liveview-credentials.ps1 -Rotate` and `new-api-credentials.ps1 -DbRole`. A bare run of the API script doesn't reset the role.
8. **Lost for good:** audit rows after the last dump (`oversee_events`, `halt_events`, `operator_audit`, `arm_decisions`, `engine_runs`). That is acceptable while read-only. It is not acceptable once approvals exist, which is why option B comes before P3.

---

## 4. Setup steps, in order

`powercfg`, `w32tm`, gpedit, BitLocker, firewall and Tailscale steps need an **admin** shell. Everything Docker- and repo-related runs **as the engine user `xtnl`**. Elevate from inside the `xtnl` session with UAC.

### 4.1 BIOS (before Windows)
1. **Restore on AC power loss: Power On.**
2. **RTC wake:** daily, for example "Automatic Power On → Wake Up on Alarm" on ThinkCentre. This is the backstop if the box ever stays off.
3. **ErP/EuP deep sleep: off.** It blocks AC restore, RTC wake and Wake-on-LAN.
4. **Wake-on-LAN: on.**
5. **Virtualization (VT-x / SVM): on**, for WSL2.
6. **TPM 2.0 and Secure Boot: on.**
7. **Intel AMT:** enable and set its password (vPro units only).
8. **BIOS admin password.** Turn off boot from USB and network, and lock the boot order.
9. **Cabling:** wired Ethernet. Plug the PC, router, NBN box and VDSL modem (if any) into the UPS battery outlets.

### 4.2 Windows edition and accounts
1. **Windows 11 Pro.** You need it for BitLocker, the Remote Desktop host and update control through gpedit.
2. **An admin account**, used only to elevate. **Never connect over RDP as this account**: Windows would offer to disconnect `xtnl`, and one wrong click signs it out and stops everything.
3. **The engine account `xtnl`:** local, Standard, in the `docker-users` and **Remote Desktop Users** groups. No Windows Hello requirement, so automatic sign-in works.
4. **Automatic sign-in plus an immediate lock:**
   - Sysinternals Autologon for `xtnl` (stores the password as an LSA secret).
   - A log-on task that runs `rundll32.exe user32.dll,LockWorkStation` **at once** (at most about 5 s). Locking doesn't stop Docker Desktop, and the engine task already waits 60 s.
   - **Why:** the task is registered `AtLogOn`, Interactive, "run only when logged on" (`install-autostart.ps1:47-57`), because Docker Desktop runs in the user session. Nothing starts before sign-in, a **sign-out stops everything**, and a locked session keeps running.
5. **Remove sign-in interruptions:** turn off the "finish setting up your device" and welcome screens (Settings > System > Notifications > Additional settings). They can block the desktop at autologon, so Docker Desktop never starts.
6. **Time zone:** (UTC+10:00) Canberra, Melbourne, Sydney. "Set time automatically": on.
7. **OneDrive:** don't sign in, or uninstall it.
8. **Python 3.12 or later** for `xtnl` (the current caches are 3.14).

### 4.3 Power (admin)

On a UPS, Windows sees a battery and uses the **on-battery (DC)** settings during a power cut. Its default on battery is to sleep within minutes, and a sleep fences the engine on wake. So set both AC and DC:

```powershell
powercfg /change standby-timeout-ac 0
powercfg /change standby-timeout-dc 0
powercfg /change hibernate-timeout-ac 0
powercfg /change hibernate-timeout-dc 0
powercfg /change disk-timeout-ac 0
powercfg /change disk-timeout-dc 0
powercfg /change monitor-timeout-ac 10
powercfg /change monitor-timeout-dc 10
powercfg /hibernate off
powercfg /setacvalueindex SCHEME_CURRENT 2a737441-1930-4402-8d77-b2bebba308a3 48e6b7a6-50f5-4782-a5d4-53bb8f07e226 0
powercfg /setdcvalueindex SCHEME_CURRENT 2a737441-1930-4402-8d77-b2bebba308a3 48e6b7a6-50f5-4782-a5d4-53bb8f07e226 0
powercfg /setacvalueindex SCHEME_CURRENT 501a4d13-42af-4429-9fd1-a8218c268e20 ee12f906-d277-404b-b6da-e5fa1a576df5 0
powercfg /setdcvalueindex SCHEME_CURRENT 501a4d13-42af-4429-9fd1-a8218c268e20 ee12f906-d277-404b-b6da-e5fa1a576df5 0
powercfg /setactive SCHEME_CURRENT
Disable-NetAdapterPowerManagement -Name 'Ethernet'
```

- `powercfg /hibernate off` also turns off Fast Startup.
- **Battery actions:** set "Low battery action" and "Critical battery action" to **Do nothing**. PowerPanel does the shutdown (§2.2).
- **Network adapter** (Device Manager): untick "allow the computer to turn off this device", and turn off Energy-Efficient Ethernet.
- **Check:** `powercfg /a` shows no hibernate and no Fast Startup.

### 4.4 Windows Update (gpedit)
- **Maintenance window: Saturday 10:00 to Monday 06:00 Melbourne.** FX closes between 07:00 and 09:00 on Saturday Melbourne time and reopens between 07:00 and 09:00 on Monday, depending on daylight saving. Check the StoneX and Pepperstone calendars for any weekend markets.
- **Configure Automatic Updates:** option 4, Saturday 11:00. Untick "install during automatic maintenance".
- **"No auto-restart with logged on users": Enabled.** Some 25H2 builds have been reported to force restarts anyway, so keep watching.
- **Feature updates:** pin the target version to the one installed (check `winver`). 25H2 Home/Pro support ends **12 October 2027**, so plan the next feature upgrade for a maintenance window by about July 2027.
- **Quality updates:** defer 0–7 days.
- **"Do not include drivers with Windows Updates": Enabled.**
- **"Receive updates for other Microsoft products": off.** It updates WSL mid-week and restarts every container.
- **Microsoft Store:** automatic app updates off.

### 4.5 Time (admin)

The engine's clock check (`src/runtime/probes.py`, `W32tmReader`) takes **one** `w32tm` sample against **time.windows.com**, which is hard-coded, and requires 50 ms or less. Make Windows sync that same source every 15 minutes:

```powershell
w32tm /config /manualpeerlist:"time.windows.com,0x9" /syncfromflags:manual /update
reg add HKLM\SYSTEM\CurrentControlSet\Services\W32Time\TimeProviders\NtpClient /v SpecialPollInterval /t REG_DWORD /d 900 /f
sc.exe config w32time start= auto
Restart-Service w32time; w32tm /resync; w32tm /query /status
```

### 4.6 WSL and Docker Desktop
1. **Admin:** `wsl --install`, then reboot.
2. **As `xtnl`:** install Docker Desktop and launch it for the first time as `xtnl`. The volumes `xtnl_local_pg` and `xtnl_local_etcd` belong to whichever user creates them.
   - **Finish onboarding:** accept the subscription agreement and close the welcome screens. Until then the engine never starts: `Wait-Docker` times out with exit 10 and retries for ever.
3. **Settings:**
   - Start Docker Desktop when you sign in: **on**;
   - Open Dashboard at start: off;
   - Resource Saver: **off**;
   - "Automatically check for updates", "Always download updates" **and** "Automatically update components": **all off**.
4. **Cap WSL memory** in `%USERPROFILE%\.wslconfig`:
   ```
   [wsl2]
   memory=8GB
   ```
   Use about 6 GB on a 16 GB box. Containers need about 3–4 GB at worst. The cap protects the engine, which runs on Windows outside WSL.
5. **Updates by hand only:** `wsl --update` and Docker updates at weekends. Afterwards, open Docker Desktop over RDP and click through any new screens before you disconnect.

### 4.7 Repo outside OneDrive

OneDrive syncs the API TLS key under `var\api-tls`, breaks the ARM lockout scan with placeholder files, and holds the logs and the spill file.

1. **On the old desktop:** right-click the repo folder > **"Always keep on this device"**, so placeholder files aren't skipped or half-copied.
2. **Copy it with its uncommitted changes:**
   `robocopy <old path>\order-server C:\xtnl\order-server /E /XD .venv`.
   Compare `git status` on both sides.
3. **Build the virtualenv as `xtnl`:** `cd C:\xtnl\order-server`, `python -m venv .venv`, `.venv\Scripts\pip install -e .[dev]`.
4. **Data from the old desktop** (optional). Either:
   - **Carry over:** `pg_dump` / `pg_restore` per "Carry over" in `docs/runbooks/single-pc-autostart.md`, **and** copy the `xtnl_local_etcd` volume from the same moment (same cluster_id and revision, so no fence); or
   - **Start fresh:** the first start catches up `XTNL_CATCHUP_FIRST_RUN_DAYS` (7) days of broker history.
5. **API TLS:** copy `var\api-tls` across, or update `ORDER_SERVER_TLS_FINGERPRINT` in Vercel.

### 4.8 Env file and credentials (as `xtnl`)
1. **Env file:** copy `deploy/env/local-live.env.example` to `deploy/env/local.env`, then set:
   - `XTNL_SINGLE_PC=1` (as in the example);
   - `XTNL_NODE_ID=home1` (the example says `pc1`), so `engine_runs`, `leader_terms` and the beats show which machine is which;
   - `XTNL_MIGRATIONS_ACCEPTED_UPTO` = the old desktop's value, 21 or more if you use the live view. The example keeps 14, and the live view needs 0021;
   - `XTNL_DEADMAN_URL` (§3.1);
   - `XTNL_ARCHIVE_DIR=C:\xtnl\archive` (W10);
   - **`XTNL_API_BIND=127.0.0.1`.** The example has `0.0.0.0`. Don't create the API runbook's 47443 firewall rule until W14 is decided.
2. **Windows User environment** (never the env file; `start-xtnl.ps1` refuses with exit 78 if the file sets them):
   - `STONE_X_USERNAME`, `STONE_X_PASS`, `STONE_X_APP_KEY`, `STONE_X_BASE_URL`;
   - `BROKER_ENV=live_readonly`;
   - `XTNL_DEADMAN_TOKEN` (required in practice; see §3.1);
   - the alert channels `XTNL_PUSH_TOKEN` / `XTNL_PUSH_USER` and the SMTP credentials, so the critical single-PC pages are sent, not only logged;
   - after W2: `XTNL_HEARTBEAT_COPY_URL`.
   - **Never** the B2 key (it belongs to the backup account, §3.2).
3. **Credential scripts:** run `deploy\local\new-liveview-credentials.ps1`, plus `new-api-credentials.ps1` and `new-md-credentials.ps1` if you use them.
   - Use a normal PowerShell, **not a packaged (MSIX) terminal**. The redis-md ACL lives in `%LOCALAPPDATA%\XTNL\`, which packaged apps redirect.

### 4.9 Scheduled task (as `xtnl`)

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\install-autostart.ps1 -StartNow
```

- **Admin:** Task Scheduler > **Enable All Tasks History**. It is off by default, and the runbook relies on it.

### 4.10 Tailscale and remote access (admin)
1. **Install Tailscale** and turn on **Run unattended**, so it connects before sign-in.
   - In the admin console, set the ACL tag `tag:xtnl-home` on this node (tags turn key expiry off). Untagged, it counts as one of your own devices and gets port 22 and Tailscale SSH to the receiver VPS (`docs/runbooks/deadman-receiver-home.md` step 2). This applies to **every** box that runs the engine, a replacement or rebuilt one too: tag it, remove the old box from the tailnet, then repeat that runbook's step 3 refusal checks from the new box.
   - Use ACLs to limit it to your own devices and the VPS.
2. **Router:** a DHCP reservation for the box. **No port forwards. UPnP off.**
3. **Remote Desktop:**
   - Enable it and turn off its built-in firewall rules.
   - Allow it **only on the Tailscale interface**:
     ```powershell
     New-NetFirewallRule -DisplayName 'RDP via Tailscale' -Direction Inbound -Protocol TCP -LocalPort 3389 -InterfaceAlias 'Tailscale' -RemoteAddress 100.64.0.0/10,fd7a:115c:a1e0::/48 -Action Allow
     ```
     The 100.64.0.0/10 range alone isn't enough, because 4G carriers use it too.
   - Keep NLA on.
   - Connect as `xtnl` and leave with **Disconnect, never Sign out**.
4. **Live view:** open `http://127.0.0.1:8090` inside the RDP session. It refuses non-loopback binds by design. Don't put it behind `tailscale serve` until the D5 tunnel design exists.

### 4.11 BitLocker (admin)
- **Protector:** BitLocker on C: with **TPM only**. A PIN would block unattended reboots.
- **What it protects:** only a removed SSD. Anyone who powers the box on reaches the locked `xtnl` session.
- **Recovery key:** keep it off the box, on paper and in a password manager. Give a copy to the person who can power-cycle the box.
- **Before firmware updates:** before a BIOS update, or a patch Saturday that carries firmware or Secure Boot changes (the 2011 Secure Boot certificates are being replaced during 2026), run `manage-bde -protectors -disable C: -RebootCount 2`.
- **Defender exclusions:**
  - `%LOCALAPPDATA%\Docker\wsl` (for `xtnl`);
  - `C:\xtnl\order-server\var`;
  - **not** `.venv`, `src` or `python.exe`.

### 4.12 Retire the old desktop (before the first start on the new box)

Each machine has its own etcd and PostgreSQL, so nothing stops two engines running at once. Here is what would happen:
- **Both alert:** both send alerts on the same channels.
- **Death is hidden:** the receiver keeps only the newest beat (`evaluate.py:272-274`), so one live engine hides the other's death.
- **Two senders:** in P3 and later there would be two senders.
- **Broker sessions:** a second StoneX session on the same login may also disturb the first (unconfirmed).

On the old desktop:
1. Run `deploy\local\uninstall-autostart.ps1` and `deploy\local\stop-xtnl.ps1 -Containers`.
2. Delete the broker, alert and dead-man variables from its User environment, or rotate the StoneX read-only credentials so the old copy can't log on.
3. Keep its Docker volumes only as a rollback.

**Never** set broker credentials or install the task on the cold spare. Without `STONE_X_*`, even a mistaken `start-xtnl.ps1` exits 78.

### 4.13 First start and checks
1. **Power cut test:** pull the plug with everything running. The box should power on, sign in, lock and start the task with no help.
2. **Status:** run `deploy\local\status-xtnl.ps1`. Expect:
   - task last result 0 or running;
   - containers healthy;
   - `[clock]` passed;
   - `uptime show` and `catchup show` with `complete` true;
   - no new `ALERT:` or `PAGE[critical]` lines;
   - the live view up.
3. **Beats:** within 1 minute the receiver and Healthchecks.io check A show beats, and the log has no 401 lines.
4. **Paging test:** stop the engine. The receiver pages within about 60–75 s, and Healthchecks.io within about 3–4 minutes.
5. **Backups:** run the first backup by hand, then restore it onto the spare before you trust it (§5.3).
6. **UPS test:** pull the UPS plug and check that:
   - the box does **not** sleep in the first 15 minutes;
   - at the runtime threshold PowerPanel runs `stop-xtnl.ps1`, then shuts Windows down;
   - the UPS cuts its output;
   - the box powers back on when mains returns.
   Note the runtime.
7. **4G test:** unplug the NBN link. The box should still be reachable over Tailscale, and the beats should continue.

---

## 5. Running it

### 5.1 Routine checks

| When | Check |
|---|---|
| Daily (glance) | The 22:00 UTC test page arrived. Healthchecks.io A, B and C are green |
| Weekly | `status-xtnl.ps1`. `Get-Volume C`: act at 70 % and 85 % (operations.md §11). `docker system df` |
| Monthly | SMART (admin): `Get-PhysicalDisk \| Get-StorageReliabilityCounter \| ft Temperature,Wear,ReadErrorsUncorrected,PowerOnHours` |
| Monthly | Event Viewer > System: Kernel-Power 41/42, WHEA and disk errors |
| Monthly | Container clock against the host: `docker exec xtnl-local-postgres-1 date -u` |
| Monthly | Last night's B2 object exists and has its lock date set |
| Monthly | Force 4G failover (unplug the WAN) and check the box is reachable. Check the SIM plan is still active |
| Quarterly | Restore drill (§5.3). UPS self-test |
| Yearly | Rotate the dead-man token (§3.1) and the live-view and API credentials. Plan the next Windows feature upgrade |
| Every 3–5 years | UPS battery (or the whole UPS if the battery isn't replaceable) |

### 5.2 Windows Update routine (Saturday, in the window)
1. Let updates download and install. Auto-restart is blocked.
2. If the updates include firmware or Secure Boot changes: `manage-bde -protectors -disable C: -RebootCount 2`.
3. `deploy\local\stop-xtnl.ps1`: a clean drain (`end_reason=SIGINT`).
4. Restart Windows. Autologon and the task bring everything back.
5. `status-xtnl.ps1`: clock check passed, containers healthy, `startup` catch-up complete.
6. Do WSL and Docker Desktop updates in the same window, by hand. Click through Docker's screens over RDP.
7. Monthly in the same window, with the engine stopped: the local etcd defrag (W15).

### 5.3 Restore drill (quarterly, on the spare)
- **No live credentials on the spare:** no `STONE_X_*`, no `XTNL_DEADMAN_TOKEN` and no scheduled task.
- **Steps:** follow §3.2 "Restore" against the spare, including the new etcd token and `epoch adopt` (W18).
- **Don't start the engine against live brokers** unless the main box is off.
- **Record:** time to restore, row counts against production, and whether `epoch adopt` and the approver call worked.
- **A failed drill means the backups don't count** until a drill passes.

### 5.4 Incidents

| Event | What happens | What you do |
|---|---|---|
| **Power cut** | The UPS carries it, probably 60 min or more (est.). At the runtime threshold PowerPanel runs `stop-xtnl.ps1`, then shuts down and cuts output. The receiver pages on silence | Nothing while it lasts. When mains returns, the BIOS powers on, autologon runs and the task starts. Check `status-xtnl.ps1` and that `catchup show` is complete. If the box stays off, RTC wake brings it back within a day; Wake-on-LAN or the smart plug is faster |
| **Internet outage** | 4G takes over in about 10 s. If both links fail, broker streams drop, beats stop and the receiver pages | Wait. On reconnect the engine resyncs and catches up. A restart while on 4G may wait for the clock check (until W11) |
| **Unplanned restart or crash** | `run-local.ps1` restarts a hard exit (codes 70–79). The open term closes as `lost`, and the startup catch-up runs | `status-xtnl.ps1`, and look at the last `ALERT:` / `PAGE[critical]` lines |
| **Hung Windows or BitLocker recovery screen** | Beats stop and the receiver pages | vPro: AMT remote screen over the Tailscale subnet router. Otherwise power-cycle with the smart plug. For BitLocker recovery, someone at the box types the recovery key |
| **Disk failure** | The box is down and the receiver pages | Swap in the spare or a new SSD. Rebuild per §4, restore per §3.2 (new etcd token, `epoch adopt`, approver call). Data loss: up to 24 h of audit rows with option A, about 60 s with option B. Broker history comes back through catch-up |
| **Theft or fire** | The box is gone. The backups are off-site, encrypted and locked, and the box never held the decryption key | **Rotate at once:** StoneX read-only credentials, `XTNL_DEADMAN_TOKEN` and the receiver token, the B2 key, the API secret, the Healthchecks.io URLs (check A's `XTNL_HEARTBEAT_COPY_URL` and check C's), the engine's alert channel credentials (`XTNL_PUSH_TOKEN` / `XTNL_PUSH_USER`, `XTNL_SMTP_USERNAME` / `XTNL_SMTP_PASSWORD` or `XTNL_SES_*`, and `XTNL_ARM_PUSH_*` / `XTNL_ARM_SMTP_*` / `XTNL_ARM_SES_*` when the ARM is on). The VPS receiver uses its own Pushover token and relay login, so the theft cannot silence it. Remove the node from Tailscale. Rebuild on the spare and tag it `tag:xtnl-home` (§4.10) |
| **Receiver or VPS down** | The engine is unaffected; check B goes red. After W1 the engine pages itself | Fix the VPS. Until then you are blind to the box dying |
| **Long holiday** | – | Before leaving: run a drill, check 4G failover, paging and the spare, and name someone who can power-cycle the box and holds the BitLocker key |

**What the engine being down means.**
- **P1:** nothing is sent to brokers. Stops are held at the broker, so positions are protected as they were. You lose monitoring and alerts until it is back.
- **P3 and later:** a down engine also means missed stop moves. That is why those phases need more (§7).

---

## 6. Work items in the repo

Size: S less than 1 day, M 1–3 days, L more than 3 days. "Config" means no code.

| # | Item | Files | Size | Gate |
|---|---|---|---|---|
| W1 | **Receiver-down alert from the engine.** Gauges `xtnl_deadman_consecutive_failures` and `xtnl_deadman_last_ok_age_seconds`, the live-view allow-list line, and a high page through the outbox after 5 min without acknowledgement. Today failures are only counted in memory and logged (`deadman.py:332-336`). This is also M10's input | `src/runtime/deadman.py`, `src/runtime/session.py`, `src/observability/exporter.py`, `src/liveview/` allow-list, tests | S–M | P1 strongly recommended; **required before P3** · **built 2026-11-23** |
| W2 | **Start-script secret handling.** Add `XTNL_HEARTBEAT_COPY_URL` to `$UserVarNames` (the engine reads it), and to `$ForbiddenInFile`, `$ApiDropEnv`, `$LiveViewDropEnv` and both Python refuse lists. The B2 key is never added: it stays in the backup account | `deploy/local/start-xtnl.ps1`, `src/api/settings.py`, `src/liveview/settings.py`, `tests/deploy/test_single_pc_autostart.py`, `tests/deploy/test_order_server_api_assets.py` | S | **P1** · **built 2026-11-23** |
| W3 | **Local backup job (option A).** A compose profile service (`postgres:17` + age + rclone); NEW `deploy/local/backup-pg.ps1`; a scheduled task under the `xtnl-backup` account; globals dump; sha256; age-encrypted copy of `XTNL_ARCHIVE_DIR`; B2 lifecycle rules; Healthchecks.io ping; NEW restore runbook | `deploy/local/docker-compose.local.yml`, NEW `deploy/local/backup/Dockerfile`, NEW `deploy/local/backup-pg.ps1`, NEW `docs/runbooks/home-backup-restore.md`, deploy tests | M | **P1** · **built 2026-11-23** |
| W4 | **WAL archiving (option B)** with pgBackRest to B2. Custom postgres image and `archive_mode`; config outside the repo. Test Object Lock on a throwaway bucket | `deploy/local/docker-compose.local.yml`, NEW image, config, runbook | M | **Before P3** |
| W5 | **Receiver self-watchdog** (optional). It pings a NEW `XTNL_RECEIVER_WATCHDOG_URL` from `tick()`, and the fail URL while paging fails. Until then, the 1-minute curl timer | `src/deadman_receiver/service.py`, `settings.py`, tests | S | P1 nice to have |
| W6 | **Receiver on a VPS over Tailscale.** Plain local unit copy, env file, healthz timer | NEW `deploy/systemd/xtnl-deadman-receiver.home.service`, `deploy/env/deadman-receiver.env.example` note, `docs/runbooks/deadman-alert.md` | S (config) | **P1** · **built 2026-11-23** |
| W7 | **M10 `dead_man_lost` drop to advice.** After 2 × 60 s without a receiver acknowledgement, P4 drops to advice. Needs acknowledgement in the receiver's response | `src/runtime/deadman.py`, `src/deadman_receiver/`, ARM and ActionGate wiring, `docs/design/arm-sl-sample/live-adjust.md` | M–L | **Before P4** |
| W8 | **STOP_TIGHTEN gate kind.** Today `src/oversee/gate.py:92-93` allows a non-risk-increasing action straight after the boot-id, epoch, lease, halt-state and adapter checks. STOP_TIGHTEN must also require: the settle window, reconcile after the latest open, no open `stream_health` fence, no `catchup_unverified`, no stale mark or UNKNOWN, a fresh RiskSweep pass, and a fresh position version and price | `src/oversee/gate.py`, tests, `.claude/rules/oversee-rules.md` | M | **Before P3** |
| W9 | **Approval page (D5) and the phone tunnel.** The page on 127.0.0.1 with its own login, audit rows and the 60 s window. **On a box with no screen, the tunnel is a P3 gate too** (strong auth, TLS end to end, CSRF, approvals bound to one intent, session timeout), unless you keep an RDP session open through every trading window | NEW module; design `docs/design/arm-sl-sample/live-adjust.md`; prototype `docs/design/live-adjust-prototypes/approve.html` | L | **Before P3** |
| W10 | **Partition archive.** Set `XTNL_ARCHIVE_DIR` to a folder outside OneDrive; W3 encrypts and uploads it | `deploy/env/local-live.env.example`, docs | S (config) | P1, **before 2027-02-12** · **built 2026-11-23** |
| W11 | **Clock probe.** Configurable NTP server; median of 3 samples | `src/runtime/probes.py`, tests | S | P1, **before relying on 4G** |
| W12 | **UPS pre-shutdown.** PowerPanel's command option runs `stop-xtnl.ps1` (config). Fallback if not available: a watcher in the `xtnl` session (`send-ctrl-c.ps1` only works from the same session) that polls `Win32_Battery`, runs `stop-xtnl.ps1 -Containers`, then `shutdown /s /t 0`, so a stopped engine never sits on a running box | `docs/runbooks/single-pc-autostart.md`, or NEW `deploy/local/ups-watch.ps1` | S | **P1** · **built 2026-11-23** |
| W13 | **Health extras in `status-xtnl.ps1`:** NVMe wear, C: usage, `docker system df`, age of the last backup | `deploy/local/status-xtnl.ps1`, deploy tests | S | P1 |
| W14 | **API exposure decision.** `docs/runbooks/order-server-api.md` says to port-forward 47443, which conflicts with "no port forwards". Choose Cloudflare Tunnel or Tailscale Funnel, and check `ORDER_SERVER_TLS_FINGERPRINT` pinning | `docs/runbooks/order-server-api.md`, `deploy/env/local-live.env.example` | M | Before enabling the API |
| W15 | **Local etcd defrag**, only with the engine stopped (a single-member defrag blocks reads and writes; 12 s fences the engine), monthly in the maintenance window | NEW step or task, runbook | S | P1, low |
| W16 | **Docs:** a home-server runbook from this plan; the `run-local.ps1` header (still says "Stage 0, development only"); the second-person approver contact | NEW `docs/runbooks/home-server.md`, `deploy/local/run-local.ps1` | S | P1 |
| W17 | **Ping-only heartbeat copy**, so Healthchecks.io gets no payload (gate, halt level, adapter states) | `src/runtime/deadman.py` | S | P1 recommended |
| W18 | **etcd rebuild fix.** Make `--initial-cluster-token` settable in the compose file (`xtnl-local` today); write the restore steps; correct `single-pc-autostart.md:303-304`; prove it in the first drill | `deploy/local/docker-compose.local.yml`, `docs/runbooks/single-pc-autostart.md`, deploy tests | S | **P1, before relying on backups** · **built 2026-11-23** |
| W19 | **Two-engine alarm.** The receiver pages when the beating `node` changes, or when two nodes are seen within 5 minutes | `src/deadman_receiver/evaluate.py`, tests | S | **P1** · **built 2026-11-23** |
| W20 | **Hung Docker recovery.** If `docker info` keeps failing for 15 minutes, `start-xtnl.ps1` runs `wsl --shutdown`, restarts Docker Desktop and logs `ALERT:` | `deploy/local/start-xtnl.ps1`, deploy tests | S | P1 recommended |

**Gates:**
- **Before relying on the box (P1):** W1, W2, W3, W6, W10, W12, W18, W19, plus the old-desktop retirement (§4.12). W17 and W20 recommended.
- **Before P3:** W4, W8, W9 (including the tunnel or RDP decision), and W11 if 4G is in use.
- **Before P4:** W7, plus §7.

**Status (2026-11-23):** W1, W2, W3, W6, W10, W12, W18 and W19 are built in the repo: code, tests and runbooks (`docs/runbooks/deadman-alert.md`, `deadman-receiver-home.md`, `home-backup-restore.md`, `single-pc-autostart.md`). They went through repeated independent review and fix rounds, and a scenario-matrix test for the receiver's conditions (`tests/deadman_receiver/test_condition_matrix.py`). **None of it has run on the home box yet.** The steps marked "unverified" in those runbooks stay open until they do: the Tailscale policy, the B2 bucket and rclone flags, the scheduled tasks and ACLs, and the UPS shutdown. The P1 gate is met once those are checked and the old desktop is retired (§4.12).

Every change in `src/oversee/`, `src/integrity/`, `src/pubsub/` (and the other folders CLAUDE.md lists) starts with `/live-trading-server`. Then run `verify_execution.py --strict`, pytest and mypy.

---

## 7. Limits

**What one home box can't give you**, compared with the designed cloud backbone (two London nodes in different zones, a witness, a Patroni sync standby):
- **No failover.** One PC, one power feed, one house. Any fault means downtime until you fix it.
- **RPO is not 0:** 24 h with option A, about 60 s with option B.
- **Shared risks:** power, internet (4G helps for hours), theft, fire, heat, and you being away.
- **Hands-on recovery:** a disk loss needs you, the spare and the approver.
- **Windows:** updates, Docker Desktop and autologon make it less predictable than systemd on Linux. `XTNL_SINGLE_PC=1` is refused under systemd (`src/runtime/single_pc.py:134`), so a Linux box would need code work.

**Fit by phase:**
- **P1 and P3:** acceptable. Missed time costs alerts (P1), or a late or missed approval (P3). The broker holds the current stops either way.
- **P4:** acceptable only with W7, so a lost receiver acknowledgement drops automation to advice.
- **P5:** not acceptable on one box.

**When to add a second machine or the cloud.** Before P4 goes live with real stop moves. Earlier if, over any 3 months, you see any of these:
- more than **1 hour a month of unplanned downtime** while markets are open;
- **2 or more incidents a month** needing hands-on recovery;
- a restore drill that **fails or takes longer than 4 hours**.

The standby can be:
- **the spare, turned warm** with a shared etcd member. That needs design and code for single-PC fencing;
- **one London cloud node**, following the existing AWS design.

**P5 needs the full fenced-leader setup back:** two nodes, a witness and a sync standby.

---

## 8. Costs (USD)

**One-off**

| Item | Recommended | Budget | Note |
|---|---|---|---|
| Mini PC | 330–480 | 190–260 | est.; refurbished vPro Tiny with 32 GB, or N150 with 16 GB |
| 1 TB TLC NVMe with DRAM (≥ 600 TBW), if not included | 0–240 | 0–150 | est., November 2026 (prices roughly tripled in a year) |
| Windows 11 Pro upgrade, if no Pro licence | 0–100 | 0–100 | est.; refurbished business units often include Pro |
| UPS (PowerPanel-supported, about 1000 VA) | 110–170 | 110–170 | est. |
| 4G failover router (MR600 or RUT241) | 155–200 | 155–175 | est.; RUT241 about 199 confirmed |
| Pushover (phone + desktop) | 5–10 | 5–10 | 4.99 per platform, confirmed |
| Smart plug with local control (if no vPro) | 0–25 | 15–25 | est. |
| **Subtotal** | **about 600–1,225** | **about 475–890** | |
| Cold spare PC (it can keep its stock SSD) | +330–480 | +190–260 | est. |

**Monthly**

| Item | Cost | Note |
|---|---|---|
| Electricity, PC | 1.20–2.20 | at USD 0.20/kWh (est.), 8–15 W |
| Electricity, UPS's own draw | 0.70–1.20 | about 5–8 W (est.) |
| VPS for the receiver | 5–6 | est.; Vultr or DigitalOcean, Sydney or Melbourne |
| 4G backup SIM | 5–10 | est.; recurring plan; some NBN plans include one |
| Backblaze B2 | 0–0.50 | first 10 GB free; option B adds under 0.30 (est.) |
| Healthchecks.io, Tailscale | 0 | free plans |
| **Total** | **about 12–20** | est. |

For reference only (not added): the router and NBN box run anyway; the whole stack draws about 35 W.

---

## Sources

**Hardware**
- [STH: ThinkCentre M75q Gen 2 power](https://www.servethehome.com/lenovo-thinkcentre-m75q-tiny-gen2-35w-amd-ryzen-5000-powerhouse/5/)
- [Best Buy: M75q G2 refurbished](https://www.bestbuy.com/product/lenovo-thinkcentre-m75q-g2-micro-amd-ryzen-5-pro-5650ge-16gb256gb-w11p-black/JXRKGP5HGS)
- [Notebookcheck: NUC 14 Essential](https://www.notebookcheck.net/Asus-NUC-14-Essential-review-An-efficient-mini-PC-as-a-desktop-replacement-that-s-useful-for-everyday-use-and-at-the-office.943361.0.html)
- [Beelink forum: power-on after power loss](https://bbs.bee-link.com/d/7731-power-on-after-power-loss-not-working)
- [Notebookcheck: SSD prices up 303 %](https://www.notebookcheck.net/SSD-prices-explode-by-303-in-a-year-as-AI-fuels-NAND-flash-shortage.1433261.0.html)
- [TechPowerUp: DDR4 prices](https://www.techpowerup.com/345717/ddr4-prices-skyrocketing-amid-dram-shortage-crunch)

**UPS**
- [CyberPower AU: VP1000ELCD](https://www.cyberpower.com/au/en/product/sku/vp1000elcd)
- [APC: UPS as a Windows battery](https://www.se.com/au/en/faqs/FA159653/)

**Internet**
- [nbn: power blackout](https://prod-aemcs.nbnco.com.au/learn/what-happens-in-a-power-blackout)
- [nbn: FTTP battery](https://www.nbnco.com.au/learn/network-technology/fibre-to-the-premises-explained-fttp/battery-back-up-information)
- [TP-Link AU: MR600](https://www.tp-link.com/au/home-networking/5g-4g-router/archer-mr600/v2/)
- [Linovision: RUT241](https://linovision.com/products/teltonika-rut241-industrial-4g-lte-cellular-wi-fi-router)
- [Telstra: mobile backup](https://www.telstra.com.au/support/internet-and-home-phone/mobile-backup)

**Off-site services**
- [Backblaze B2 pricing](https://www.backblaze.com/cloud-storage/pricing)
- [B2 Object Lock](https://www.backblaze.com/docs/cloud-storage-object-lock)
- [Healthchecks.io pricing](https://healthchecks.io/pricing/)
- [Pushover pricing](https://pushover.net/pricing)
- [Tailscale: run unattended](https://tailscale.com/kb/1088/run-unattended)
- [Restic and B2 Object Lock](https://forum.restic.net/t/anyone-using-restic-with-b2-backblaze-and-object-locks/4277)

**Windows and Docker**
- [Microsoft lifecycle: Windows 11 Home and Pro](https://learn.microsoft.com/en-us/lifecycle/products/windows-11-home-and-pro)
- [Docker: silent component updates](https://www.docker.com/blog/docker-desktop-silent-component-updates/)
