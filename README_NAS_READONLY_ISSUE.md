# NAS data files becoming read-only

## Symptom

Files saved to the Netgear NAS (via a mapped network drive) eventually become
read-only — but only files that are no longer being actively written to
(previous days' rotated-out files). The actively-growing file for the current
day continues to be writable. The behavior shows up after the software has
been running for more than a day, or once a new rotation file is created.

## Why this is very unlikely to be caused by ExxonController itself

- Nothing in `core/data_logger.py` or `core/nas_relay.py` ever calls
  `os.chmod()` / sets file attributes. Files are opened in append mode and
  closed normally on rotation.
- A leaked/un-released Python file handle does **not** cause Windows/SMB to
  flip the read-only DOS attribute bit. A lingering handle causes *sharing
  violations* (`PermissionError [WinError 32]`) if another process tries to
  delete/rename/exclusively-open the file — a different symptom from a file
  showing as read-only in Explorer/`attrib`.
- Once a file rotates out (new day/period), the app never reopens or touches
  it again, so there's no code path left running against those files at all.

## Leading hypotheses (NAS/SMB-side, ranked by likelihood)

1. **A scheduled job on the NAS itself.** Netgear ReadyNAS boxes commonly run
   nightly background tasks — built-in antivirus scans, snapshot/versioning,
   or backup/replication apps (ReadyNAS Replicate, rsync jobs). Several of
   these intentionally mark files read-only after they've been
   scanned/backed up, as a safeguard against modifying something already
   archived. This fits the observed timing well: today's actively-growing
   file is excluded (it's open/locked by the app), while yesterday's
   now-closed files get swept by the nightly job and flipped read-only.

2. **Windows "Offline Files" client-side caching.** If the mapped network
   drive's connection to the NAS drops even briefly (NAS reboot for
   maintenance, drive spin-down/sleep, DHCP lease renewal, network blip) and
   Offline Files is enabled for that share, Windows can serve a locally
   cached copy that's read-only until the share reconnects and reconciles.

3. **SMB session/credential fallback.** If the mapped drive uses cached
   credentials that expire, or the NAS drops the authenticated session,
   Windows can silently reconnect with reduced (sometimes read-only or
   guest-level) access. Newly created files in a fresh, properly
   authenticated session would still write fine, while files touched under
   the old/expired session might report as read-only.

## Diagnostics to run

- **Confirm whether the attribute bit is actually set**, vs. just a write
  failing. In Windows Explorer, check the `Read-only` checkbox in the file's
  Properties dialog, or from a command prompt: `attrib <file>`. This
  distinguishes two different problems:
  - **Attribute bit is actually set** → points to hypothesis #1 (a NAS-side
    job flipping it). Check the ReadyNAS admin UI for scheduled
    antivirus/backup/snapshot apps and their run times; correlate with when
    files flip.
  - **Attribute looks normal but writes still fail** (permission denied /
    sharing violation, not a read-only flag) → points to #2 or #3. Check the
    mapped drive's reconnect behavior and whether Offline Files is enabled.
- **Check the NAS admin UI directly** for: antivirus scan schedule, snapshot
  schedule, backup/replication jobs, and any "read-only" toggle on the
  share/folder that might be applied on a schedule.
- **Check Windows Event Viewer** (System log, and any SMB client event
  sources) around the time a file is known to have flipped, for share
  disconnect/reconnect events.
- **Check if Offline Files is enabled** for the mapped drive: `Control Panel
  → Sync Center → Manage offline files`, or `net use` to see the current
  mapping/connection state.
- **Conclusive isolation test**: temporarily point `Data/` at a local,
  non-NAS path and watch whether old files still go read-only. If they don't,
  it's conclusively the NAS/network path, not the app.

## Possible code-side mitigations (not yet implemented — for discussion)

These don't fix the underlying NAS-side cause, but could make the app more
resilient to it:

1. **Proactively clear the read-only attribute before opening a file for
   append.** E.g. `os.chmod(path, stat.S_IWRITE)` (or `attrib -R`) right
   before `open(path, 'a')`, in `RawDataLogger._rotate()`,
   `PeripheralDataLogger._rotate()`, and `NasRelay._get_writer()`. Cheap and
   harmless if the file is already writable; self-heals if it isn't,
   regardless of which external process set it. Only helps for files the app
   itself still needs to write to (current-period file, or a continued
   experiment file) — it would not "fix" the NAS job, just stop it from
   blocking the app.
2. **Surface write failures instead of swallowing them.** Right now, if
   `open()`/`write()`/`flush()` raises `OSError` (which is exactly what a
   read-only file produces), both loggers catch it and silently return —
   there's no log line, toast, or UI indicator that a write failed. Adding an
   `_emit_log(..., 'error')` (rate-limited, so it doesn't spam) when this
   happens would at least make data-loss visible in real time instead of
   being discovered later when reviewing files.
3. **A small standalone diagnostic script** (run manually, not part of the
   polling loop) that walks `Data/Raw/` and `Data/Experiments/`, reports any
   file with the read-only attribute set, and logs the result with a
   timestamp — run it a few times a day for a few days to nail down exactly
   when the flip happens and correlate it with the NAS's own job schedule.

## Status

Discussion only — no code changes have been made for this issue yet.
