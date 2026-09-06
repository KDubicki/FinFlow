# Deploying the daily run

Four unit files and one environment file. Everything else the pipeline needs is
already on local disk, so nothing here pushes, pulls or round-trips anything
(`PROJECT.md` §11.1).

## Once, on the box

```bash
sudo useradd --system --home /opt/finflow finflow
sudo install -d -o finflow -g finflow /opt/finflow /opt/finflow/data
sudo install -d -o root -g finflow -m 750 /etc/finflow

# Secrets: root-owned, mode 600, never in the repository
# (daily-operations standard 6).
sudo install -o root -g root -m 600 /dev/null /etc/finflow/finflow.env
sudoedit /etc/finflow/finflow.env      # see .env.example for every key

sudo cp deploy/finflow-*.service deploy/finflow-*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now finflow-daily.timer finflow-backup.timer
```

The backup timer needs `/mnt/backup` to be a **different physical device** — an
external disk or a NAS mount. A copy on the same disk is not a backup, and on
one machine that is the failure that actually happens (`PROJECT.md` §11.3).

## Checking it

```bash
systemctl list-timers 'finflow-*'      # when each next fires, and when it last did
journalctl -u finflow-daily -n 200     # the last run, structured JSON
sudo -u finflow /opt/finflow/.venv/bin/finflow-daily --dry-run   # rehearse: no messages sent
```

A dry run prints the digest instead of sending it and does **not** drain the
command inbox — draining acknowledges the updates, and a rehearsal that
swallowed a `/position` would lose it silently.

## Why the schedule looks like that

| Choice | Reason |
|---|---|
| `OnCalendar=... UTC` | The pipeline is scheduled in UTC (`PROJECT.md` §11.6). A local-time schedule shifts twice a year against the data it reads. |
| Two calendar entries | 05:30 is well after the US close; 10:30 is the retry before declaring a bad day. An unchanged decision produces no second message, so the retry is free. |
| `Persistent=true` | A run missed while the machine was off fires on the next boot. On one box, that is what a power cut looks like. |
| `Type=oneshot` | M4 is a timer, not a daemon. There is no long-lived process to crash-loop unnoticed, which is a named risk for the ones that arrive in M6. |
| Backup at 02:00 | Before the daily run, so the copy is of a quiet database and the two jobs never contend for the disk. |

## The dead-man's switch

`FINFLOW_HEALTHCHECKS_URL` points at a healthchecks.io check. Set its period to
24h and its grace to 1h, so a missed run emails within the hour. The monitor has
to run *elsewhere*: a monitor on the box cannot tell you the box is down, which
is precisely the failure it exists to catch.

## Rolling back

Every deploy names a git SHA; `latest` is for humans, never for a rollback.

```bash
sudo -u finflow git -C /opt/finflow checkout <sha>
sudo -u finflow /opt/finflow/.venv/bin/pip install -e /opt/finflow
sudo systemctl start finflow-daily.service     # smoke it before the next timer
```

The ops store migrates forward on start and is never migrated back. Rolling the
code back past a migration is the one case that needs a restore
(`finflow-backup --restore`), which is why the restore drill is monthly rather
than theoretical.
