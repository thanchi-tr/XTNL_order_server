# XTNL local backup job image (home-server plan 3.2 option A, work item W3).
# Runbook: docs/runbooks/home-backup-restore.md. Built and run only through compose profile `backup` of
# deploy/local/docker-compose.local.yml (deploy/local/backup-pg.ps1), never by a plain `up`.
#
# Why these pieces:
#   * FROM postgres:17, the same image as the stack's postgres service, so pg_dump / pg_dumpall / pg_restore are
#     the server's major version (a newer pg_dump would also work, an older one refuses).
#   * rclone is copied from the official rclone image at a pinned tag: one static Go binary, newer than the
#     Debian package and with the native B2 backend. Pinned: rclone/rclone:1.69.1.
#   * age comes from the base image's Debian release through its signed apt archive (Debian bookworm ships
#     age 1.1.1, trixie 1.2.1; which one depends on the Debian release under postgres:17 when the image is built).
#     Record the versions after a build with:
#         docker compose -f deploy/local/docker-compose.local.yml --profile backup run --rm -T backup versions
#   * ca-certificates: the TLS roots rclone uses. No curl: the job sends no Healthchecks.io ping (backup-pg.ps1
#     sends every check C ping itself, outside Docker; home-server review round 2).
# The image holds no secret and no rclone.conf. At run time only the non-secret settings (the bucket, the age
# PUBLIC key) arrive as environment variables, passed by name by backup-pg.ps1. The secrets (the B2 key id, the B2
# key) arrive on the job's stdin (XTNL_BACKUP_SECRETS=stdin): never bake them into this image and never pass them
# as -e or environment variables (docker inspect would show them). The check C URL never reaches this image, not
# even on stdin.
FROM rclone/rclone:1.69.1 AS rclone

FROM postgres:17
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends age ca-certificates; \
    rm -rf /var/lib/apt/lists/*
COPY --from=rclone /usr/local/bin/rclone /usr/local/bin/rclone
COPY entrypoint.sh /usr/local/bin/xtnl-backup
# A checkout with CRLF line ends would break /bin/sh: strip them. Then fail the build if a tool is missing.
RUN sed -i 's/\r$//' /usr/local/bin/xtnl-backup \
    && chmod 0755 /usr/local/bin/xtnl-backup \
    && rclone version > /dev/null \
    && age --version > /dev/null \
    && pg_dump --version > /dev/null
# Runs as the image's unprivileged postgres user (uid 999); compose also drops every capability.
USER postgres
ENTRYPOINT ["/usr/local/bin/xtnl-backup"]
CMD ["backup"]
