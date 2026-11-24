#!/usr/bin/env bash
# Copy the Snowflake private keys to owner-only files, then run the command.
#
# Compose mounts the keys under /run/secrets. A file mounted from a Windows
# host shows up as 0777, and the Snowflake connector refuses a key file that
# is writable by group or others; compose ignores uid/gid/mode on secrets
# outside Swarm. So each key named by a *_KEY_PATH variable below is copied
# to PROJECT_KEYS_DIR (a tmpfs in compose: the copy never reaches the disk)
# with mode 0400, and the variable is pointed at the copy.
#
# A variable that is unset or names a missing file is left as it is, so the
# application still reports the problem by variable name.
#
#   with-private-keys.sh python -m ingestion.check_snowflake
set -euo pipefail

keys_dir="${PROJECT_KEYS_DIR:-/tmp/project-keys}/$(id -u)"
# Owner-only while the copies are made; the command gets the caller's umask back, or
# everything it writes (batches, exports) would be unreadable to other users.
previous_umask="$(umask)"
umask 077
mkdir -p "${keys_dir}"
chmod 0700 "${keys_dir}"

for var in SNOWFLAKE_PRIVATE_KEY_PATH SNOWFLAKE_DBT_PRIVATE_KEY_PATH \
    SNOWFLAKE_REPORTER_PRIVATE_KEY_PATH; do
    src="${!var:-}"
    if [[ -n "${src}" && -f "${src}" ]]; then
        dst="${keys_dir}/${var,,}.p8"
        rm -f "${dst}"
        cp "${src}" "${dst}"
        chmod 0400 "${dst}"
        export "${var}=${dst}"
    fi
done

umask "${previous_umask}"
exec "$@"
