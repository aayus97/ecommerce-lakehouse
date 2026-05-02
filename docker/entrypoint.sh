#!/usr/bin/env bash
set -euo pipefail

export HOME="${HOME:-/tmp/lakehouse-home}"
mkdir -p "${HOME}"

if ! whoami >/dev/null 2>&1; then
    export NSS_WRAPPER_PASSWD=/tmp/passwd.nss_wrapper
    export NSS_WRAPPER_GROUP=/tmp/group.nss_wrapper

    cat /etc/passwd > "${NSS_WRAPPER_PASSWD}"
    cat /etc/group > "${NSS_WRAPPER_GROUP}"
    printf 'lakehouse:x:%s:%s:Lakehouse User:%s:/usr/sbin/nologin\n' "$(id -u)" "$(id -g)" "${HOME}" >> "${NSS_WRAPPER_PASSWD}"
    printf 'lakehouse:x:%s:\n' "$(id -g)" >> "${NSS_WRAPPER_GROUP}"

    for wrapper in /usr/lib/*/libnss_wrapper.so /usr/lib/libnss_wrapper.so; do
        if [ -f "${wrapper}" ]; then
            export LD_PRELOAD="${wrapper}${LD_PRELOAD:+:${LD_PRELOAD}}"
            break
        fi
    done
fi

exec "$@"
