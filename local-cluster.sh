#!/usr/bin/env bash

set -euo pipefail

export RELX_REPLACE_OS_VARS=true

an-stop() {
    local n="${1}"
    for i in $(seq 1 "${n}"); do
        INSTANCE_NAME=antidote${i} _build/default/rel/antidote${i}/bin/env stop
    done
}

rel() {
    local n="${1}"
    for i in $(seq 1 "${n}"); do
        ./rebar3 release -n antidote${i}
        ./change_script.sh _build/default/rel/antidote${i}/bin/antidote${i}
    done
}

an-run() {
    local n="${1}"
    for i in $(seq 1 "${n}"); do
        PLATFORM_DATA_DIR="data/${i}" \
            RING_STATE_DIR="data/ring/${i}" \
            HANDOFF_PORT=8${i}99 \
            PB_PORT=8${i}87 \
            PUBSUB_PORT=8${i}86 \
            LOGREADER_PORT=8${i}85 \
            RUBIS_PB_PORT=7${i}78 \
            INSTANCE_NAME=antidote${i} \
            ./_build/default/rel/antidote${i}/bin/env start

        sleep 2

        INSTANCE_NAME=antidote${i} ./_build/default/rel/antidote${i}/bin/env ping
    done
}

an-ping() {
    local n="${1}"
    for i in $(seq 1 "${n}"); do
        INSTANCE_NAME=antidote${i} _build/default/rel/antidote${i}/bin/env ping
    done
}

an-rebuild() {
    set +e
    _build/default/rel/antidote/bin/env stop > /dev/null 2>&1
    rm -rf _build/default/rel
    ./rebar3 release -n antidote > /dev/null 2>&1
    ./change_script.sh _build/default/rel/antidote/bin/antidote > /dev/null 2>&1
    ./_build/default/rel/antidote/bin/env start > /dev/null 2>&1
    sleep 2
    ./_build/default/rel/antidote/bin/env ping > /dev/null 2>&1
	if [[ $? -eq 1 ]]; then
	    echo "Node not responding"
	    exit 1
	fi
	./_build/default/rel/antidote/bin/env attach
	set -e
}

main() {
    if [[ $# -eq 0 ]]; then
        an-rebuild
        exit $?
    fi

    local comm="${1}"
    local nodes="${2}"

    case "${comm}" in
        "rebuild")
            an-stop "${nodes}"
            make relclean
            rel "${nodes}"
            an-run "${nodes}"
            exit $?
            ;;

        "restart")
            make relclean
            rel "${nodes}"
            an-run "${nodes}"
            exit $?
            ;;

        "stop")
            an-stop "${nodes}"
            exit $?
            ;;
        "ping")
            an-ping "${nodes}"
            exit $?
            ;;
        *)
            exit 1
            ;;
    esac
}

main "$@"
