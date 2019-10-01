#!/usr/bin/env bash

main() {
    if [[ $# -lt 1 ]]; then
        echo "./build.sh cmd profile || cmd := compile | run | stop | restart"
        exit 1
    fi

    local command="${1}"
    local profile="${2:-default}"

    case "${command}" in
        "compile")
            ./rebar3 as "${profile}" compile
            ./rebar3 as "${profile}" release -n antidote
            ./change_script.sh _build/"${profile}"/rel/antidote/bin/antidote
            ;;
        "run")
            _build/"${profile}"/rel/antidote/bin/env start
            sleep 2
            _build/"${profile}"/rel/antidote/bin/env ping
            ;;
        "stop")
            _build/"${profile}"/rel/antidote/bin/env stop
            ;;
        "restart")
            _build/"${profile}"/rel/antidote/bin/env stop
            rm -rf _build/"${profile}"/rel
            ./rebar3 as "${profile}" compile
            ./rebar3 as "${profile}" release -n antidote
            ./change_script.sh _build/"${profile}"/rel/antidote/bin/antidote
            _build/"${profile}"/rel/antidote/bin/env start
            sleep 2
            _build/"${profile}"/rel/antidote/bin/env ping
            ;;

    esac
}

main "$@"
