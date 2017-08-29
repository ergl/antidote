#!/usr/bin/env bash

TARGET="_build/default/rel/antidote/bin"

rebuild() {
    make relclean
    make rel
}

change() {
    sed -i.bak -e '20,22d;19i\
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd -P)"' "${1}"
}

simple-ping() {
    "${TARGET}"/env ping > /dev/null 2>&1
}

adb-ping() {
    simple-ping
    local status=$?
    if [[ status -eq 0 ]]; then
        echo "pong"
    else
        echo "pang"
    fi
}

adb-stop() {
    echo "Stopping antidote..."
    "${TARGET}"/env stop > /dev/null 2>&1
}

adb-start() {
    echo "Starting antidote, please wait..."
    "${TARGET}"/env start > /dev/null 2>&1
    sleep 1
    simple-ping
    echo "Done"
}

change-script() {
    change "${TARGET}"/antidote > /dev/null 2>&1
}

adb-rebuild() {
    echo "Rebuilding antidote..."
    rebuild > /dev/null
    change-script
}

adb-full() {
    adb-stop
    adb-rebuild
    adb-start
}

display-help() {
    echo -e "Usage: ${0##*/} -frspch\n\
    -f\tRebuild and restart Antidote.
    -r\tRestart Antidote.
    -s\tStop Antidote.
    -p\tPing Antidote.
    -c\tChange run script.
    -h\tDisplay this message."
}

main() {
    if [[ $# -eq 0 ]]; then
        display-help
        exit 0
    fi

    while getopts ":sfrpch" opt; do
        case ${opt} in
            s)
                adb-stop
                exit $?
                ;;
            r)
                adb-stop
                adb-start
                exit $?
                ;;
            f)
                adb-full
                exit $?
                ;;
            p)
                adb-ping
                exit $?
                ;;
            c)
                change-script
                exit $?
                ;;
            h)
                display-help
                exit 0
                ;;
            *)
                echo "Invalid option: -${OPTARG}"
                display-help
                exit 1
                ;;
        esac
    done
}

main "$@"
