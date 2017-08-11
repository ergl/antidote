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

adb-stop() {
    echo "Stopping antidote..."
    "${TARGET}"/env stop > /dev/null 2>&1
}

adb-start() {
    echo "Starting antidote, please wait..."
    "${TARGET}"/env start > /dev/null 2>&1
    sleep 1
    "${TARGET}"/env ping > /dev/null 2>&1
    echo "Done"
}

adb-rebuild() {
    echo "Rebuilding antidote..."
    rebuild > /dev/null 2>&1
    change "${TARGET}"/antidote > /dev/null 2>&1
}

adb-full() {
    adb-stop
    adb-rebuild
    adb-start
}

main() {
    if [[ $# -eq 0 ]]; then
        adb-full
        exit 0
    fi
}

main "$@"
