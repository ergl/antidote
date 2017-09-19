#!/usr/bin/env bash

main() {
    sed -i.bak -e '20,22d;19i\
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd -P)"' "${1}"
}

main "$@"
