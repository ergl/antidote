#!/bin/bash
set -e

if [ ! -f /opt/antidote/releases/0.0.1/setup_ok ]; then
  cd /opt/antidote/releases/0.0.1/
  if [ "$SHORT_NAME" = "true" ]; then
    sed -i.backup "s|^-name |-sname |g" vm.args
  fi
  cd /opt/antidote/bin/
  sed -i.backup "s|^#!/bin/sh|#!/bin/bash|" antidote
  touch setup_ok
fi

exec "$@"
