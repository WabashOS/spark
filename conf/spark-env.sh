#!/usr/bin/env bash
CONF_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export LD_LIBRARY_PATH=${CONF_DIR}/../rmemLib:$LD_LIBRARY_PATH
