#!/bin/zsh -euo pipefail
cd "`dirname $0`/.."

clj -A:transit -M -m user $@
