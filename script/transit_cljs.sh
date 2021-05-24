#!/bin/zsh -euo pipefail
cd "`dirname $0`/.."

lein cljsbuild once transit
node transit.js
