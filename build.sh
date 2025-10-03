#!/usr/bin/env bash

set -e
set -x

docker build --platform linux/arm64 -t kaonicradioimage .

exit 0
