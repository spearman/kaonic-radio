#!/usr/bin/env bash

set -e
set -x

docker run -dit --rm --net=host --platform linux/arm64 --name kaonicradiocontainer \
  kaonicradioimage bash

exit 0
