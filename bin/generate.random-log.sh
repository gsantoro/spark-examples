#!/usr/bin/env bash

docker run -it  -v $PWD/../src/main/resources/streaming/random-logs:/data gsantoro/alpine-log-synth:0.1 /log-synth -count 1000 -schema /data/random-log.schema.json -template /data/random-log.template.json -format JSON -output /data/output
