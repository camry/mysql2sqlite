#!/usr/bin/env bash

go build -o mysql2sqlite mysql2sqlite.go && \
upx -qvf ./mysql2sqlite

exit 0
