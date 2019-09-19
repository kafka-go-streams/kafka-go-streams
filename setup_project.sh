#!/bin/bash

export CGO_CFLAGS="-I/home/victor/sources/rocksdb/include" \
export CGO_LDFLAGS="-L/home/victor/sources/rocksdb -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" \
export GO111MODULE=on
  #go get github.com/tecbot/gorocksdb
