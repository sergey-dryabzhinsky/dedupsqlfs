#!/bin/sh

mnt=~/src/dedupsqlfs/bin/mount.dedupsqlfs

python3.13 $mnt \
    --data ~/sqlfs \
    --data-clustered ~/clsqlfs \
    --block-partitions 10 \
    --no-cache-flusher --no-sync \
    --minimal-compress-size -1 \
    --flush-interval 5 \
    --block-size 65536 \
    --journal-mode off \
    --auto-vacuum 2 \
    --compress zstd \
    -v -v -M --verbose-stats \
    -o noatime \
    --log-file mount-dedupsqlfs.log \
    ~/temp/mount