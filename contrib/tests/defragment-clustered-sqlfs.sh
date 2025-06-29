#!/bin/sh

dod=~/src/dedupsqlfs/bin/do.dedupsqlfs

/usr/bin/time -vp python3.13 $dod \
    --data ~/sqlfs \
    --data-clustered ~/clsqlfs \
    --block-partitions 10 \
     --no-sync \
    --minimal-compress-size -1 \
    --compress zstd \
    --journal-mode off \
    --auto-vacuum 2 \
    -v -v -M \
    --log-file defragment-clustered-dedupsqlfs.log \
    --defragment-clustered
