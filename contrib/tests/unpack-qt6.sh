#!/bin/sh

mkdir -p ~/temp/mount/qt6
/usr/bin/time -v \
 tar -xf qt6.tar \
  -C  ~/temp/mount/qt6

sudo umount mount