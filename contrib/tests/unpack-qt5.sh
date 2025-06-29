#!/bin/sh

mkdir -p ~/temp/mount/qt5
/usr/bin/time -v \
 tar -xf qt5.tar \
  -C  ~/temp/mount/qt5

sudo umount mount