#!/bin/sh

mkdir -p ~/temp/mount/qt4
/usr/bin/time -v \
 tar -xf qt4.tar \
  -C  ~/temp/mount/qt4

sudo umount mount