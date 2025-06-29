#!/bin/sh

mkdir -p ~/temp/mount/qt
/usr/bin/time -v \
  rsync -ah ~/temp/mount/qt4 ~/temp/mount/qt

sudo umount mount