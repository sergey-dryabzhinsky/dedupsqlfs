#!/bin/sh

mkdir -p ~/temp/mount/qt
/usr/bin/time -v \
  rsync -ah ~/temp/mount/qt6 ~/temp/mount/qt

sudo umount mount