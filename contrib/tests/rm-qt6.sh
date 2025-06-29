#!/bin/sh

mkdir -p ~/temp/mount/qt
/usr/bin/time -v \
  rm -rf ~/temp/mount/qt6*

sudo umount mount