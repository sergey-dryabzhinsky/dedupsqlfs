#!/bin/sh

mkdir -p ~/temp/mount/qt
/usr/bin/time -v \
  rm -rf ~/temp/mount/qt4*

sudo umount mount