#!/bin/sh
####
# This script will try to build
# every available compression module
# If build fails - it will continue to another module
###


THIS_DIR=`dirname $0`

if [ -z "$PY" ]; then
	PY=python3
fi

PY=`which $PY`

EXTRA_OPT=$1
if [ -n "${EXTRA_OPT}" ]; then
	EXTRA_OPT="--extra-optimization"
else
	EXTRA_OPT=""
fi

ONLY_METHOD=$2

cd "${THIS_DIR}/../lib-dynload"

for mdir in `ls .`
do
	if [ -d ${mdir} ]; then

		if [ -n "$ONLY_METHOD" ] && [ "$ONLY_METHOD" != "$mdir" ]; then
			continue
		fi

		cd ${mdir}
		$PY setup.py clean -a
		$PY setup.py build_ext ${EXTRA_OPT} clean
		cd ..
	fi
done
