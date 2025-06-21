#!/bin/sh
####
# This script will try to build
# every available compression module
# If build fails - it will continue to another module
###


THIS_DIR=`dirname $0`

if [ -z "$PY" ]; then
	# generic
	PY=python3
fi

PY=`which $PY`

EXTRA_OPT=$1
if [ -n "${EXTRA_OPT}" ]; then
#	EXTRA_OPT="--extra-optimization"
	export RC_EXTRAOPT=1
	export CFLAGS="-O3 -march=native"
else
	EXTRA_OPT=""
fi

ONLY_MODULE=$2

if [ "${EXTRA_OPT}" = "-h" ]; then
	echo "Usage: [env PY=python3.x] $0 [extra-optimizations=/y only-module=/zstd/..]"
	exit 0
fi

if [ -z "$CC" ]; then
	export CC=gcc
fi
if [ -z "$CXX" ]; then
	export CXX=g++
fi

cd "${THIS_DIR}/../lib-dynload"

for mdir in `ls .`
do
	if [ -d ${mdir} ]; then

		if [ -n "$ONLY_MODULE" ] && [ "$ONLY_MODULE" != "$mdir" ]; then
			continue
		fi

		if [ ! -r ${mdir}/setup.py ]; then
			continue
		fi

		echo ""
		echo "MODULE rebuild: ${mdir}"

		cd ${mdir}
		$PY setup.py clean -a
		$PY setup.py build_ext ${EXTRA_OPT} clean || (echo "---= !ERROR! =---" && $PY setup.py clean -a)
		cd ..
	fi
done
