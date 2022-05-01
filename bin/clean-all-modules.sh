#!/bin/sh
####
# This script will try to clean
# every available compression module
###


THIS_DIR=`dirname $0`

if [ -z "$PY" ]; then
	# generic
	PY=python3
fi

PY=`which $PY`

cd "${THIS_DIR}/../lib-dynload"

for mdir in `ls .`
do
	if [ -d ${mdir} ]; then

		if [ ! -r ${mdir}/setup.py ]; then
			continue
		fi

		echo ""
		echo "MODULE clean: ${mdir}"

		cd ${mdir}
		$PY setup.py clean -a
		[ -d ".eggs" ] && rm -rf .eggs
		find . -type d -name "__pycache__" -exec rm -rf '{}' \;
		cd ..
	fi
done
