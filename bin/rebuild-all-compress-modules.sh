#!/bin/sh
####
# This script will try to build
# every available compression module
# If build fails - it will continue to another module
###


THIS_DIR=`dirname $0`

EXTRA_OPT=$1
if [ -n "${EXTRA_OPT}" ]; then
    EXTRA_OPT="--extra-optimization"
else
    EXTRA_OPT=""
fi

cd "${THIS_DIR}/../lib-dynload"

for mdir in `ls .`
do
    if [ -d ${mdir} ]; then
        cd ${mdir}
        python3 setup.py clean -a
        python3 setup.py build_ext ${EXTRA_OPT}
        python3 setup.py build_ext clean
        cd ..
    fi
done
