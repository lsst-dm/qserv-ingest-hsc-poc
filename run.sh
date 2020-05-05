#!/bin/bash

DIR=$(cd $(dirname $0) && pwd)

if [ $# -ne 1 ]; then
    echo "Usage: $0 DAXFILE"
    exit 1
fi

DAXFILE=$1

pegasus-plan \
    -Dpegasus.catalog.site.file=sites.xml \
    -Dpegasus.catalog.transformation.file=tc.txt \
    -Dpegasus.register=false \
    -Dpegasus.data.configuration=sharedfs \
    --input-dir $DIR/input \
    --output-dir $DIR/output \
    --dir $DIR/submit \
    --dax $DAXFILE \
    --submit 
