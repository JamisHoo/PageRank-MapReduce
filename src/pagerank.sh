#!/bin/sh

INPUT=input2
TMPDIR=pagerank_tmp
OUTPUT=output
KROUNDS=10


for ((i=0;i<$KROUNDS;++i)); do
    if (($i == 0)); then
        INPUT_DIR=$INPUT
    else
        INPUT_DIR=${TMPDIR}$(($i - 1))
    fi

    bin/mapred pipes -conf conf.xml \
    -input $INPUT_DIR -output ${TMPDIR}$i -program bin/pagerank

    rtv=$?

    if (($rtv == 0 && i != 0)); then
        bin/hdfs dfs -rm -r $INPUT_DIR
        if ((i == $KROUNDS - 1)); then
            bin/hdfs dfs -mv $INPUT_DIR $OUTPUT
        fi
    elif (($rtv != 0)); then
        break
    fi
    
    done

