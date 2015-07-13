#!/bin/sh

INPUT=input2
TMPDIR=pagerank_tmp
OUTPUT=output
KROUNDS=10


for ((i=0;i<$KROUNDS;++i)); do
    if (($i == 0)); then
        STEP_INPUT_DIR=$INPUT
    else
        STEP_INPUT_DIR=${TMPDIR}$(($i - 1))
    fi

    bin/mapred pipes -conf conf.xml \
    -input $STEP_INPUT_DIR -output ${TMPDIR}$i -program bin/pagerank_iter

    rtv=$?

    if (($rtv == 0 && i != 0)); then
        bin/hdfs dfs -rm -r $STEP_INPUT_DIR
    elif (($rtv != 0)); then
        break
    fi
    
    done

bin/mapred pipes -conf conf0.xml \
-input ${TMPDIR}$(($KROUNDS - 1)) -output $OUTPUT -program bin/pagerank_resultsort
