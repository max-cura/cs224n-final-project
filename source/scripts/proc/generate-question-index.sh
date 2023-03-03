#!/bin/sh

preproc-fast/target/release/preproc-fast \
  generate-question-index  \
    --in-file data/inputs/Posts.xml \
    --out-file data/proc/question_idx.txt \
    --tcount 115443102 --flush-interval 1000000
#output: 23273009