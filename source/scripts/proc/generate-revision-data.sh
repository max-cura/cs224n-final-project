#!/bin/sh

preproc-fast/target/release/preproc-fast \
  generate-revision-set \
    --in-history data/inputs/PostHistory.xml \
    --in-qidx data/proc/question_idx.txt \
    --out-file data/proc/revision_set.tsv \
    --flush-interval 1000000 \
    --qcount 23273009 --pcount 234510258
# generates 13320049