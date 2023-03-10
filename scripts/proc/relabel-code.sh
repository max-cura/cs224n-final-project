#!/bin/bash

bin/preproc-fast \
  relabel-code \
    --in-revision-set data/proc/revision_set.tsv \
    --out-file data/proc/relabeled_revision_set.tsv \
    --rcount 8139450 --flush-interval 1000000
# 7889014
