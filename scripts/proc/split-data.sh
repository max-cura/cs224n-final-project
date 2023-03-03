#!/bin/sh

if ! [[ -d $SPLIT_OUTPUT_DIR ]] ; then
  echo "SPLIT_OUTPUT_DIR does not have a valid directory"
  exit 1
fi

SPLIT_TEST_FILE="soq-test-700k.tsv"
SPLIT_TRAIN_FILE="soq-train-7_2m.tsv"

# 7189014 + 1 for the input\toutput line
split -l7189015 data/proc/relabeled_revision_set.tsv tmp-soq-parse-set.tsv.

mv tmp-soq-parse-set.tsv.aa $SPLIT_OUTPUT_DIR/$SPLIT_TRAIN_FILE
echo -e "input\toutput" > $SPLIT_OUTPUT_DIR/$SPLIT_TEST_FILE
cat tmp-soq-parse-set.tsv.ab >> $SPLIT_OUTPUT_DIR/$SPLIT_TEST_FILE
rm tmp-soq-parse-set.tsv.ab