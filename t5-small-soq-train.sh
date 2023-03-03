#!/bin/sh

python3 224n_ibit/train.py \
    --use-tokenizer "t5-small" \
    --dataset-type "soq" \
    --use-train-dataset $(pwd)/t5-small-soq/soq-train-7_2m.tsv \
    --use-test-dataset $(pwd)/t5-small-soq/soq-test-700k.tsv \
    --use-model "google/t5-v1_1-small" \
    --model-dir t5-small-soq/test1 \
    --metric sacrebleu \
    --train-batch-size 64 \
    --max-length 4096 \
    --use-generation-prefix "soq: "
