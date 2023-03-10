#!/bin/sh

python3 224n_ibit/train.py \
    --use-tokenizer "t5-small" \
    --dataset-type "soq" \
    --use-train-dataset $(pwd)/t5-small-soq/soq-train-micro1k.tsv \
    --use-test-dataset $(pwd)/t5-small-soq/soq-test-micro1k.tsv \
    --use-model "t5-small" \
    --model-dir t5-small-soq/test1 \
    --metric bleu \
    --train-batch-size 8 \
    --max-length 1536 \
    --use-generation-prefix "soq: "
