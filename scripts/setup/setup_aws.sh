#!/bin/sh

sudo apt install python3-pip

pip install -c huggingface transformers
pip install datasets scikit-learn sacrebleu evaluate torch sentencepiece tqdm numpy
