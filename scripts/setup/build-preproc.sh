#!/bin/sh

( cd preproc-fast ; cargo build --release )
mkdir -p bin
cp preproc-fast/target/release/preproc-fast bin