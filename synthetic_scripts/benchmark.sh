#!/usr/bin/env bash

outdir='../synthetic_output/scaling_test_100_4/'
nc=20
nr=1000
nv=15
b=100
m=4

python ../fuzzydata/fuzzydata/cli.py --wf_client=pandas  \
                                    --output_dir=$outdir/Base_1000 \
                                    --columns=$nc --rows=$nr \
                                    --version=$nv --bfactor=$b \
                                    --matfreq=$m --exclude_ops='["groupby"]'

