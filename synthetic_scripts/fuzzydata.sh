#!/usr/bin/env bash

nc=20
nr=1000
nv=15
b=100
m=4

outdir='../output/fuzzydata_scaling_test_100_4'

python ../fuzzydata/cli.py --wf_client=pandas \
                            --output_dir=$outdir/Base_1000 \
                            --matfreq=$m --bfactor=$b \
