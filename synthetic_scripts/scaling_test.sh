#!/usr/bin/env bash

outdir='../synthetic_output/scaling_test_100_4'

for scale in 10000 100000 1000000 5000000 10000000 20000000 30000000;
do
  python ../fuzzydata/fuzzydata/cli.py --wf_client=pandas \
                             --replay_dir=$outdir/Base_1000/ \
                             --output_dir=$outdir/Base_${scale}/ \
                             --scale_artifact='{"artifact_0": '$scale'}' 
done


