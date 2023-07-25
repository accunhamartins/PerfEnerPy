#!/usr/bin/env bash

indir='../synthetic_output/scaling_test_100_4/Base_1000'
outdir='../synthetic_output/scaling_test_100_4/Modin_Dask'

python ../fuzzydata/fuzzydata/cli.py --wf_client=modin \
                            --replay_dir=$indir \
                            --output_dir=$outdir \
                            --wf_options='{"modin_engine": "ray"}'