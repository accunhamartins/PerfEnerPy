#!/usr/bin/env bash


outdir='../synthetic_output/scaling_test_100_4/'

for scale in 1000 10000 100000 1000000 5000000 10000000 20000000 30000000;
do
  python ../fuzzydata/fuzzydata/cli.py  --wf_client=pandas \
                               --replay_dir=$outdir/Base_${scale}/ \
                               --output_dir=$outdir/Pandas_${scale}/ 

  python ../fuzzydata/fuzzydata/cli.py  --wf_client=polars \
                             --replay_dir=$outdir/Base_${scale}/ \
                             --output_dir=$outdir/Polars_${scale}/ 

  python ../fuzzydata/fuzzydata/cli.py  --wf_client=modin \
                              --replay_dir=$outdir/Base_${scale}/ \
                              --output_dir=$outdir/Modin_Dask_${scale}/ \
                              --wf_options='{"modin_engine": "dask"}' 

   
  python ../fuzzydata/fuzzydata/cli.py  --wf_client=modin \
                             --replay_dir=$outdir/Base_${scale}/ \
                             --output_dir=$outdir/Modin_Ray_${scale}/ \
                             --wf_options='{"modin_engine": "ray"}' 

done



