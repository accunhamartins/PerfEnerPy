#!/bin/bash

#Setup conda
source ~/miniconda3/etc/profile.d/conda.sh

WORKFLOWS="macro_1Y_v2 macro_2Y_v2"
#CONDA_FRAMEWORKS=""
VENV_FRAMEWORKS="Pandas Polars"
MODIN_FRAMEWORKS="Modin_Dask Modin_Ray" 

for WORKFLOW in $WORKFLOWS; do
    echo "Running $WORKFLOW"
    sudo rm -rf energy/$WORKFLOW  
    mkdir energy/$WORKFLOW

    for CONDA_FRAMEWORK in $CONDA_FRAMEWORKS; do
        echo "Running $CONDA_FRAMEWORK"
        #Running powerjoular
        conda activate $CONDA_FRAMEWORK
        sudo powerjoular -f energy/$WORKFLOW/energy_$CONDA_FRAMEWORK.csv > energy/$WORKFLOW/energy_$CONDA_FRAMEWORK.txt 2>&1 &
        python src/benchmark.py --workflow $DATASET --framework $CONDA_FRAMEWORK  > $CONDA_FRAMEWORK.out 2>&1 &
        PID=$!
        wait $PID
        sudo pkill -2 powerjoular
        python src/energy_dataset.py --workflow $WORKFLOW --framework $CONDA_FRAMEWORK
        conda deactivate
    done

    for MODIN_FRAMEWORK in $MODIN_FRAMEWORKS; do
        echo "Running $MODIN_FRAMEWORK"
        #Running Powerjoular
        conda activate Modin
        sudo powerjoular -f energy/$WORKFLOW/energy_$MODIN_FRAMEWORK.csv > energy/$WORKFLOW/energy_$MODIN_FRAMEWORK.txt 2>&1 &
        python src/benchmark.py --workflow $WORKFLOW --framework $MODIN_FRAMEWORK  > $MODIN_FRAMEWORK.out 2>&1 &
        PID=$!
        wait $PID
        sudo pkill -2 powerjoular
        python src/energy_dataset.py --workflow $WORKFLOW --framework $MODIN_FRAMEWORK
        conda deactivate
    done

    for VENV_FRAMEWORK in $VENV_FRAMEWORKS; do
        echo "Running $VENV_FRAMEWORK"
        #Running Powerjoular
        source $VENV_FRAMEWORK/bin/activate
        sudo powerjoular -f energy/$WORKFLOW/energy_$VENV_FRAMEWORK.csv > energy/$WORKFLOW/energy_$VENV_FRAMEWORK.txt 2>&1 &
        python src/benchmark.py --workflow $WORKFLOW --framework $VENV_FRAMEWORK  > $VENV_FRAMEWORK.out 2>&1 &
        PID=$! 
        wait $PID
        sudo pkill -2 powerjoular
        python src/energy_dataset.py --workflow $WORKFLOW --framework $VENV_FRAMEWORK
        deactivate
    done
done
