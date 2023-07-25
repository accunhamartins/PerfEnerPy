#!/bin/bash

source ~/miniconda3/etc/profile.d/conda.sh

# Create Conda environments
conda env create --file envs/env_modin.yml
conda env create --file envs/env_pyspark.yml

# Create Polars venv
python3 -m venv Polars
source Polars/bin/activate
python -m pip install --upgrade polars
python -m pip install --upgrade pandas
python -m pip install --upgrade pyarrow
deactivate

# Create Pandas venv
python3 -m venv Pandas
source Pandas/bin/activate
python -m pip install --upgrade pandas
python -m pip install --upgrade pyarrow
deactivate

# Create Plot venv
python3 -m venv plot
source plot/bin/activate
python -m pip install --upgrade numpy
python -m pip install --upgrade pandas
python -m pip install --upgrade matplotlib
deactivate

#Download NYC TLC data to local parquet folders
source Pandas/bin/activate
python src/download-NYC-TLC-data.py --start_year 2019 --end_year 2019 --months 4
deactivate
