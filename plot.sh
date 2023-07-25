#!/bin/bash

source plot/bin/activate
python src/show_results.py --config plot_macro
python src/energy.py --config plot_macro
deactivate
