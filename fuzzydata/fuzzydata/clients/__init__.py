import importlib
from copy import deepcopy

from fuzzydata.clients.pandas import DataFrameWorkflow
from fuzzydata.clients.pyspark import PySparkWorkflow
from fuzzydata.clients.sqlite import SQLWorkflow
from fuzzydata.clients.polars import PolarsWorkflow
from fuzzydata.clients.modin import ModinWorkflow

travis_workflows = {
    'pandas': DataFrameWorkflow,
    'sql': SQLWorkflow,
    'pyspark': PySparkWorkflow,
    'polars': PolarsWorkflow,
    'modin': ModinWorkflow
}

supported_workflows = deepcopy(travis_workflows)

modin_spec = importlib.util.find_spec('modin')
if modin_spec:
    from fuzzydata.clients.modin import ModinWorkflow
    supported_workflows['modin'] = ModinWorkflow
