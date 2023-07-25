import logging
from typing import List

import modin.pandas as mpd
from modin.config import Engine

from fuzzydata.clients.pandas import DataFrameWorkflow

from fuzzydata.core.workflow import Workflow
from fuzzydata.core.artifact import Artifact
from fuzzydata.core.generator import generate_table
from fuzzydata.core.operation import Operation, T

logger = logging.getLogger(__name__)
    
class ModinArtifact(Artifact):

    def __init__(self, *args, **kwargs):
        self.pd = kwargs.pop("pd", mpd)
        from_df = kwargs.pop("from_df", None)
        super(ModinArtifact, self).__init__(*args, **kwargs)
        self._deserialization_function = {
            'parquet': self.pd.read_parquet
        }
        self._serialization_function = {
            'parquet': 'to_parquet'
        }

        self.operation_class = ModinOperation
        self.table = None
        self.in_memory = False

        if from_df is not None:
            self.from_df(from_df)

    def generate(self, num_rows, schema):
        self.table = generate_table(num_rows, column_dict=schema)
        self.table.reset_index(drop = True, inplace = True)
        self.schema_map = schema
        self.in_memory = True

    def from_df(self, df):
        self.table = df
        self.table.reset_index(drop = True, inplace = True)
        self.in_memory = True

    def deserialize(self, filename=None):
        if not filename:
            filename = self.filename

        self.table = self._deserialization_function[self.file_format](filename)
        self.in_memory = True

    def serialize(self, filename=None):
        if not filename:
            filename = self.filename

        if self.in_memory:
            serialization_method = getattr(self.table, self._serialization_function[self.file_format])
            serialization_method(filename)

    def destroy(self):
        del self.table

    def to_df(self) -> mpd.DataFrame:
        return self.table

    def __len__(self):
        if self.in_memory:
            return len(self.table)


class ModinOperation(Operation['ModinArtifact']):
    def __init__(self, *args, **kwargs):
        self.artifact_class = kwargs.pop('artifact_class', ModinArtifact)
        super(ModinOperation, self).__init__(*args, **kwargs)
        self.code = 'self.sources[0].table' # Starting point for chained code generation.

    def apply(self, numeric_col: str, a: float, b: float) -> ModinArtifact:
        super(ModinOperation, self).apply(numeric_col, a, b)
        new_col_name = f"{numeric_col}__{int(a)}x_{int(b)}"
        return f'.assign({new_col_name} = lambda x: x.{numeric_col}*{a}+{b})'

    def sample(self, frac: float) -> ModinArtifact:
        super(ModinOperation, self).sample(frac)
        return f'.sample(frac={frac})'

    def groupby(self, group_columns: List[str], agg_columns: List[str], agg_function: str) -> T:
        super(ModinOperation, self).groupby(group_columns, agg_columns, agg_function)
        logger.debug(f"Groupby on {self.sources[0].label} : {group_columns}/{agg_columns}")
        return f'[{group_columns+agg_columns}].groupby({group_columns}).{agg_function}().reset_index()'

    def project(self, output_cols: List[str]) -> T:
        super(ModinOperation, self).project(output_cols)
        return f'[{output_cols}]'

    def select(self, condition: str) -> T:
        super(ModinOperation, self).select(condition)
        return f'.query("{condition}")'

    def merge(self, key_col: List[str]) -> T:
        super(ModinOperation, self).merge(key_col)
        return f'.merge(self.sources[1].table, on="{key_col}")'

    def pivot(self, index_cols: List[str], columns: List[str], value_col: List[str], agg_func: str) -> T:
        super(ModinOperation, self).pivot(index_cols, columns, value_col, agg_func)
        return f'.pivot_table(index={index_cols}, columns={columns},values={value_col},aggfunc="{agg_func}")'

    def fill(self, col_name: str, old_value, new_value):
        super(ModinOperation, self).fill(col_name, old_value, new_value)
        return f'.replace({{ "{col_name}": {old_value} }}, {new_value})'

    def chain_operation(self, op, args):
        self.code += getattr(self, op)(**args)
        super(ModinOperation, self).chain_operation(op, args)

    def materialize(self, new_label):
        new_df = eval(self.code)
        super(ModinOperation, self).materialize(new_label)
        return self.artifact_class(label=self.new_label,
                                   from_df=new_df,
                                   schema_map=self.current_schema_map)
    
    @property
    def export_code(self):
        ''' Returns a string representation of code to run outside fuzzydata'''
        code = self.code
        for ix in range(len(self.sources)):
            code = code.replace(f'self.sources[{ix}].table', self.sources[ix].label)


class ModinWorkflow(DataFrameWorkflow):
    def __init__(self, *args, **kwargs):
        self.modin_engine = kwargs.pop('modin_engine', 'dask')
        super(ModinWorkflow, self).__init__(*args, **kwargs)
        self.artifact_class = ModinArtifact
        self.operator_class = ModinOperation

        self.wf_code_export = self.wf_code_export.replace("import pandas as pd", "import modin.pandas as pd")

        if self.modin_engine == 'dask':
            from dask.distributed import Client
            processes = kwargs.pop('processes', True)
            Client(processes=processes)
            dask_code=f"\nfrom dask.distributed import Client\nClient(processes={processes})"
            self.wf_code_export += dask_code
        elif self.modin_engine == 'unidist':
            import modin.config as modin_cfg
            import unidist.config as unidist_cfg
            import unidist

            unidist.init()
            modin_cfg.Engine.put('unidist') # Modin will use Unidist
            unidist_cfg.Backend.put('mpi') # Unidist will use MPI backend

            mpi_code=f"\nimport modin.config as modin_cfg\nimport unidist.config as unidist_cfg\nimport unidist\nunidist.init()\nmodin_cfg.Engine.put('unidist')\nunidist_cfg.Backend.put('mpi')\n"

            self.wf_code_export += mpi_code
        else:
            import ray
            ray.init(ignore_reinit_error=True)
            ray_code=f"\nimport ray\nray.init(ignore_reinit_error=True)"
            self.wf_code_export += ray_code
        Engine.put(self.modin_engine)

    def initialize_new_artifact(self, label=None, filename=None, schema_map=None):
        return ModinArtifact(label, filename=filename, schema_map=schema_map)
