import logging
from typing import List

import polars

import pandas

from fuzzydata.core.artifact import Artifact
from fuzzydata.core.generator import generate_table
from fuzzydata.core.operation import Operation, T
from fuzzydata.core.workflow import Workflow

logger = logging.getLogger(__name__)


class PolarsArtifact(Artifact):

    def __init__(self, *args, **kwargs):
        self.pd = kwargs.pop("pd", polars)
        from_df = kwargs.pop("from_df", None)
        super(PolarsArtifact, self).__init__(*args, **kwargs)
        self._deserialization_function = {
            'parquet': self.pd.read_parquet
        }
        self._serialization_function = {
            'parquet': 'write_parquet'
        }

        self.operation_class = PolarsOperation
        self.table = None
        self.in_memory = False

        if from_df is not None:
            self.from_df(from_df)
            

    def generate(self, num_rows, schema):
        df = generate_table(num_rows, column_dict=schema)
        self.table = polars.from_pandas(df)
        self.schema_map = schema
        self.in_memory = True

    def from_df(self, df):
        self.table = df
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
            if(isinstance(self.table, pandas.core.frame.DataFrame)):
                serialization_function = {
                    'parquet': 'to_parquet'
                }
                serialization_method = getattr(self.table, serialization_function[self.file_format])
                serialization_method(filename)
            else:
                serialization_method = getattr(self.table, self._serialization_function[self.file_format])
                serialization_method(filename)

    def destroy(self):
        del self.table

    def to_df(self) -> polars.DataFrame:
        return self.table

    def __len__(self):
        if self.in_memory:
            return len(self.table)


class PolarsOperation(Operation['PolarsArtifact']):
    def __init__(self, *args, **kwargs):
        self.artifact_class = kwargs.pop('artifact_class', PolarsArtifact)
        super(PolarsOperation, self).__init__(*args, **kwargs)
        self.code = 'self.sources[0].table' # Starting point for chained code generation.

    def apply(self, numeric_col: str, a: float, b: float) -> PolarsArtifact:
        super(PolarsOperation, self).apply(numeric_col, a, b)
        new_col_name = f"{numeric_col}__{int(a)}x_{int(b)}"
        return f'.assign({new_col_name} = lambda x: x.{numeric_col}*{a}+{b})'

    def sample(self, frac: float) -> PolarsArtifact:
        super(PolarsOperation, self).sample(frac)
        return f'.sample(frac={frac})'

    def groupby(self, group_columns: List[str], agg_columns: List[str], agg_function: str) -> T:
        super(PolarsOperation, self).groupby(group_columns, agg_columns, agg_function)
        logger.debug(f"Groupby on {self.sources[0].label} : {group_columns}/{agg_columns}")
        return f'.groupby(polars.col({group_columns})).agg(polars.col({agg_columns}).{agg_function}())'

    def project(self, output_cols: List[str]) -> T:
        super(PolarsOperation, self).project(output_cols)
        return f'[{output_cols}]'

    def select(self, condition: str) -> T:
        super(PolarsOperation, self).select(condition)
        return f'.query("{condition}")'

    def merge(self, key_col: List[str]) -> T:
        super(PolarsOperation, self).merge(key_col)
        return f'.join(self.sources[1].table, right_on="{key_col}" , left_on="{key_col}")'

    def pivot(self, index_cols: List[str], columns: List[str], value_col: List[str], agg_func: str) -> T:
        super(PolarsOperation, self).pivot(index_cols, columns, value_col, agg_func)
        return f'.pivot(index={index_cols}, columns={columns},values={value_col},aggregate_function="{agg_func}")'

    def fill(self, col_name: str, old_value, new_value):
        super(PolarsOperation, self).fill(col_name, old_value, new_value)
        return f'.replace({{ "{col_name}": {old_value} }}, {new_value})'

    def chain_operation(self, op, args):
        self.code += getattr(self, op)(**args)
        super(PolarsOperation, self).chain_operation(op, args)

    def materialize(self, new_label):
        new_df = eval(self.code)
        super(PolarsOperation, self).materialize(new_label)
        return self.artifact_class(label=self.new_label,
                                   from_df=new_df,
                                   schema_map=self.current_schema_map)
    
    @property
    def export_code(self):
        ''' Returns a string representation of code to run outside fuzzydata'''
        code = self.code
        for ix in range(len(self.sources)):
            code = code.replace(f'self.sources[{ix}].table', self.sources[ix].label)
        return code

class PolarsWorkflow(Workflow):
    def __init__(self, *args, **kwargs):
        super(PolarsWorkflow, self).__init__(*args, **kwargs)
        self.artifact_class = PolarsArtifact
        self.operator_class = PolarsOperation
        self.wf_code_export = "import polars\n"

    def initialize_new_artifact(self, label=None, filename=None, schema_map=None):
        return PolarsArtifact(label, filename=filename, schema_map=schema_map)
    

    def add_artifact(self, artifact: Artifact,
                    from_artifacts: List[Artifact] = None, operation: Operation = None) -> None:
        """ Override to add code export to workflow."""
        super(PolarsWorkflow, self).add_artifact(artifact, from_artifacts, operation)
        if from_artifacts:
            self.wf_code_export += f"{self.artifact_list[-1]} = {operation.export_code}\n"
        else:
            self.wf_code_export += f"{artifact.label} = polars.read_parquet('artifacts/{artifact.label}.{artifact.file_format}')\n"

    def serialize_workflow(self, output_dir: str = None) -> None:
        """ Override to add code export to workflow."""
        super(PolarsWorkflow, self).serialize_workflow(output_dir)
        if not output_dir:
            output_dir = self.out_dir
        # Write out Generated code
        with open(f"{output_dir}/{self.name}_code.py", 'w') as outfile:
            outfile.write(self.wf_code_export)
    