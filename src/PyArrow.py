import pyarrow as pa
import pyarrow.compute as pc
import pandas as pd
import numpy as np

class PyArrow:
    def load(self, file_path):
        df = pd.read_parquet(file_path+".parquet")
        self.df = pa.Table.from_pandas(df)

    def filter(self):
        self.df = self.df.filter((pc.field("tip_amount") >= 1) & (pc.field("tip_amount") <= 5))

    def mean(self):
        return pa.compute.mean(self.df.column("passenger_count"))

    def sum(self):
         add_1 = pa.compute.add(self.df.column("fare_amount"),self.df.column("extra"))
         add_2 = pa.compute.add(self.df.column("mta_tax"),self.df.column("tip_amount"))
         add_3 = pa.compute.add(self.df.column("tolls_amount"),self.df.column("improvement_surcharge"))
         add_x = pa.compute.add(add_1, add_2)

         return pa.compute.add(add_x, add_3)
    
    def unique_rows(self):
        return self.df.column("VendorID").value_counts()

    def groupby(self):
        return pa.TableGroupBy(self.df, "passenger_count").aggregate([("tip_amount", "mean")])

    def multiple_groupby(self):
        return pa.TableGroupBy(self.df, ["passenger_count", "payment_type"]).aggregate([("tip_amount", "mean")])

    def join(self):
        payments =  pd.DataFrame({'payments': ['Credit Card', 'Cash', 'No Charge', 'Dispute', 'Unknown', 'Voided trip'],'payment_type':[1,2,3,4,5,6]})
        t1 = pa.Table.from_pandas(payments)
        fill_value = pa.compute.mean(self.df.column("payment_type"))
        self.df.column("payment_type").fill_null(fill_value)
        return self.df.join(t1, 'payment_type',join_type="full outer")
    
    def sort(self):
        return self.df.sort_by('total_amount')