import polars as pl
import pandas as pd
import numpy as np
import pyarrow as pa

class Polars:
    def load(self, file_path):
        self.df = pl.read_parquet(file_path+".parquet")
        
    def filter(self):
        self.df = self.df.filter((self.df['tip_amount'] >= 1) & (self.df['tip_amount'] <= 5))

    def mean(self):
        return self.df['passenger_count'].mean()

    def sum(self):
        return self.df['fare_amount']+self.df['extra']+self.df['mta_tax']+self.df['tip_amount']+self.df['tolls_amount']+self.df['improvement_surcharge']
    
    def unique_rows(self):
        return self.df['VendorID'].value_counts()

    def groupby(self):
        return self.df.groupby("passenger_count").agg(pl.col("tip_amount").mean())

    def multiple_groupby(self):
        return self.df.groupby(["passenger_count", "payment_type"]).agg(pl.col("tip_amount").mean())

    def join(self):
        pd_payments =  pd.DataFrame({'payments': ['Credit Card', 'Cash', 'No Charge', 'Dispute', 'Unknown', 'Voided trip'],'payment_type':[1,2,3,4,5,6]})  
        pd_payments['payment_type'] = pd_payments['payment_type'].astype('int64')
        payments= pl.from_arrow(pa.Table.from_pandas(pd_payments))
        return self.df.join(payments, left_on='payment_type', right_on='payment_type')
    
    def sort(self):
        return self.df.sort("total_amount")

    
    def macro_workflow(self,operations):
        artifacts = {}
        artifacts["source"] = self.df

        for op in operations:
            operation = op["op"]
            artifacts[op["label"]] = None
            application_df = artifacts[op["artifact"]]

            if operation == "groupby":
                if op["args"]["agg_function"] == "mean":
                    new_dataframe = application_df.groupby(op["args"]["group_columns"]).agg(pl.col(op["args"]["agg_column"]).mean())
                elif op["args"]["agg_function"] == "sum":
                    new_dataframe = application_df.groupby(op["args"]["group_columns"]).agg(pl.col(op["args"]["agg_column"]).sum())
                else:
                    print("Aggregation function invalid on groupby")

            elif operation == "mean":
                new_dataframe = application_df[op["column"]].mean()

            elif operation == "unique_rows":
                new_dataframe = application_df[op["column"]].value_counts()

            elif operation == "sum":
                new_dataframe = application_df[op["column"]].sum()
            
            elif operation == "join":
                join_dataframe = pd.DataFrame(op["dataframe"])
                join_dataframe[op["key"]] = join_dataframe[op["key"]].astype('int64')
                join_df = pl.from_arrow(pa.Table.from_pandas(join_dataframe))
                new_dataframe = application_df.join(join_df, left_on=op["key"], right_on=op["key"])     
            
            elif operation == "sort":
                new_dataframe = application_df.sort(op["column"])
            
            else:
                print("Invalid Operation")

            artifacts[op["label"]] = new_dataframe
        