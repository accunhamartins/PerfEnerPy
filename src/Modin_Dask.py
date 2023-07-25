import modin.pandas as pd
import os
import dask.distributed
from distributed import Client

class Modin_Dask:
    def load(self, file_path):
        os.environ["MODIN_ENGINE"] = "dask"
        client = Client()
        self.df = pd.read_parquet(file_path+".parquet")

    def filter(self):
        self.df = self.df[(self.df.tip_amount >= 1) & (self.df.tip_amount <= 5)]

    def mean(self):
        return self.df.passenger_count.mean()

    def sum(self):
        return self.df.fare_amount+self.df.extra+self.df.mta_tax+self.df.tip_amount+self.df.tolls_amount+self.df.improvement_surcharge
    
    def unique_rows(self):
        return self.df.VendorID.value_counts()

    def groupby(self):
        return self.df.groupby("passenger_count").tip_amount.mean()

    def multiple_groupby(self):
        return self.df.groupby(["passenger_count", "payment_type"]).tip_amount.mean()

    def join(self):
        payments =  pd.DataFrame({'payments': ['Credit Card', 'Cash', 'No Charge', 'Dispute', 'Unknown', 'Voided trip'],'payment_type':[1,2,3,4,5,6]})
        return self.df.merge(payments, left_on='payment_type', right_on='payment_type',right_index=True)
    
    def sort(self):
        return self.df.sort_values(by=["tip_amount"])
        
    def macro_workflow(self,operations):
        artifacts = {}
        artifacts["source"] = self.df
        for op in operations:
            operation = op["op"]
            artifacts[op["label"]] = None
            application_df = artifacts[op["artifact"]]
            
            if operation == "groupby":
                if op["args"]["agg_function"] == "mean":
                    new_dataframe = application_df.groupby(op["args"]["group_columns"]).agg({op["args"]["agg_column"] : "mean"})
                
                elif op["args"]["agg_function"] == "sum":
                    new_dataframe = application_df.groupby(op["args"]["group_columns"]).agg({op["args"]["agg_column"] : "sum"})
               
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
                new_dataframe = application_df.merge(join_dataframe, left_on=op["key"], right_on=op["key"], right_index=True)

            elif operation == "sort":
                new_dataframe = application_df.sort_values(by=op["column"])
            
            else:
                print("Invalid Operation")

            artifacts[op["label"]] = new_dataframe
