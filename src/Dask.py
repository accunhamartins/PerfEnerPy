from numpy import right_shift
import dask.dataframe as dd
import pandas as pd
import distributed
import dask.config
from dask.distributed import LocalCluster, Client
import ctypes

class Dask:
    def load(self, file_path,n_workers):
        cluster = LocalCluster(n_workers=n_workers)
        self.client = Client(cluster)
        self.df = dd.read_parquet(file_path+".parquet")
        self.df.persist()
        
    def filter(self):
        self.df = self.df[(self.df.tip_amount >= 1) & (self.df.tip_amount <= 5)]

    def mean(self):
        return self.df.passenger_count.mean().compute()
        
    def sum(self):
        return (self.df.fare_amount+self.df.extra+self.df.mta_tax+self.df.tip_amount+self.df.tolls_amount+self.df.improvement_surcharge).compute()
    
    def unique_rows(self):
        return self.df.VendorID.value_counts().compute()
    
    def groupby(self):
        return self.df.groupby("passenger_count").tip_amount.mean().compute()

    def multiple_groupby(self):
        return self.df.groupby(["passenger_count", "payment_type"]).tip_amount.mean().compute()

    def join(self):
        payments =  dd.from_pandas(pd.DataFrame({'payment_name': ['Credit Card', 'Cash', 'No Charge', 'Dispute', 'Unknown', 'Voided trip'],'payment_type':[1,2,3,4,5,6]}),npartitions=1)
        return self.df.merge(payments, left_on='payment_type',right_on='payment_type',right_index=True).compute()

    def sort(self):
        return self.df.set_index("tip_amount").compute()
    
    def macro_workflow(self,operations):
        artifacts = {}
        artifacts["source"] = self.df
        for op in operations:
            operation = op["op"]
            artifacts[op["label"]] = None
            application_df = artifacts[op["artifact"]]
            if operation == "groupby":
                if op["args"]["agg_function"] == "mean":
                    new_dataframe = application_df.groupby(op["args"]["group_columns"]).agg({op["args"]["agg_column"]: "mean"}).compute()
                
                elif op["args"]["agg_function"] == "sum":
                    new_dataframe = application_df.groupby(op["args"]["group_columns"]).agg({op["args"]["agg_column"]: "sum"}).compute()

                else:
                    print("Aggregation function invalid on groupby")

            elif operation == "mean":
                new_dataframe = application_df[op["column"]].mean().compute()

            elif operation == "unique_rows":
                new_dataframe = application_df[op["column"]].value_counts().compute()

            elif operation == "sum":
                new_dataframe = application_df[op["column"]].sum().compute()
            
            elif operation == "join":
                join_dataframe = dd.from_pandas(pd.DataFrame(op["dataframe"]))
                new_dataframe = application_df.merge(join_dataframe, left_on=op["key"], right_on=op["key"], right_index=True).compute()

            elif operation == "sort":
                new_dataframe = application_df.set_index().compute()

            else:
                print("Invalid Operation")

            artifacts[op["label"]] = new_dataframe
    

    
