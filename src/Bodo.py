import bodo
import pandas as pd

class Bodo:
    def load(self, file_path):
        self.df=bodo_load(file_path+".parquet")
    
    def filter(self):
        return bodo_filter(self.df)
    
    def mean(self):
        return bodo_mean(self.df)
    
    def sum(self):
        return bodo_sum(self.df)
    
    def unique_rows(self):
        return bodo_unique_rows(self.df)

    def groupby(self):
        return bodo_groupby(self.df)

    def multiple_groupby(self):
        return bodo_multiple_groupby(self.df)

    def join(self):
        return bodo_join(self.df)
    
    def macro_workflow(self, operations)
        return bodo_workflow(self, operations)

@bodo.jit
def bodo_load(file_path):
	return pd.read_parquet(file_path)

@bodo.jit
def bodo_filter(df):
    return df[(df.tip_amount >= 1) & (df.tip_amount <= 5)]

@bodo.jit 
def bodo_mean(df):
    return df.passenger_count.mean()

@bodo.jit
def bodo_sum(df):
        return df.fare_amount+df.extra+df.mta_tax+df.tip_amount+df.tolls_amount+df.improvement_surcharge

@bodo.jit 
def bodo_unique_rows(df):
    return df.VendorID.value_counts()

@bodo.jit
def bodo_groupby(df):
    return df.groupby("passenger_count").tip_amount.mean()

@bodo.jit
def bodo_multiple_groupby(df):
    return df.groupby(["passenger_count", "payment_type"]).tip_amount.mean()

@bodo.jit
def bodo_join(df):
    payments =  pd.DataFrame({'payments': ['Credit Card', 'Cash', 'No Charge', 'Dispute', 'Unknown', 'Voided trip'],'payment_type':[1,2,3,4,5,6]})
    return df.merge(payments, left_on='payment_type', right_on='payment_type', right_index=True)

@bodo.jit
def bodo_workflow(df, operations):
    artifacts = {}
    artifacts["source"] = df
    for op in operations:
        operation = op["op"]
        artifacts[op["label"]] = None
        application_df = artifacts[op["artifact"]]

        if operation == "groupby":
            if op["args"]["agg_function"] == "mean":
                new_dataframe = application_df.groupby(op["args"]["group_columns"]).agg(op["args"]["agg_column"]).mean()
                
            elif op["args"]["agg_function"] == "sum":
                new_dataframe = application_df.groupby(op["args"]["group_columns"]).agg(op["args"]["agg_column"]).sum()

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

        else:
            print("Invalid Operation")

        artifacts[op["label"]] = new_dataframe