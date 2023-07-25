import ibis
import pandas as pd

class Ibis:
    def load(self, file_path):  
        df = pd.read_parquet(file_path+".parquet")
        self.table = ibis.pandas.connect({'t': df}).table('t')

    def filter(self):
        self.table = self.table[(self.table.tip_amount >= 1) & (self.table.tip_amount <= 5)]
    
    def mean(self):
        result = self.table.passenger_count.mean()
        return result.execute()

    def sum(self):
        result = self.table.fare_amount+self.table.extra+self.table.mta_tax+self.table.tip_amount+self.table.tolls_amount+self.table.improvement_surcharge 
        return result.execute() 

    def unique_rows(self):
        result = self.table.VendorID.value_counts()
        return result.execute()

    def groupby(self):
        result = self.table.groupby("passenger_count").aggregate(avg_tip_amount = self.table.tip_amount.mean()) 
        return result.execute()

    def multiple_groupby(self):
        result = self.table.groupby(["passenger_count", "payment_type"]).aggregate(avg_tip_amount = self.table.tip_amount.mean())
        return result.execute()

    def join(self):
        payments =  pd.DataFrame({'payments': ['Credit Card', 'Cash', 'No Charge', 'Dispute', 'Unknown', 'Voided trip'],'payment_type':[1,2,3,4,5,6]})
        pd_ibis = ibis.pandas.connect({'payments': payments}).table('payments')
        join_expr = self.table.payment_type == pd_ibis.payment_type
        return self.table.join(pd_ibis, join_expr).execute()

    def sort(self):
        return self.table.sort_by('tip_amount').execute()

    def macro_workflow(self,operations):
        artifacts = {}
        artifacts["source"] = self.table
        for op in operations:
            operation = op["op"]
            artifacts[op["label"]] = None
            application_df = artifacts[op["artifact"]]
            if operation == "groupby":
                if op["args"]["agg_function"] == "mean":
                    new_dataframe = application_df.groupby(op["args"]["group_columns"]).aggregate(avg_macro = application_df[op["args"]["agg_column"]].mean()).execute()
                
                elif op["args"]["agg_function"] == "sum":
                    new_dataframe = application_df.groupby(op["args"]["group_columns"]).aggregate(avg_macro = application_df[op["args"]["agg_column"]].sum()).execute()

                else:
                    print("Aggregation function invalid on groupby")

            elif operation == "mean":
                new_dataframe = application_df[op["column"]].mean().execute()

            elif operation == "unique_rows":
                new_dataframe = application_df[op["column"]].value_counts().execute()

            elif operation == "sum":
                new_dataframe = application_df[op["column"]].sum().execute()
            
            elif operation == "join":
                join_dataframe = pd.DataFrame(op["dataframe"])
                new_dataframe = application_df.merge(join_dataframe, left_on=op["key"], right_on=op["key"], right_index=True)     

            elif operation == "sort":
                new_dataframe = application_df.sort_by(op["column"]).execute()

            else:
                print("Invalid Operation")

            artifacts[op["label"]] = new_dataframe