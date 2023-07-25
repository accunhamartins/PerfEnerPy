import vaex

class Vaex:   
    def load(self, file_path):
        self.df = vaex.open(file_path+".parquet")
        vaex.cache.memory()

    def filter(self):
        self.df = self.df[self.df.tip_amount >= 1 and self.df.tip_amount <= 5]

    def sum(self):
        return self.df.fare_amount+self.df.extra+self.df.mta_tax+self.df.tip_amount+self.df.tolls_amount+self.df.improvement_surcharge

    def mean(self):
        return self.df.passenger_count.mean()

    def unique_rows(self):
        return self.df.groupby(by="VendorID",agg={'count_VendorID': vaex.agg.count()})

    def groupby(self):
        return self.df.groupby(by="passenger_count",agg={'mean_tip_amount': vaex.agg.mean('tip_amount')})

    def multiple_groupby(self):
        return self.df.groupby(by=["passenger_count",],agg={'mean_tip_amount': vaex.agg.mean('tip_amount')})

    def join(self):
        payments =  vaex.from_arrays(payments=['Credit Card', 'Cash', 'No Charge', 'Dispute', 'Unknown', 'Voided trip'],payment_type=[1,2,3,4,5,6])
        return self.df.join(payments, left_on='payment_type', right_on='payment_type')

    def sort(self):
        return self.df.sort('total_amount')
    
    def macro_workflow(self,operations):
        artifacts = {}
        artifacts["source"] = self.df
        for op in operations:
            operation = op["op"]
            artifacts[op["label"]] = None
            application_df = artifacts[op["artifact"]]
            if operation == "groupby":
                if op["args"]["agg_function"] == "mean":
                    new_dataframe = application_df.groupby(by= op["args"]["group_columns"],agg={"mean" + op["args"]["agg_column"]: vaex.agg.mean(op["args"]["agg_column"])})
                
                elif op["args"]["agg_function"] == "sum":
                    new_dataframe = application_df.groupby(by= op["args"]["group_columns"],agg={"sum" + op["args"]["agg_column"]: vaex.agg.sum(op["args"]["agg_column"])})

                else:
                    print("Aggregation function invalid on groupby")

            elif operation == "mean":
                new_dataframe = application_df[op["column"]].mean()

            elif operation == "unique_rows":
                new_dataframe = application_df.groupby(by= op["column"],agg={"count" + op["column"]: vaex.agg.count()})

            elif operation == "sum":
                new_dataframe = application_df[op["column"]].sum()
            
            elif operation == "join":
                join_dataframe = vaex.from_arrays(payments=['Credit Card', 'Cash', 'No Charge', 'Dispute', 'Unknown', 'Voided trip'],payment_type=[1,2,3,4,5,6])
                new_dataframe = application_df.merge(join_dataframe, left_on=op["key"], right_on=op["key"])     

            elif operation == "sort":
                new_dataframe = application_df.sort()

            else:
                print("Invalid Operation")

            artifacts[op["label"]] = new_dataframe
    