from datatable import dt, f, by, join, rowsum


class Datatable:
    def load(self, file_path):
        self.df = dt.fread(file_path+".csv")
    
    def filter(self):
        self.df=self.df[(f.tip_amount>=1) & (f.tip_amount<=5), :]
        
    def mean(self):
        return self.df[:, dt.mean(dt.f.passenger_count)]
    
    def sum(self):
        return self.df[:, rowsum(f.fare_amount,f.extra,f.mta_tax,f.tip_amount,f.tolls_amount,f.improvement_surcharge)]
        
    def unique_rows(self):
        return self.df[:, dt.count(), by("VendorID")]

    def groupby(self):
        return self.df[:, dt.mean(f.tip_amount), by("passenger_count")]    

    def multiple_groupby(self):
        return self.df[:, dt.mean(f.tip_amount), by("passenger_count","payment_type")]    

    def join(self):
        payments =  dt.Frame({'payments': ['Credit Card', 'Cash', 'No Charge', 'Dispute', 'Unknown', 'Voided trip'],'payment_type':[1,2,3,4,5,6]})
        payments.key="payment_type"
        return self.df[:,:, join(payments)]
        
    def sort(self):
        return self.df.sort("tip_amount")

    def macro_workflow(self,operations):
        artifacts = {}
        artifacts["source"] = self.df
        for op in operations:
            operation = op["op"]
            artifacts[op["label"]] = None
            application_df = artifacts[op["artifact"]]
            if operation == "groupby":
                if op["args"]["agg_function"] == "mean":
                    new_dataframe = application_df[:, dt.mean(f[op["args"]["agg_column"]]), by(op["args"]["group_columns"])]
                
                elif op["args"]["agg_function"] == "sum":
                    new_dataframe = application_df[:, dt.sum(f[op["args"]["agg_column"]]), by(op["args"]["group_columns"])]

                else:
                    print("Aggregation function invalid on groupby")

            elif operation == "mean":
                new_dataframe = application_df[:, dt.mean(dt.f[op["column"]])]

            elif operation == "unique_rows":
                new_dataframe = application_df[:, dt.count(), by(op["column"])]

            elif operation == "sum":
                new_dataframe = application_df[:, dt.sum(dt.f[op["column"]])]
            
            elif operation == "join":
                join_dataframe = dt.Frame(op["dataframe"])
                join_dataframe.key = op["key"]
                new_dataframe = application_df[:, :, join(join_dataframe)]   

            elif operation == "sort":
                new_dataframe = application_df.sort()

            else:
                print("Invalid Operation")

            artifacts[op["label"]] = new_dataframe

    
 