import modin.config as modin_cfg
import unidist.config as unidist_cfg
import unidist

unidist.init()

modin_cfg.Engine.put('unidist') # Modin will use Unidist
unidist_cfg.Backend.put('mpi') # Unidist will use MPI backend

import modin.pandas as pd

class Modin_MPI:
    def load(self, file_path):
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
