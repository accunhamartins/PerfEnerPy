import duckdb

class DuckDB:
    def load(self, file_path):
        self.con = duckdb.connect(database='pdfpeeb.duckdb', read_only=False)
        self.con.execute("DROP VIEW IF EXISTS trips")
        self.con.execute("DROP VIEW IF EXISTS filtered_trips")
        self.con.execute("CREATE VIEW trips AS SELECT * FROM parquet_scan('"+file_path+".parquet')")
        self.view_name="trips"
        self.con.execute("PRAGMA memory_limit='16GB'")

    def filter(self):
        self.con.execute("CREATE VIEW filtered_trips AS SELECT * FROM trips where tip_amount >= 1 and tip_amount <= 5")
        self.view_name="filtered_trips"
        pass

    def mean(self):
        self.con.execute("SELECT avg(passenger_count) FROM "+self.view_name)
        return self.con.fetchall()

    def sum(self):
        self.con.execute("SELECT fare_amount+extra+mta_tax+tip_amount+tolls_amount+improvement_surcharge FROM "+self.view_name)
        return self.con.fetchall()
    
    def unique_rows(self):
        self.con.execute("SELECT count(*) FROM "+self.view_name+" GROUP BY VendorID")
        return self.con.fetchall()

    def groupby(self):
        self.con.execute("SELECT avg(tip_amount) FROM "+self.view_name+" GROUP BY passenger_count")
        return self.con.fetchall()

    def multiple_groupby(self):
        self.con.execute("SELECT avg(tip_amount) FROM "+self.view_name+" GROUP BY passenger_count,payment_type")
        return self.con.fetchall()

    def join(self):
        self.con.execute("DROP TABLE IF EXISTS payments")
        self.con.execute("CREATE TABLE payments(payment_type UTINYINT, payment_name VARCHAR)")
        self.con.execute("INSERT INTO payments VALUES (1,'Credit Card'), (2,'Cash'), (3,'No Charge'), (4,'Dispute'), (5,'Unknown'), (6,'Voided trip')")
        return self.con.execute("select * from " + self.view_name + " JOIN payments USING (payment_type)")
    
    def sort(self):
        return self.con.execute("SELECT * FROM " + self.view_name + " ORDER BY tip_amount ASC")

