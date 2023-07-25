import pandas as pd
import argparse

dfs=[]

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Download NYC TLC Dataset.')
	parser.add_argument('--start_year', required=True, help='Start year to download')
	parser.add_argument('--end_year', required=True, help='End year to download')
	parser.add_argument('--months', required=True, help='Number of month to download')

	args = parser.parse_args()
	start_year = int(args.start_year)
	end_year = int(args.end_year) + 1

	def save(year,month):
		df=pd.concat(dfs, ignore_index=True)
		filename='yellow_tripdata_'+str(start_year)+'_01-'+str(year)+'_'+str(month)
		df.to_parquet(filename+'.parquet',row_group_size=1000000,engine="pyarrow")
		df.to_csv(filename+'.csv',chunksize=1000000)

	for year in range(start_year, end_year):
		for month in range(1, int(args.months) + 1):
			dfs.append(pd.read_parquet("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"+format(year, '04d')+"-"+format(month, '02d')+".parquet"))
		save(year,month)



