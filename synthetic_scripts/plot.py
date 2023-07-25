import pandas as pd
import numpy as np
import glob 
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt


BASE_DIR = '../synthetic_output/scaling_test_100_4/'
frameworks = ['Pandas', 'Polars', 'Modin_Ray', 'Modin_Dask']
sizes = ['1000', '10000', '100000', '1000000', '5000000', '10000000', '20000000', '30000000']

all_perfs = []

for framework in frameworks:
    for size in sizes:
        input_dir = f"{BASE_DIR}/{framework}_{size}/"
        try:
            perf_file = glob.glob(f"{input_dir}/*_perf.csv")[0]
            perf_df = pd.read_csv(perf_file, index_col=0)
            perf_df['end_time_seconds'] = np.cumsum(perf_df.elapsed_time)
            perf_df['start_time_seconds'] = perf_df['end_time_seconds'].shift().fillna(0)
            perf_df['framework'] = framework
            perf_df['size'] = size
            all_perfs.append(perf_df)
        except (IndexError, FileNotFoundError) as e:
            #raise(e)
            pass
        
all_perfs_df = pd.concat(all_perfs, ignore_index=True)
total_wf_times = all_perfs_df.loc[all_perfs_df.dst == 'artifact_9' ][['framework','size','end_time_seconds']].reset_index(drop=True).pivot(index='size', columns='framework', values='end_time_seconds')
total_wf_times = total_wf_times.rename_axis('Client')
total_wf_times = total_wf_times[['Pandas', 'Polars', 'Modin_Ray', 'Modin_Dask']]
total_wf_times= total_wf_times.sort_values('Polars')


print(total_wf_times)


font = {'family' : 'serif',
        'weight' : 'normal',
        'size'   : 12}

matplotlib.rc('font', **font)


x_axis_replacements = ['1K', '10k', '100k', '1M', '5M', '10M', '20M', '30M']


plt.figure(figsize=(6,4))

ax = sns.lineplot(data=total_wf_times, markers=True, linewidth=2.5, markersize=10)
plt.xticks(total_wf_times.index, x_axis_replacements)
plt.grid()
plt.xlabel('Número de Linhas do Artefacto (r)')
plt.ylabel('Tempo de execução (s)')

handles, labels = ax.get_legend_handles_labels()
ax.legend(handles=handles, labels=labels)

plt.savefig("scaling_100_4.png", bbox_inches='tight')


breakdown = all_perfs_df[['framework', 'size', 'op_list', 'elapsed_time']].groupby(['framework', 'size', 'op_list']).sum().reset_index()
pivoted_breakdown = breakdown.pivot(index=['size', 'framework'], columns=['op_list'], values='elapsed_time')





