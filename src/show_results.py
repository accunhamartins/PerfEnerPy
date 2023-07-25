import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import argparse, json

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Graphics Generator.')
    parser.add_argument('--config', required=True, help='Workflow Descriptor')
    
    args = parser.parse_args()

    with open(args.config + ".json", "r") as workflow_descriptor:
        workflow_data = json.load(workflow_descriptor)

    benchmarking_type = workflow_data["type"]  
    operations = workflow_data["operations"]
    frameworks = workflow_data["frameworks"]
    framework_legend = workflow_data["framework_legend"]
    results = workflow_data["files"]
    datasets = workflow_data["datasets"]
    names = workflow_data["series"]

    # width of the bars
    barWidth = 0.9
    load = {}
    max = 5000000000

    # Dict {dataset -> { operation -> [times]}}
    operations_warmup_time = {}
    operations_mean_time = {}
    operations_var_time = {}
    filtered_operations_warmup_time = {}
    filtered_operations_mean_time = {}
    filtered_operations_var_time = {}
    macro_execution = {}

    energy = {}
    cpu = {}
    efficiency = {}

    if benchmarking_type == "micro":
        for result in results:
            print(result)
            load[result] =[]
            energy[result] = []
            efficiency[result] = []
            cpu[result] = []
            operations_warmup_time[result] = {}
            operations_mean_time[result] = {}
            operations_var_time[result] = {}
            filtered_operations_warmup_time[result] = {}
            filtered_operations_mean_time[result] = {}
            filtered_operations_var_time[result] = {}

            f = open("results/" + result + "_results.csv", "r")
            f_cols={}
            for line in f.readlines():
                cols = line.split(",")
                framework=cols[0] if int(cols[1])==1 else cols[0]+"_"+cols[1]
                f_cols[framework]=cols
            f.close()
            for operation in operations:
                    operations_warmup_time[result][operation] = []
                    operations_mean_time[result][operation] = []
                    operations_var_time[result][operation] = []

            for framework in frameworks:
                if framework in f_cols:
                    cols=f_cols[framework]
                    load[result].append(float(cols[2]))
                    index=3
                    for operation in operations:
                        if cols[index]:
                            operations_warmup_time[result][operation].append(float(cols[index]))
                            operations_mean_time[result][operation].append(float(cols[index+1]))
                            operations_var_time[result][operation].append(float(cols[index+2]))
                        else:
                            operations_warmup_time[result][operation].append(np.nan)
                            operations_mean_time[result][operation].append(np.nan)
                            operations_var_time[result][operation].append(np.nan)
                        index+=3

                else:
                    # Full row for this framework is missing
                    load[result].append(np.nan)
                    for operation in operations:
                        operations_warmup_time[result][operation].append(np.nan)
                        operations_mean_time[result][operation].append(np.nan)
                        operations_var_time[result][operation].append(np.nan)

        # Extract the keys and values from the dictionary
        keys = list(load.keys())
        values = list(load.values())

        #Set up the figure and axis
        fig, ax = plt.subplots()

        #Set the width of each bar
        bar_width = 0.35

        #Set the positions of the bars on the x-axis
        x = np.arange(len(values[0]))

        #Plot the bars
        for i in range(len(keys)):
            ax.bar(x + (i * bar_width), values[i], bar_width, label=names[datasets[i]], color=['blue', 'orange'][i], edgecolor='black', hatch=['//', '+'][i])

        #Set the labels and title
        ax.set_xlabel('Frameworks')
        ax.set_ylabel('Execution Time (s)')
        ax.set_title('Load Time')

        ax.set_xticks(x + ((len(keys) - 1) * bar_width) / 2)
        ax.set_xticklabels(framework_legend, fontsize=6)

        #Add the value of each bar at the top
        for i in range(len(keys)):
            for j, v in enumerate(values[i]):
                ax.text(x[j] + (i * bar_width), v, str(round(v, 2)), ha='center', fontsize=6)

        #Move the legend to the left side and make it smaller
        ax.legend(loc='upper left', fontsize='small')

        plt.savefig(args.config + '_load.png')

        for operation in operations:
            # Extract the keys and values from the dictionary
            keys = list(operations_mean_time.keys())
            values = list(operations_mean_time.values())

            # Set up the figure and axis
            fig, ax = plt.subplots()

            # Set the width of each bar
            bar_width = 0.35

            # Set the positions of the bars on the x-axis
            x = np.arange(len(values[0][operation]))

            # Plot the bars
            for i in range(len(keys)):
                v = values[i][operation]
                ax.bar(x + (i * bar_width), v, bar_width, label=names[datasets[i]], color=['blue', 'orange'][i], edgecolor='black', hatch=['//', '+'][i])

            # Set the labels and title
            ax.set_xlabel('Frameworks')
            ax.set_ylabel('Time(s)')
            ax.set_title(operation + ' Execution Time')

            ax.set_xticks(x + ((len(keys) - 1) * bar_width) / 2)
            ax.set_xticklabels(framework_legend, fontsize=6)

            # Add the value of each bar at the top
            for i in range(len(keys)):
                for j, v in enumerate(values[i][operation]):
                    ax.text(x[j] + (i * bar_width), v, str(round(v, 2)), ha='center', fontsize=6)

            # Move the legend to the left side and make it smaller
            ax.legend(loc='best', fontsize='small')

            plt.savefig(args.config + "_" + operation + '.png')

    elif benchmarking_type == "macro":
        for result in results:
            print(result)
            load[result] =[]
            energy[result] = []
            macro_execution[result] = {}

            f = open("results/" + result + "_results.csv", "r")
            f_cols={}

            for line in f.readlines():
                cols = line.split(",")
                framework=cols[0] if int(cols[1])==1 else cols[0]+"_"+cols[1]
                f_cols[framework]=cols
            f.close()
            
            for framework in frameworks:
                macro_execution[result][framework] = 0.0
                if framework in f_cols:
                    cols=f_cols[framework]
                    load[result].append(float(cols[2]))
                    index=3
                    if cols[index]:
                        macro_execution[result][framework] += float(cols[index + 1])
                    else:
                        macro_execution[result][framework] += 0.0
                else:
                    # Full row for this framework is missing
                    load[result].append(np.nan)

        keys = list(macro_execution.keys())
        frameworks = list(macro_execution[keys[0]].keys())
        values = np.array([[macro_execution[k][f] for f in frameworks] for k in keys])

        # Set up the figure and axis
        fig, ax = plt.subplots()

        # Set the width of each bar
        bar_width = 0.35

        # Set the positions of the bars on the x-axis
        x = np.arange(len(frameworks))

        # Plot the bars
        for i, val in enumerate(values):
            ax.bar(x + (i * bar_width), val, bar_width, label=names[datasets[i]], color=['blue', 'orange'][i], edgecolor='black', hatch=['//', '+'][i])

        # Set the labels and title
        ax.set_xlabel('Frameworks')
        ax.set_ylabel('Execution Time (s)')
        ax.set_title('Execution Time')

        ax.set_xticks(x + ((len(keys) - 1) * bar_width) / 2)
        ax.set_xticklabels(framework_legend, fontsize=6)

        # Add the value of each bar at the top
        for i, val in enumerate(values):
            for j, v in enumerate(val):
                ax.text(x[j] + (i * bar_width), v + 0.1, str(round(v, 2)), ha='center', fontsize=6)

        #Move the legend to the left side and make it smaller
        ax.legend(loc='best', fontsize='small')

        plt.savefig(args.config + '_execution.png')
