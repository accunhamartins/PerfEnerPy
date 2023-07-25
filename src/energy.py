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
    energy = {}

    if benchmarking_type == "micro":
        for result in results:
            energy[result] =[]
            f = open("results/" + result + "_energy.csv", "r")
            f_cols={}
            for line in f.readlines():
                cols = line.split(",")
                framework=cols[0]
                f_cols[framework]=cols
            f.close()
            for framework in frameworks:
                if framework in f_cols:
                    cols=f_cols[framework]
                    energy[result].append(float(cols[1]))

        # Extract the keys and values from the dictionary
        keys = list(energy.keys())
        values = list(energy.values())

        # Set up the figure and axis
        fig, ax = plt.subplots()

        # Set the width of each bar
        bar_width = 0.35

        # Set the positions of the bars on the x-axis
        x = np.arange(len(values[0]))

        # Plot the bars
        for i in range(len(keys)):
            v = values[i]
            ax.bar(x + (i * bar_width), v, bar_width, label=names[datasets[i]], color=['blue', 'orange'][i], edgecolor='black', hatch=['//', '+'][i])

        # Set the labels and title
        ax.set_xlabel('Frameworks')
        ax.set_ylabel('Energy consumption (J)')
        ax.set_title('Energy consumption')

        ax.set_xticks(x + ((len(keys) - 1) * bar_width) / 2)
        ax.set_xticklabels(framework_legend, fontsize=6)

            # Add the value of each bar at the top
        for i in range(len(keys)):
            for j, v in enumerate(values[i]):
                ax.text(x[j] + (i * bar_width), v + 0.3, str(round(v, 2)), ha='center', fontsize=6)

        # Move the legend to the left side and make it smaller
        ax.legend(loc='best', fontsize='small')

        plt.savefig(args.config + '_micro_Energy.png')


    elif benchmarking_type == "macro":
        for result in results:
            with open(result + ".json", "r") as workflow_descriptor:
                workflow_data = json.load(workflow_descriptor)

            total = len(workflow_data["operations"])
            
            energy[result] =[]
            f = open("results/" + result+ "_energy.csv", "r")
            f_cols={}
            for line in f.readlines():
                cols = line.split(",")
                framework=cols[0]
                f_cols[framework]=cols
            f.close()
            for framework in frameworks:
                if framework in f_cols:
                    cols=f_cols[framework]
                    energy[result].append(float(float(cols[1]) / total))

        # Extract the keys and values from the dictionary
        keys = list(energy.keys())
        values = list(energy.values())

        # Set up the figure and axis
        fig, ax = plt.subplots()

        # Set the width of each bar
        bar_width = 0.35

        # Set the positions of the bars on the x-axis
        x = np.arange(len(values[0]))

        # Plot the bars
        for i in range(len(keys)):
            v = values[i]
            ax.bar(x + (i * bar_width), v, bar_width, label=names[datasets[i]], color=['blue', 'orange'][i], edgecolor='black', hatch=['//', '+'][i])

        # Set the labels and title
        ax.set_xlabel('Frameworks')
        ax.set_ylabel('Energy consumption  per Operation (J/Operation)')
        ax.set_title('Energy consumption')

        ax.set_xticks(x + ((len(keys) - 1) * bar_width) / 2)
        ax.set_xticklabels(framework_legend, fontsize=6)

        # Add the value of each bar at the top
        for i in range(len(keys)):
            for j, v in enumerate(values[i]):
                ax.text(x[j] + (i * bar_width), v + 0.3, str(round(v, 2)), ha='center', fontsize=6)

        # Move the legend to the left side and make it smaller
        ax.legend(loc='best', fontsize='small')

        plt.savefig(args.config + '_macro_Energy.png')











































