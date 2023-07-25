import re
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Benchmark Python DataFrames.')
    parser.add_argument('--framework', required=True, help='Class of the dataframe to benchmark')
    parser.add_argument('--workflow', required=True, help='Workflow Descriptor')
   
    args = parser.parse_args()

    f = open("energy/" + args.workflow + "/energy_" + args.framework + ".txt")
    text = f.read()

    pattern = r"Total energy: ([\d.]+) Joules"
    match = re.search(pattern, text)

    if match:
        total_energy = float(match.group(1))
    
    f = open("results/" + args.workflow + "_energy.csv", "a")
    f.write(args.framework)
    f.write(",")
    f.write(str(total_energy))
    f.write("\n")
    f.close()