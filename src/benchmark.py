import time, importlib, argparse
import numpy as np
import json


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Benchmark Python DataFrames.')
    parser.add_argument('--framework', required=True, help='Class of the dataframe to benchmark')
    parser.add_argument('--workflow', required=True, help='Workflow Descriptor')
   
    args = parser.parse_args()
    print(args)
    module = importlib.import_module(args.framework)
    klass = getattr(module, args.framework)


    with open(args.workflow + ".json", "r") as workflow_descriptor:
        workflow_data = json.load(workflow_descriptor)


    benchmarking_type = workflow_data["type"]  
    dataset = workflow_data["dataset"]
    operations = workflow_data["operations"]

    if "runs" in workflow_data:
        runs = workflow_data["runs"]
    else:
        runs = 4

    sut = klass()

    if benchmarking_type == "micro":
        operations_time={}
        filtered_operations_time={}
        methods={}
        for operation in operations:
            operations_time[operation]=[]
            filtered_operations_time[operation]=[]
            try:
                methods[operation]=getattr(klass,operation)
            except AttributeError as error:
                # Ignore. Not added to map and latter CSV with empty values
                pass

        print(sut.__class__)
        t0 = time.time()
        if (args.framework == "Dask"):
            sut.load(dataset,2)
        else:
            sut.load(dataset)
        load_time = time.time() - t0
        print("Load time: {:.8f}".format(load_time))

        t_start = time.time()
        
        for i  in range(runs):
            for operation in operations:
                if operation in methods:
                    t0 = time.time()
                    methods[operation](sut)
                    operations_time[operation].append(time.time()-t0)
        
        sut.filter()

        for i  in range(runs):
            for operation in operations:
                if operation in methods:
                    t0 = time.time()
                    methods[operation](sut)
                    filtered_operations_time[operation].append(time.time()-t0)

        ops_time=time.time()-t_start
        
        f = open("results/" + args.workflow + "_results.csv", "a")
        f.write(args.framework)
        f.write(",")

        f.write(str(1))
        f.write(",")

        f.write(str(load_time))

        for operation in operations:
            if operation in methods:
                f.write(",")
                f.write(str(operations_time[operation][0]))
                f.write(",")
                f.write(str(np.mean(operations_time[operation][1:])))
                f.write(",")
                f.write(str(np.std(operations_time[operation][1:])))
            else:
                for i in range(6):
                    f.write(",")
        f.write("\n")
        f.close()

    elif benchmarking_type == "macro":
        operations_time={}
        filtered_operations_time={}
        methods={}

        operation = "macro_workflow"

        operations_time[operation]=[]
        filtered_operations_time[operation]=[]
        try:
            methods[operation]=getattr(klass,operation)
        except AttributeError as error:
            # Ignore. Not added to map and latter CSV with empty values
            pass

        print(sut.__class__)
        t0 = time.time()
        if (args.framework == "Dask"):
            sut.load(dataset,2)
        else:
            sut.load(dataset)
        load_time = time.time() - t0
        print("Load time: {:.8f}".format(load_time))
        
        for i  in range(runs):
            t0 = time.time()
            sut.macro_workflow(operations)
            operations_time[operation].append(time.time()-t0)
        
        f = open("results/" + args.workflow + "_results.csv", "a")
        f.write(args.framework)
        f.write(",")
        f.write(str(1))
        f.write(",")
        f.write(str(load_time))
        f.write(",")
        f.write(str(operations_time[operation][0]))
        f.write(",")
        f.write(str(np.mean(operations_time[operation][1:])))
        f.write(",")
        f.write(str(np.std(operations_time[operation][1:])))
        f.write("\n")
        f.close()
    else:
        print("Benchmarking type invalid")