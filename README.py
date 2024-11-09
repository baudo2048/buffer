import subprocess
import pandas as pd
import json
import time

# Function to run YCSB command and capture output
def run_ycsb(database, workload, operation_type):
    command = [
        "ycsb", operation_type, database,
        "-P", f"workloads/{workload}",
        "-p", "recordcount=100000",   # Define number of records for the test
        "-p", "operationcount=10000", # Define number of operations to perform
    ]
    
    # Run the YCSB command and capture the output
    result = subprocess.run(command, stdout=subprocess.PIPE, text=True)
    return result.stdout

# Function to parse YCSB output into metrics
def parse_ycsb_output(ycsb_output):
    metrics = {}
    for line in ycsb_output.splitlines():
        if line.startswith("[READ]") or line.startswith("[WRITE]"):
            operation, metric = line.split(",")[0], line.split(",")[1:]
            for m in metric:
                key, value = m.strip().split("=")
                metrics[f"{operation}_{key}"] = float(value)
    return metrics

# Initialize results storage
results = []

# Databases to compare
databases = ["mongodb", "jdbc"]  # Assuming "jdbc" is used for MySQL in YCSB

# Workloads to run
workloads = ["workloada", "workloadb", "workloadc"]

# Run YCSB tests and collect results
for db in databases:
    for workload in workloads:
        # Load data into the database
        print(f"Loading data for {db} with {workload}...")
        load_output = run_ycsb(db, workload, "load")
        load_metrics = parse_ycsb_output(load_output)
        
        # Run the workload
        print(f"Running workload {workload} for {db}...")
        run_output = run_ycsb(db, workload, "run")
        run_metrics = parse_ycsb_output(run_output)
        
        # Record results
        results.append({
            "Database": db,
            "Workload": workload,
            "LoadMetrics": load_metrics,
            "RunMetrics": run_metrics
        })

# Convert results into DataFrame for analysis
df_results = pd.json_normalize(results)
df_results.head()



# ----------------------------------------



# Extract and analyze specific metrics
throughput_df = df_results[["Database", "Workload", "RunMetrics.[READ]_Throughput(ops/sec)", "RunMetrics.[WRITE]_Throughput(ops/sec)"]]
latency_df = df_results[["Database", "Workload", "RunMetrics.[READ]_AverageLatency(us)", "RunMetrics.[WRITE]_AverageLatency(us)"]]

# Display throughput comparison
print("Throughput Comparison:")
print(throughput_df)

# Display latency comparison
print("\nLatency Comparison:")
print(latency_df)

# Visualization (optional)
import matplotlib.pyplot as plt
import seaborn as sns

# Throughput Comparison Plot
sns.barplot(data=throughput_df, x="Workload", y="RunMetrics.[READ]_Throughput(ops/sec)", hue="Database")
plt.title("Read Throughput Comparison")
plt.show()

sns.barplot(data=throughput_df, x="Workload", y="RunMetrics.[WRITE]_Throughput(ops/sec)", hue="Database")
plt.title("Write Throughput Comparison")
plt.show()

# Latency Comparison Plot
sns.barplot(data=latency_df, x="Workload", y="RunMetrics.[READ]_AverageLatency(us)", hue="Database")
plt.title("Read Latency Comparison")
plt.show()

sns.barplot(data=latency_df, x="Workload", y="RunMetrics.[WRITE]_AverageLatency(us)", hue="Database")
plt.title("Write Latency Comparison")
plt.show()
