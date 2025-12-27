import os
import uuid
import time
import random
import pandas as pd
import matplotlib.pyplot as plt
from drg.ingest.generator import generate_and_save
from drg.contracts.loader import load_contract
from drg.validation.core import run_validations
from drg.utils import logger

# Ensure dirs exist
os.makedirs("benchmarks/results", exist_ok=True)

SCENARIOS = ['clean'] * 80 + ['schema_drift', 'late_data', 'volume_spike', 'null_explosion', 'missing_partition'] * 4
random.shuffle(SCENARIOS)

def run_benchmarks(num_runs=100):
    logger.info(f"Starting benchmark of {num_runs} runs...")
    
    results = []
    contract = load_contract("config/contract.yaml")
    
    start_total = time.time()
    
    for i in range(num_runs):
        run_id = str(uuid.uuid4())
        scenario = random.choice(SCENARIOS) if i % 5 == 0 else 'clean' # Override for controlled mix if needed, or use SCENARIOS list
        
        # Ingest
        t0 = time.time()
        fpath = generate_and_save("data/raw", run_id, scenario=scenario if scenario != 'clean' else None, seed=i)
        
        # Validate
        t1 = time.time()
        try:
            df = pd.read_parquet(fpath)
            val_results = run_validations(df, contract)
            passed = all(r.passed for r in val_results)
        except Exception:
            passed = False # Crash counts as fail
        
        t2 = time.time()
        
        # Detection logic
        expected_pass = (scenario == 'clean')
        false_positive = (passed == False and expected_pass == True)
        false_negative = (passed == True and expected_pass == False)
        
        results.append({
            "run_id": run_id,
            "scenario": scenario,
            "passed": passed,
            "ingest_time": t1-t0,
            "validate_time": t2-t1,
            "false_positive": false_positive,
            "false_negative": false_negative
        })
        
        if (i+1) % 10 == 0:
            print(f"Completed {i+1}/{num_runs} runs...")

    total_time = time.time() - start_total
    df_res = pd.DataFrame(results)
    
    # Analysis
    fn_rate = df_res['false_negative'].mean()
    fp_rate = df_res['false_positive'].mean()
    avg_latency = df_res['validate_time'].mean()
    detection_rate = 1.0 - fn_rate
    
    logger.info(f"Benchmark Complete in {total_time:.2f}s")
    logger.info(f"Detection Rate: {detection_rate:.2%}")
    logger.info(f"False Positive Rate: {fp_rate:.2%}")
    logger.info(f"Avg Validation Latency: {avg_latency*1000:.2f}ms")
    
    # Save Report
    report = f"""
# Benchmark Report

**Runs**: {num_runs}
**Total Time**: {total_time:.2f}s

## Metrics
- **Detection Rate**: {detection_rate:.2%}
- **False Positive Rate**: {fp_rate:.2%}
- **Avg Validation Latency**: {avg_latency*1000:.2f}ms

## Failure Breakdown
{df_res.groupby('scenario')['passed'].value_counts().to_markdown()}

"""
    with open("BENCHMARK_REPORT.md", "w") as f:
        f.write(report)
        
    df_res.to_csv("benchmarks/results/runs.csv", index=False)
    
    # Plot
    try:
        plt.figure(figsize=(10, 6))
        df_res['validate_time'].hist(bins=20)
        plt.title("Validation Latency Distribution")
        plt.xlabel("Seconds")
        plt.savefig("benchmarks/results/latency.png")
    except Exception as e:
        logger.warning(f"Could not save plot: {e}")

if __name__ == "__main__":
    run_benchmarks(100)
