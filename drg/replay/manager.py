import os
from drg.ingest.generator import generate_and_save
from drg.policy.engine import fetch_one
from drg.utils import logger

def replay_run(run_id: str, fix_scenario: str = None):
    """
    Replays a pipeline run.
    Optionally regenerates data with a 'fix' (which might be just Clean data or a specific scenario).
    """
    logger.info(f"Replaying Run ID: {run_id}")
    
    # Check if run exists
    run = fetch_one("SELECT dataset_id FROM pipeline_runs WHERE run_id = %s", (run_id,))
    if not run:
        logger.error(f"Run {run_id} not found.")
        return False
        
    dataset_id = run['dataset_id']
    # If path isn't stored in DB, we reconstruct it or stored it in 'dataset_id' as path?
    # Design says 'dataset_id / dataset_path'. Let's assume dataset_id IS the path's identifier or we constructed path conventionally.
    # In CLI ingest, we output to `data/raw/...`.
    # Let's assume we can re-generate to the same path.
    
    output_path = "data/raw" # Hardcoded for V1
    
    if fix_scenario:
        logger.info(f"Applying fix scenario: {fix_scenario} (simulating data correction)")
        # In reality, 'fix' might just mean generating CLEAN data to replace bad data.
        # If fix_scenario is 'clean', we generate clean data.
        scenario = None if fix_scenario == 'clean' else fix_scenario
        
        # We need to overwrite the file. 
        # CAUTION: 'generate_and_save' uses current time unless overridden.
        # For replay to be valid "fix", it should validly pass checks.
        # If the original run failed due to 'late_data', new generation with 'now' will pass freshness.
        
        generate_and_save(output_path, run_id, scenario=scenario, seed=42) # Using fixed seed for repro-fix?
        
    logger.info(f"Data ready for re-validation. Use 'drg validate --run-id {run_id}' to complete replay.")
    return True
