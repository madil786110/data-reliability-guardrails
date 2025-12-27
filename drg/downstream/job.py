import sys
import time
from drg.policy.engine import is_gate_open
from drg.utils import logger

def run_downstream_job(run_id: str):
    logger.info(f"Attempting to start downstream job for run {run_id}...")
    
    if not is_gate_open():
        logger.error("GATE IS BLOCKED. Downstream execution aborted.")
        sys.exit(1)
        
    logger.info("Gate is OPEN. Starting compute...")
    # Simulate work
    time.sleep(1)
    logger.info("Computing daily aggregates...")
    logger.info("Update dashboard tables...")
    logger.info("Downstream job COMPLETED successfully.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m drg.downstream <run_id>")
        sys.exit(1)
    run_downstream_job(sys.argv[1])
