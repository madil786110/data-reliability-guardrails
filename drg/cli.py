import argparse
import sys
import os
import uuid
import pandas as pd # For reading to pass to validation
from drg.ingest.generator import generate_and_save
from drg.contracts.loader import load_contract
from drg.validation.core import run_validations
from drg.policy.engine import enforce_policy, register_run, save_check_result, is_gate_open
from drg.replay.manager import replay_run
from drg.downstream.job import run_downstream_job
from drg.utils import logger

def setup_parser():
    parser = argparse.ArgumentParser(description="Data Reliability Guardrails (DRG) CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Ingest
    cmd_ingest = subparsers.add_parser("ingest", help="Generate/Ingest data")
    cmd_ingest.add_argument("--run-id", type=str, required=True, help="Unique run identifier")
    cmd_ingest.add_argument("--output", type=str, default="data/raw", help="Output directory")
    cmd_ingest.add_argument("--inject", type=str, help="Failure scenario to inject")
    cmd_ingest.add_argument("--seed", type=int, default=42, help="Random seed")
    
    # Validate
    cmd_validate = subparsers.add_parser("validate", help="Validate dataset against contract")
    cmd_validate.add_argument("--run-id", type=str, required=True, help="Unique run identifier")
    cmd_validate.add_argument("--contract", type=str, default="config/contract.yaml", help="Path to contract")
    
    # Status (simple gate check)
    cmd_status = subparsers.add_parser("status", help="Check gate status")
    
    # Downstream
    cmd_downstream = subparsers.add_parser("downstream", help="Run downstream job")
    sub_down = cmd_downstream.add_subparsers(dest="action", required=True)
    down_run = sub_down.add_parser("run", help="Execute job")
    down_run.add_argument("--run-id", type=str, required=True)
    
    # Replay
    cmd_replay = subparsers.add_parser("replay", help="Replay/Fix a run")
    cmd_replay.add_argument("--run-id", type=str, required=True)
    cmd_replay.add_argument("--fix", type=str, help="Scenario to apply (use 'clean' to fix failures)")

    # Init/Reference
    cmd_init = subparsers.add_parser("init", help="Initialize reference data")

    return parser

def main():
    parser = setup_parser()
    args = parser.parse_args()
    
    try:
        if args.command == "ingest":
            fpath = generate_and_save(args.output, args.run_id, args.inject, args.seed)
            # Register run in DB
            register_run(args.run_id, fpath)
            print(f"Ingested: {fpath}")
            
        elif args.command == "validate":
            # 1. Load Contract
            contract = load_contract(args.contract)
            
            # 2. Determine File Path (naive assumption: predictable name)
            # In real system, look up run_id in DB to get path? 
            # Or pass path as arg? Design doc said 'dataset_id / dataset_path'.
            # I'll rely on convention used in ingest: data/raw/rides_{run_id}.parquet
            fpath = f"data/raw/rides_{args.run_id}.parquet"
            if not os.path.exists(fpath):
                logger.error(f"Data file not found: {fpath}")
                sys.exit(1)
                
            # 3. Read Data
            logger.info(f"Reading {fpath}...")
            df = pd.read_parquet(fpath)
            
            # 4. Run Validations
            logger.info("Running validations...")
            results = run_validations(df, contract)
            
            # 5. Save Results & Enforce Policy
            for r in results:
                save_check_result(args.run_id, r.check_name, r.passed, r.metric, r.details)
                status = "PASS" if r.passed else "FAIL"
                logger.info(f"Check {r.check_name}: {status} (Val: {r.metric})")
                
            passed = enforce_policy(args.run_id, results)
            
            if passed:
                logger.info("Validation PASSED. Gate OPEN.")
                sys.exit(0)
            else:
                logger.error("Validation FAILED. Gate BLOCKED.")
                sys.exit(1)

        elif args.command == "status":
            open = is_gate_open()
            print("GATE IS " + ("OPEN" if open else "BLOCKED"))
            
        elif args.command == "downstream":
            if args.action == "run":
                run_downstream_job(args.run_id)

        elif args.command == "replay":
            success = replay_run(args.run_id, args.fix)
            if success:
                # Auto-trigger validation? User story says "rerun validation... and then unblocking".
                # It's better UX to just run it. But CLI args parsing for 'validate' is separate.
                # I'll instantiate arguments and call main logic or use subprocess?
                # Calling subprocess is safer for clean state.
                cmd = f"{sys.executable} -m drg.cli validate --run-id {args.run_id}"
                logger.info(f"Triggering validation: {cmd}")
                os.system(cmd)

        elif args.command == "init":
            # Generate reference data
            logger.info("Generating reference dataset...")
            # generate_and_save produces "data/reference/rides_reference.parquet"
            generate_and_save("data/reference", "reference", seed=123)
            logger.info("Reference data ready.")

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        # traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
