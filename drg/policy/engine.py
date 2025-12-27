import json
from datetime import datetime
from typing import Any
from drg.db import execute_query, fetch_one
from drg.utils import logger

def register_run(run_id: str, dataset_id: str):
    sql = """
    INSERT INTO pipeline_runs (run_id, dataset_id, status)
    VALUES (%s, %s, NULL)
    ON CONFLICT (run_id) DO NOTHING
    """
    execute_query(sql, (run_id, dataset_id))

def save_check_result(run_id: str, check_name: str, passed: bool, metric: Any, details: dict):
    sql = """
    INSERT INTO check_results (run_id, check_name, passed, metric_value, details)
    VALUES (%s, %s, %s, %s, %s)
    """
    execute_query(sql, (run_id, check_name, bool(passed), str(metric), json.dumps(details)))

def enforce_policy(run_id: str, results: list) -> bool:
    """
    Applies Fail-Stop policy.
    Returns True if overall PASS, False if BLOCK.
    """
    failed_checks = [r for r in results if not r.passed]
    is_success = len(failed_checks) == 0
    
    # Update Run Status
    status = 'PASSED' if is_success else 'FAILED'
    sql_run = "UPDATE pipeline_runs SET status = %s, completed_at = NOW() WHERE run_id = %s"
    execute_query(sql_run, (status, run_id))
    
    # Manage Incident
    if not is_success:
        summary = f"Run {run_id} failed {len(failed_checks)} checks: {', '.join([r.check_name for r in failed_checks])}"
        create_incident(run_id, summary)
        block_gate(reason=summary)
    else:
        # If success, we should resolve any open incidents for this pipeline? 
        # Or just ensure gate is open if this is the "latest" run? 
        # For simplicity: If this run passed, we open the gate.
        open_gate(f"Run {run_id} passed validation")
        resolve_incident_if_exists(run_id)

    return is_success

def create_incident(run_id: str, summary: str):
    # Idempotency: Check if incident exists for this run
    row = fetch_one("SELECT incident_id FROM incidents WHERE run_id = %s", (run_id,))
    if row:
        return # Already exists
        
    sql = """
    INSERT INTO incidents (run_id, severity, status, summary)
    VALUES (%s, 'BLOCK', 'OPEN', %s)
    """
    execute_query(sql, (run_id, summary))
    logger.error(f"Incident created for run {run_id}")

def resolve_incident_if_exists(run_id: str):
    sql = "UPDATE incidents SET status = 'RESOLVED', resolved_at = NOW() WHERE run_id = %s AND status = 'OPEN'"
    execute_query(sql, (run_id,))

def block_gate(reason: str):
    sql = "UPDATE downstream_gate SET blocked = TRUE, reason = %s, updated_at = NOW() WHERE gate_id = 1"
    execute_query(sql, (reason,))
    logger.warning("Downstream gate BLOCKED.")

def open_gate(reason: str):
    sql = "UPDATE downstream_gate SET blocked = FALSE, reason = %s, updated_at = NOW() WHERE gate_id = 1"
    execute_query(sql, (reason,))
    logger.info("Downstream gate OPEN.")

def is_gate_open() -> bool:
    row = fetch_one("SELECT blocked FROM downstream_gate WHERE gate_id = 1")
    return not row['blocked'] if row else True
