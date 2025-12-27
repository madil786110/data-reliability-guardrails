import pytest
import uuid
import os
import time
import psycopg2
from drg.cli import generate_and_save, load_contract, run_validations, enforce_policy, save_check_result
from drg.policy.engine import is_gate_open, fetch_one
from drg.db import execute_query

# Check if DB is available
def is_db_available():
    try:
        conn = psycopg2.connect(
            dbname="drg_db",
            user="drg_user",
            password="drg_password",
            host="localhost",
            port="5432"
        )
        conn.close()
        return True
    except:
        return False

DB_AVAILABLE = is_db_available()

@pytest.mark.skipif(not DB_AVAILABLE, reason="Database not available. Run 'make up' to enable integration tests.")
class TestIntegration:
    
    @pytest.fixture(autouse=True)
    def clean_db(self):
        # Reset bits for clean slate
        execute_query("TRUNCATE pipeline_runs, check_results, incidents CASCADE")
        execute_query("UPDATE downstream_gate SET blocked = FALSE, reason = 'Test Init' WHERE gate_id = 1")
    
    def test_end_to_end_good_pipeline(self):
        from drg.policy.engine import register_run
        run_id = str(uuid.uuid4())
        
        # 1. Ingest
        fpath = generate_and_save("data/raw", run_id, seed=123)
        assert os.path.exists(fpath)
        register_run(run_id, fpath)
        
        # 2. Validation
        contract = load_contract("config/contract.yaml")
        import pandas as pd
        df = pd.read_parquet(fpath)
        results = run_validations(df, contract)
        
        # 3. Policy
        passed = enforce_policy(run_id, results)
        assert passed == True
        
        # 4. DB Check
        row = fetch_one("SELECT status FROM pipeline_runs WHERE run_id = %s", (run_id,))
        assert row is not None # Logic in enforce_policy updates run, need to ensure run was created. 
                               # Note: register_run is called in CLI, not automatically in enforce_policy.
                               # We must register manually in this test harness or update harness.
                               # Let's call register first.
        
    def test_cli_flow_simulation_good(self):
        """Simulate the CLI commands flow"""
        from drg.policy.engine import register_run
        
        run_id = str(uuid.uuid4())
        
        # 1. Ingest
        fpath = generate_and_save("data/raw", run_id, seed=123)
        register_run(run_id, fpath)
        
        # 2. Validate
        contract = load_contract("config/contract.yaml")
        import pandas as pd
        df = pd.read_parquet(fpath)
        results = run_validations(df, contract)
        
        # Save results like CLI does
        for r in results:
            save_check_result(run_id, r.check_name, r.passed, r.metric, r.details)
            
        passed = enforce_policy(run_id, results)
        
        assert passed == True
        assert is_gate_open() == True
        
    def test_cli_flow_simulation_bad(self):
        from drg.policy.engine import register_run
        
        run_id = str(uuid.uuid4())
        
        # 1. Ingest Bad Data (Schema Drift)
        fpath = generate_and_save("data/raw", run_id, scenario="schema_drift", seed=666)
        register_run(run_id, fpath)
        
        # 2. Validate
        contract = load_contract("config/contract.yaml")
        import pandas as pd
        df = pd.read_parquet(fpath)
        results = run_validations(df, contract)
        
        for r in results:
            save_check_result(run_id, r.check_name, r.passed, r.metric, r.details)
            
        passed = enforce_policy(run_id, results)
        
        # 3. Assertions
        assert passed == False
        assert is_gate_open() == False
        
        # Check Incident
        inc = fetch_one("SELECT * FROM incidents WHERE run_id = %s", (run_id,))
        assert inc is not None
        assert inc['status'] == 'OPEN'
        assert inc['severity'] == 'BLOCK'

    def test_replay_fix_flow(self):
        """Test blocking then fixing via replay"""
        from drg.policy.engine import register_run
        from drg.replay.manager import replay_run
        
        run_id = str(uuid.uuid4())
        
        # --- BAD RUN ---
        fpath = generate_and_save("data/raw", run_id, scenario="late_data", seed=666)
        register_run(run_id, fpath)
        
        # Validate (should fail freshness)
        import pandas as pd
        df = pd.read_parquet(fpath)
        contract = load_contract("config/contract.yaml")
        results = run_validations(df, contract)
        enforce_policy(run_id, results)
        
        assert is_gate_open() == False
        
        # --- REPLAY FIX ---
        # User requests replay with "clean" fix
        replay_run(run_id, fix_scenario="clean")
        
        # Re-validate
        df_fixed = pd.read_parquet(fpath) # Should be overwritten
        results_fixed = run_validations(df_fixed, contract)
        passed_fixed = enforce_policy(run_id, results_fixed)
        
        assert passed_fixed == True
        assert is_gate_open() == True
        
        # Incident should be resolved
        inc = fetch_one("SELECT * FROM incidents WHERE run_id = %s", (run_id,))
        assert inc['status'] == 'RESOLVED'
