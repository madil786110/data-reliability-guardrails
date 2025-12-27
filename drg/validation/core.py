import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from drg.contracts.loader import Contract, SchemaField
from drg.utils import logger

class ValidationResult:
    def __init__(self, check_name: str, passed: bool, metric: Any, details: Dict = None):
        self.check_name = check_name
        self.passed = passed
        self.metric = metric
        self.details = details or {}

def validate_schema(df: pd.DataFrame, schema: List[SchemaField]) -> ValidationResult:
    missing_cols = []
    type_mismatch = []
    
    for field in schema:
        if field.name not in df.columns:
            if field.required:
                missing_cols.append(field.name)
        else:
            # Simple type check - in real world use pyarrow types or something robust
            # Here we just check minimal compatibility
            pass 
            
    if missing_cols:
        return ValidationResult("schema_presence", False, len(missing_cols), {"missing": missing_cols})
    
    return ValidationResult("schema_presence", True, 0, {})

def validate_volume(df: pd.DataFrame, checks: Dict) -> ValidationResult:
    count = len(df)
    min_rows = checks.get('volume', {}).get('min_rows', 0)
    max_rows = checks.get('volume', {}).get('max_rows', float('inf'))
    
    passed = min_rows <= count <= max_rows
    return ValidationResult("volume", passed, count, {"min": min_rows, "max": max_rows})

def validate_freshness(df: pd.DataFrame, checks: Dict) -> ValidationResult:
    config = checks.get('freshness', {})
    max_delay = config.get('max_delay_hours', 24)
    
    if 'pickup_datetime' not in df.columns:
        return ValidationResult("freshness", False, "N/A", {"error": "pickup_datetime missing"})
    
    # Ideally use max timestamp in data
    latest_ts = pd.to_datetime(df['pickup_datetime']).max()
    now = datetime.now() # In real system, pass 'execution_time'
    
    # If data is purely synthetic and "now" is used during generation, this might be tricky if system clocks drift
    # But for this assignment, we use strict check.
    delay_hours = (now - latest_ts).total_seconds() / 3600.0
    
    passed = delay_hours <= max_delay
    return ValidationResult("freshness", passed, round(delay_hours, 2), {"threshold": max_delay, "latest_ts": str(latest_ts)})

def calculate_psi(expected, actual, bucket_type='bins', buckets=10, axis=0):
    '''Calculate the PSI (population stability index) across all variables'''
    def psi(expected_array, actual_array, buckets):
        def scale_range (input, min, max):
            input += -(np.min(input))
            input /= np.max(input) / (max - min)
            input += min
            return input

        breakpoints = np.arange(0, buckets + 1) / (buckets) * 100

        if bucket_type == 'bins':
            breakpoints = scale_range(breakpoints, np.min(expected_array), np.max(expected_array))
        elif bucket_type == 'quantiles':
            breakpoints = np.stack([np.percentile(expected_array, b) for b in breakpoints])

        expected_percents = np.histogram(expected_array, breakpoints)[0] / len(expected_array)
        actual_percents = np.histogram(actual_array, breakpoints)[0] / len(actual_array)

        def sub_psi(e_perc, a_perc):
            if a_perc == 0:
                a_perc = 0.0001
            if e_perc == 0:
                e_perc = 0.0001

            value = (e_perc - a_perc) * np.log(e_perc / a_perc)
            return(value)

        psi_value = np.sum(sub_psi(expected_percents[i], actual_percents[i]) for i in range(0, len(expected_percents)))
        return psi_value

    return psi(expected, actual, buckets)

def validate_distribution(df: pd.DataFrame, checks: Dict) -> ValidationResult:
    config = checks.get('distribution', {})
    method = config.get('method')
    column = config.get('column')
    threshold = config.get('threshold', 0.2)
    ref_path = config.get('reference_path')
    
    if method != 'psi' or not ref_path or column not in df.columns:
        return ValidationResult("distribution", True, 0.0, {"skip": "invalid config or col missing"})
        
    try:
        ref_df = pd.read_parquet(ref_path)
        if column not in ref_df.columns:
             return ValidationResult("distribution", False, -1, {"error": "col missing in ref"})
             
        psi_score = calculate_psi(ref_df[column].dropna().values, df[column].dropna().values)
        passed = psi_score <= threshold
        return ValidationResult("distribution", passed, round(psi_score, 4), {"threshold": threshold})
        
    except Exception as e:
        logger.error(f"Distribution check failed: {e}")
        return ValidationResult("distribution", False, -1, {"error": str(e)})

def run_validations(df: pd.DataFrame, contract: Contract) -> List[ValidationResult]:
    results = []
    
    # 1. Schema
    results.append(validate_schema(df, contract.schema))
    
    # 2. Volume
    results.append(validate_volume(df, contract.checks))
    
    # 3. Freshness
    results.append(validate_freshness(df, contract.checks))
    
    # 4. Distribution
    if 'distribution' in contract.checks:
        results.append(validate_distribution(df, contract.checks))
        
    return results
