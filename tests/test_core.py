import pytest
import pandas as pd
import numpy as np
from drg.validation.core import validate_schema, validate_volume, validate_freshness, validate_distribution, SchemaField
from drg.ingest.generator import DataGenerator

# --- Schema Tests ---
def test_schema_pass():
    df = pd.DataFrame({'a': [1], 'b': ['x']})
    schema = [SchemaField('a', 'int'), SchemaField('b', 'string')]
    res = validate_schema(df, schema)
    assert res.passed

def test_schema_fail_missing_col():
    df = pd.DataFrame({'a': [1]})
    schema = [SchemaField('a', 'int'), SchemaField('b', 'string', required=True)]
    res = validate_schema(df, schema)
    assert not res.passed
    assert 'b' in res.details['missing']

# --- Volume Tests ---
def test_volume_pass():
    df = pd.DataFrame({'a': range(10)})
    checks = {'volume': {'min_rows': 5, 'max_rows': 20}}
    res = validate_volume(df, checks)
    assert res.passed

def test_volume_fail_min():
    df = pd.DataFrame({'a': range(1)})
    checks = {'volume': {'min_rows': 5}}
    res = validate_volume(df, checks)
    assert not res.passed

# --- Ingest Tests ---
def test_generator_deterministic():
    from datetime import datetime
    fixed_date = datetime(2023, 1, 1, 12, 0, 0)
    
    gen1 = DataGenerator(seed=42)
    df1 = gen1.generate_batch(10, reference_date=fixed_date)
    
    gen2 = DataGenerator(seed=42)
    df2 = gen2.generate_batch(10, reference_date=fixed_date)
    
    pd.testing.assert_frame_equal(df1, df2)

def test_failure_injection_schema_drift():
    gen = DataGenerator(seed=42)
    df = gen.generate_batch(10)
    df_bad = gen.inject_failure(df, 'schema_drift')
    
    # Validation should catch this
    # Contract expects 'vendor_id', drift renames it to 'provider_id'
    schema = [SchemaField('vendor_id', 'int', required=True)]
    res = validate_schema(df_bad, schema)
    assert not res.passed

def test_failure_injection_value_spike():
    gen = DataGenerator(seed=42)
    df = gen.generate_batch(100)
    # Get mean before
    mean_before = df['fare_amount'].mean()
    
    df_bad = gen.inject_failure(df, 'value_spike')
    mean_after = df_bad['fare_amount'].mean()
    
    assert mean_after > mean_before * 10 
