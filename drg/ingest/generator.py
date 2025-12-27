import sys
import os
import uuid
import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from drg.utils import logger

class DataGenerator:
    def __init__(self, seed: int = 42):
        self.seed = seed
        self.rng = np.random.default_rng(seed)
        random.seed(seed)
        
    def generate_batch(self, num_rows: int = 1000, reference_date: datetime = None) -> pd.DataFrame:
        """Generates a clean batch of ride data."""
        
        # Determine reference time
        now = reference_date if reference_date else datetime.now()
        timestamps = [now - timedelta(minutes=int(x)) for x in self.rng.integers(0, 120, size=num_rows)]
        
        data = {
            # Use smaller range for numpy capability or use python random for full 128 bit if needed.
            # Using 2**63-1 is sufficient for uniqueness in this toy dataset.
            'ride_id': [str(uuid.UUID(int=self.rng.integers(0, 2**63 - 1))) for _ in range(num_rows)],
            'vendor_id': self.rng.choice([1, 2], size=num_rows),
            'pickup_datetime': timestamps,
            'dropoff_datetime': [t + timedelta(minutes=int(self.rng.integers(5, 60))) for t in timestamps],
            'passenger_count': self.rng.choice([1, 2, 3, 4, 5, 6], size=num_rows, p=[0.7, 0.15, 0.05, 0.05, 0.02, 0.03]),
            'trip_distance': self.rng.exponential(2.5, size=num_rows).clip(0.1, 50.0),
            'fare_amount': self.rng.lognormal(2.5, 0.5, size=num_rows).clip(2.5, 500.0)
        }
        
        df = pd.DataFrame(data)
        return df

    def inject_failure(self, df: pd.DataFrame, scenario: str) -> pd.DataFrame:
        """Injects specific data quality failures."""
        logger.info(f"Injecting failure scenario: {scenario}")
        
        if scenario == 'schema_drift':
            # Rename a required column
            df = df.rename(columns={'vendor_id': 'provider_id'})
            # Or change type
            df['passenger_count'] = df['passenger_count'].astype(str)
            
        elif scenario == 'late_data':
            # Shift data back by 48 hours (freshness failure)
            df['pickup_datetime'] = df['pickup_datetime'] - timedelta(hours=48)
            df['dropoff_datetime'] = df['dropoff_datetime'] - timedelta(hours=48)
            
        elif scenario == 'value_spike':
            # Multiply fare_amount by 100 for 50% of rows
            mask = self.rng.choice([True, False], size=len(df))
            df.loc[mask, 'fare_amount'] = df.loc[mask, 'fare_amount'] * 100.0
            
        elif scenario == 'null_explosion':
            # Set 40% of vendor_id to None
            mask = self.rng.random(len(df)) < 0.4
            df.loc[mask, 'vendor_id'] = None
            
        elif scenario == 'missing_partition':
            # Simulate by returning empty DF? or raising error?
            # Or just return empty dataframe to simulate empty file found
            # But the volume check (min_rows) should catch empty DF.
            return pd.DataFrame(columns=df.columns)
            
        return df

def generate_and_save(output_path: str, run_id: str, scenario: str = None, seed: int = 42, rows: int = 1000):
    os.makedirs(output_path, exist_ok=True)
    
    gen = DataGenerator(seed=seed)
    df = gen.generate_batch(num_rows=rows)
    
    if scenario:
        df = gen.inject_failure(df, scenario)
    
    filename = f"{output_path}/rides_{run_id}.parquet"
    df.to_parquet(filename, index=False)
    logger.info(f"Generated {len(df)} rows to {filename}")
    return filename

if __name__ == "__main__":
    # Test generation
    generate_and_save("data/raw/test", "test_run")
