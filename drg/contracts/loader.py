import yaml
import os
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

@dataclass
class SchemaField:
    name: str
    type: str
    required: bool = False
    min: Optional[float] = None
    max: Optional[float] = None

@dataclass
class Contract:
    dataset_id: str
    owner: str
    schema: List[SchemaField]
    checks: Dict[str, Any]

def load_contract(path: str) -> Contract:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Contract file not found: {path}")
    
    with open(path, 'r') as f:
        data = yaml.safe_load(f)
        
    schema_fields = []
    for field in data.get('schema', []):
        schema_fields.append(SchemaField(
            name=field['name'],
            type=field['type'],
            required=field.get('required', False),
            min=field.get('min'),
            max=field.get('max')
        ))
        
    return Contract(
        dataset_id=data['dataset_id'],
        owner=data.get('owner', 'unknown'),
        schema=schema_fields,
        checks=data.get('checks', {})
    )
