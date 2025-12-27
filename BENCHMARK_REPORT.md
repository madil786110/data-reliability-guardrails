
# Benchmark Report

**Runs**: 100
**Total Time**: 1.37s

## Metrics
- **Detection Rate**: 98.00%
- **False Positive Rate**: 0.00%
- **Avg Validation Latency**: 3.82ms

## Failure Breakdown
|                              |   count |
|:-----------------------------|--------:|
| ('clean', True)              |      95 |
| ('late_data', False)         |       1 |
| ('missing_partition', False) |       1 |
| ('null_explosion', True)     |       1 |
| ('schema_drift', False)      |       1 |
| ('volume_spike', True)       |       1 |

