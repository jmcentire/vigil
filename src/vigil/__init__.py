"""vigil — sub-threshold anomaly detection for the Exemplar stack.

ADR-001 locks: standalone Python service, off-path, V1 detection is
quantile-based (rolling 7d p95 + multiplicative threshold), NOT z-score.

Public surface here is intentionally thin. Most callers integrate via the
HTTP API (see `vigil.api`) rather than importing the Python module.
"""

from vigil.types import Anomaly, AnomalyKey, BaselineSnapshot, MetricBucket

__version__ = "0.1.0"

__all__ = [
    "Anomaly",
    "AnomalyKey",
    "BaselineSnapshot",
    "MetricBucket",
    "__version__",
]
