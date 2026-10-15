---
paths:
  - "src/**/*.py"
  - "notebooks/**"
---

# Research hygiene for detector and evaluation code

- A detector function receives only the past: its inputs end at the current event's timestamp.
  If a function takes a whole DataFrame, it must sort by time and use rolling/expanding windows
  that exclude the current row (`shift(1)` or `closed="left"`).
- Baselines ("normal" volume, a book's usual gap) are computed on data strictly before the event.
- Evaluation code is separate from detection code (`evaluate.py` vs `detect.py`). Only evaluation
  may read outcomes, closing odds or future prices.
- The tuning/test split is by time, defined once in `src/market_anomaly_detection/core/splits.py`, and reused.
- Every evaluation reports: alerts per day, precision at the alert budget, and the same metrics for
  at least one naive baseline.
