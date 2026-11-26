#!/usr/bin/env bash
# End-to-end local run: generate -> daily incremental silver -> gold marts -> validate against ground truth
set -euo pipefail
EVENTS="${1:-300000}"
python3 -c "import pyspark, pandas, numpy" 2>/dev/null || pip install -q -r requirements.txt
python3 src/generate_data.py --out data/input --events "$EVENTS"
python3 src/silver_clean.py --input data/input --output data/lake     # loops over every dt= partition, one "day" at a time
python3 src/gold_marts.py --output data/lake
python3 src/validate_results.py data/input data/lake
