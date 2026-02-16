#!/bin/bash

# ============================
# User-editable parameters
# ============================

REGISTRY=~/software/TEST_PROJECT_ROOT/Config/Parameter_Registry.yaml
SENSITIVITY=~/software/TEST_PROJECT_ROOT/Config/Sensitivity_Coeff.yaml
OUTPUT=~/scratch/CGMF_2026/FULL_TEST_15_02_26/

EVENTS=100000
TARGET_ID=92235
INCIDENT_E=0.0000000253

JOB_NAME=FULL_TEST_15_02_26
MAX_CONCURRENT=80

POST_PROCESSOR=~/software/TEST_PROJECT_ROOT/scripts/post_processing.py

TIME_LIMIT="05:30:00"

# ============================
# Run orchestrator
# ============================

python3 ~/software/TEST_PROJECT_ROOT/scripts/submit_sensitivity.py \
  --registry "$REGISTRY" \
  --sensitivity "$SENSITIVITY" \
  --output "$OUTPUT" \
  --events "$EVENTS" \
  --target-id "$TARGET_ID" \
  --incident-e "$INCIDENT_E" \
  --job-name "$JOB_NAME" \
  --max-concurrent "$MAX_CONCURRENT" \
  --post-processor "$POST_PROCESSOR" \
  --time-limit "$TIME_LIMIT" \
  --force

