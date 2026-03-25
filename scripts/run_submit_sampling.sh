#!/bin/bash

# ============================
# User-editable parameters
# ============================

REGISTRY=config/Parameter_Registry.yaml
SAMPLING=config/Sampling_Config.yaml
OUTPUT=DryTest_Results/SAMPLING_TEST/

EVENTS=100000
TARGET_ID=92235
INCIDENT_E=0.0000000253

JOB_NAME=SAMPLING_TEST
MAX_CONCURRENT=80

PROJECT_DIR=.
CGMF_ROOT=/path/to/CGMF_GDR_Params
CONDA_ROOT=/path/to/miniforge3
CGMF_DEFAULT_DATA=CGMF_Data_Default
POST_PROCESSOR=scripts/post_processing.py
CONDA_ENV=cgmf_py

TIME_LIMIT="05:30:00"

# ============================
# Run orchestrator (dry-run by default)
# Add --submit to actually submit to SLURM
# ============================

python3 ./scripts/submit_sampling.py \
  --registry "$REGISTRY" \
  --sampling "$SAMPLING" \
  --output "$OUTPUT" \
  --project-dir "$PROJECT_DIR" \
  --cgmf-root "$CGMF_ROOT" \
  --conda-root "$CONDA_ROOT" \
  --cgmf-default-data "$CGMF_DEFAULT_DATA" \
  --post-processor "$POST_PROCESSOR" \
  --conda-env "$CONDA_ENV" \
  --events "$EVENTS" \
  --target-id "$TARGET_ID" \
  --incident-e "$INCIDENT_E" \
  --job-name "$JOB_NAME" \
  --max-concurrent "$MAX_CONCURRENT" \
  --time-limit "$TIME_LIMIT" \
  --force

# Optional reuse from prior run:
#   --reuse-configs /path/to/old/output \
#   --reuse-groups all-except:yamodel_uncorr_20pct
