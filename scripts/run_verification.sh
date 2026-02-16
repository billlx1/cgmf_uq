#!/bin/bash --login
#SBATCH -p serial
#SBATCH -t 30
#SBATCH -o verification_results/logs/%x_%j.out
#SBATCH -e verification_results/logs/%x_%j.err


# Path variables
SCRIPT_ROOT=~/software/TEST_PROJECT_ROOT/tests

RUNS_DIR=./runs

BASELINE_DIR=~/software/TEST_PROJECT_ROOT/CGMF_Data_Default

TARGET_ZAID=92236

# Output Organization
BASE_OUT_DIR=./verification_results

DAT_RPT_DIR=${BASE_OUT_DIR}/dat_perturbations_logs


# Ensure output directories exist
echo "Creating output directories at: ${BASE_OUT_DIR}"
mkdir -p "${DAT_RPT_DIR}"

echo "Starting verification job at $(date)"
echo "Target Runs Directory: ${RUNS_DIR}"
echo "----------------------------------------------------------------"

# ==============================================================================
# STEP 1: Verify Post-Processing Uniqueness
# ==============================================================================
echo "[1/2] Running Verify Post-Procs..."

python "${SCRIPT_ROOT}/verify_post_procs.py" \
    --runs-dir "${RUNS_DIR}" \
    --output "${BASE_OUT_DIR}/post_proc_summary.txt" \
    --verbose

echo "Step 1 Complete. Summary saved to: ${BASE_OUT_DIR}/post_proc_summary.txt"
echo "----------------------------------------------------------------"

# ==============================================================================
# STEP 2: Verify DAT Perturbations (ZAID 92236)
# ==============================================================================
echo "[2/2] Running Verify DAT Perturbations for ZAID ${TARGET_ZAID}..."

# Note: This requires the BASELINE_DIR to be set correctly above
if [ ! -d "$BASELINE_DIR" ]; then
    echo "ERROR: Baseline directory '$BASELINE_DIR' not found."
    echo "Please edit the script and set the correct BASELINE_DIR path."
    exit 1
fi

python "${SCRIPT_ROOT}/verify_dat_perturbations.py" \
    --baseline "${BASELINE_DIR}" \
    --runs "${RUNS_DIR}" \
    --output "${DAT_RPT_DIR}" \
    --zaid "${TARGET_ZAID}" \
    --verbose

echo "Step 2 Complete. Detailed reports saved to: ${DAT_RPT_DIR}"
echo "----------------------------------------------------------------"
echo "Job finished at $(date)"

