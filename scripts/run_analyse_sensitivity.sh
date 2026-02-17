#!/bin/bash --login
#SBATCH -p serial
#SBATCH -t 120
#SBATCH -J sensitivity_analysis
#SBATCH -o sensitivity_results/logs/%x_%j.out
#SBATCH -e sensitivity_results/logs/%x_%j.err

# ==============================================================================
#  run_sensitivity_analysis.sh
#  ----------------------------
#  Runs the Phase-I sensitivity post-processor (analyse_sensitivity.py)
#  in the SLURM serial queue.
#
#  Usage:
#    sbatch run_sensitivity_analysis.sh
#
#  Customise the variables in the CONFIG block below before submitting.
#  All other sections should not need to be edited for a standard run.
# ==============================================================================

# ==============================================================================
# CONFIG  –  Edit these before submitting
# ==============================================================================

# Root of the test run (directory containing runs/ and configs/)
RUN_ROOT=.

# Path to the analysis script
SCRIPT_ROOT=~/software/TEST_PROJECT_ROOT/scripts

# Conda environment with numpy + matplotlib
CONDA_ENV=cgmf_py

# Manifest produced by the sensitivity sweep
MANIFEST="${RUN_ROOT}/configs/manifest.txt"

# Directory containing task_0/, task_1/, … sub-directories
RUNS_DIR="${RUN_ROOT}/runs"

# Where to write figures, CSVs, and the summary
BASE_OUT_DIR="${RUN_ROOT}/sensitivity_results"

# Percentile of |S| used for the symmetric colour scale (99 = show outliers,
# 95 = compress them harder). Float, 0–100.
VMAX_PCT=85

# Figure DPI  (150 is a good balance of quality vs file size)
FIG_DPI=250

# Set to "--debug" to enable verbose per-task printouts, "" to suppress
DEBUG_FLAG="--debug"

# ==============================================================================
# ENVIRONMENT SETUP
# ==============================================================================

echo "============================================================"
echo "  CGMF Sensitivity Analysis  –  Phase I Post-Processor"
echo "============================================================"
echo "  Job name    : ${SLURM_JOB_NAME}"
echo "  Job ID      : ${SLURM_JOB_ID}"
echo "  Node        : ${SLURM_NODELIST}"
echo "  Partition   : ${SLURM_JOB_PARTITION}"
echo "  Started at  : $(date)"
echo "  Run root    : ${RUN_ROOT}"
echo "  Output dir  : ${BASE_OUT_DIR}"
echo "============================================================"


module purge
module load tools/gcc/git/2.24.0
module load apps/binapps/conda/miniforge3/25.9.1

# Explicit conda initialization (handles non-interactive shells)
#source "$CONDA_ROOT/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV"

# Activate conda – the --login flag on the shebang ensures module/conda
# initialisers in ~/.bashrc are sourced before this point
conda activate "${CONDA_ENV}"
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to activate conda environment '${CONDA_ENV}'."
    echo "       Check CONDA_ENV in the CONFIG block."
    exit 1
fi

echo "Python  : $(which python)"
echo "Version : $(python --version)"
echo "------------------------------------------------------------"

# ==============================================================================
# PRE-FLIGHT CHECKS
# ==============================================================================

echo "[PRE-FLIGHT] Checking required paths..."

if [ ! -f "${MANIFEST}" ]; then
    echo "ERROR: Manifest not found at '${MANIFEST}'."
    echo "       Check MANIFEST in the CONFIG block."
    exit 1
fi

if [ ! -d "${RUNS_DIR}" ]; then
    echo "ERROR: Runs directory not found at '${RUNS_DIR}'."
    echo "       Check RUNS_DIR in the CONFIG block."
    exit 1
fi

if [ ! -f "${RUN_ROOT}/analyse_sensitivity.py" ]; then
    echo "ERROR: Analysis script not found at '${SCRIPT_ROOT}/analyse_sensitivity.py'."
    echo "       Check SCRIPT_ROOT in the CONFIG block."
    exit 1
fi

# Count how many task directories are present for the log
N_TASKS=$(find "${RUNS_DIR}" -maxdepth 1 -type d -name "task_*" | wc -l)
N_MANIFEST=$(tail -n +2 "${MANIFEST}" | wc -l)

echo "  Manifest entries : ${N_MANIFEST}"
echo "  task_* dirs found: ${N_TASKS}"

if [ "${N_TASKS}" -eq 0 ]; then
    echo "ERROR: No task_* directories found in '${RUNS_DIR}'."
    exit 1
fi

echo "[PRE-FLIGHT] All checks passed."
echo "------------------------------------------------------------"

# ==============================================================================
# OUTPUT DIRECTORY SETUP
# ==============================================================================

LOG_DIR="${BASE_OUT_DIR}/logs"

echo "Creating output directories..."
mkdir -p "${LOG_DIR}"
echo "  ${BASE_OUT_DIR}  (figures + CSV)"
echo "  ${LOG_DIR}  (SLURM stdout/stderr)"

# ==============================================================================
# STEP 1: Run Sensitivity Analysis
# ==============================================================================

echo ""
echo "[1/1] Running analyse_sensitivity.py..."
echo "      vmax_pct  = ${VMAX_PCT}"
echo "      fig_dpi   = ${FIG_DPI}"
echo "      debug     = ${DEBUG_FLAG:-off}"
echo ""

python "${RUN_ROOT}/analyse_sensitivity.py" \
    --runs_dir   "${RUNS_DIR}"    \
    --manifest   "${MANIFEST}"    \
    --output_dir "${BASE_OUT_DIR}" \
    --vmax_pct   "${VMAX_PCT}"    \
    --fig_dpi    "${FIG_DPI}"     \
    --max_gamma_energy_MeV 4.3 \
    --max_neutron_energy_MeV 5.5 \
    --drop_all_zero_params
    ${DEBUG_FLAG}

EXIT_CODE=$?

echo ""
echo "------------------------------------------------------------"

# ==============================================================================
# POST-RUN SUMMARY
# ==============================================================================

if [ ${EXIT_CODE} -eq 0 ]; then
    echo "[SUCCESS] analyse_sensitivity.py completed normally."
    echo ""
    echo "Output files:"
    find "${BASE_OUT_DIR}" -maxdepth 1 -type f | sort | while read -r f; do
        SIZE=$(du -sh "${f}" 2>/dev/null | cut -f1)
        echo "  ${SIZE}  ${f}"
    done
else
    echo "[FAILED] analyse_sensitivity.py exited with code ${EXIT_CODE}."
    echo "         Check the error log:"
    echo "         ${LOG_DIR}/${SLURM_JOB_NAME}_${SLURM_JOB_ID}.err"
fi

echo ""
echo "Finished at: $(date)"
echo "============================================================"

exit ${EXIT_CODE}
