#!/bin/bash --login
#SBATCH -p serial
#SBATCH -t 120
#SBATCH -J sensitivity_analysis
# NOTE: The log directory must exist before sbatch is called. Run:
#   mkdir -p ./test_results_v2/logs
# once before your first sbatch submission.
#SBATCH -o ./test_results_v3/logs/%x_%j.out
#SBATCH -e ./test_results_v3/logs/%x_%j.err

# ==============================================================================
#  run_sensitivity_analysis.sh
#  ----------------------------
#  Runs the Phase-I sensitivity post-processor (analyse_sensitivity.py)
#  in the SLURM serial queue.
# ==============================================================================

# ==============================================================================
# CONFIG  –  Edit these before submitting
# ==============================================================================

# Root of the test run (directory containing runs/ and configs/)
RUN_ROOT="./SAMPLING_TEST_23_03_ALL_v2"

# Path to the analyse_sensitivity.py script.
SCRIPT_PATH="./test_analyse_v4.py"

# Conda environment containing numpy + matplotlib
CONDA_ENV=cgmf_py

# Manifest produced by the sensitivity sweep
MANIFEST="${RUN_ROOT}/configs/manifest.csv"

# Directory containing task_0/, task_1/, … sub-directories
RUNS_DIR="${RUN_ROOT}/runs"

# Path to a completed, UNPERTURBED CGMF task directory
DEFAULT_TASK_DIR="/mnt/iusers01/fatpou01/phy01/mbcxawh2/scratch/CGMF_2026/FULL_TEST_15_02_26/runs/task_108"

# Where to write figures, CSVs, and the summary
BASE_OUT_DIR="./test_results_v3"

# Truncate the gamma spectrum plots at this energy (MeV).
MAX_GAMMA_ENERGY_MEV=14.0

# Truncate the neutron spectrum plots at this energy (MeV).
MAX_NEUTRON_ENERGY_MEV=14.0

# Percentile used for the symmetric colour scale on any heatmap panels.
VMAX_PCT=85

# Figure DPI
FIG_DPI=250

# Set to "--debug" to enable verbose per-task printouts, "" to suppress
DEBUG_FLAG="--debug"

# Evaluated/Experimental spectra and multiplicity files/values
PFNS_FILE="U5_PFNS_E80_Dat.txt"
PFGS_FILE="U5_PFGS_My_Dat.txt"
EVAL_NUBAR_N="2.43"
EVAL_NUBAR_N_UNC="0.005"
EVAL_NUBAR_G="7.75"
EVAL_NUBAR_G_UNC="0.58125"

# χ² energy evaluation windows (MeV).  Leave blank for no restriction.
PFNS_EMIN=""
PFNS_EMAX="6.0"
PFGS_EMIN="1.0"
PFGS_EMAX="4.0"

# χ² weights for combined score.  Need not sum to 1; normalised internally.
W_PFNS="0.005"
W_PFGS="0.705"
W_NUBAR_N="0.0001"
W_NUBAR_G="0.2899"

# Number of top runs (lowest χ²_combined) to isolate and re-plot.
# Set to an integer (e.g., 20) or leave blank ("") to disable.
N_ACCEPTED="250"

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

# Explicit conda initialisation (required for non-interactive shells)
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

if [ ! -f "${SCRIPT_PATH}" ]; then
    echo "ERROR: Analysis script not found at '${SCRIPT_PATH}'."
    exit 1
fi

if [ ! -d "${RUNS_DIR}" ]; then
    echo "ERROR: Runs directory not found at '${RUNS_DIR}'."
    exit 1
fi

# Count task dirs and manifest entries for the log
N_TASKS=$(find "${RUNS_DIR}" -maxdepth 1 -type d -name "task_*" | wc -l)
echo "  task_* dirs found : ${N_TASKS}"

if [ "${N_TASKS}" -eq 0 ]; then
    echo "ERROR: No task_* directories found in '${RUNS_DIR}'."
    exit 1
fi

if [ -f "${MANIFEST}" ]; then
    N_MANIFEST=$(tail -n +2 "${MANIFEST}" | wc -l)
    echo "  Manifest entries  : ${N_MANIFEST}"
else
    echo "  Manifest          : not found at '${MANIFEST}' (non-fatal)"
fi

if [ -n "${DEFAULT_TASK_DIR}" ] &&[ ! -d "${DEFAULT_TASK_DIR}" ]; then
    echo "WARNING: DEFAULT_TASK_DIR '${DEFAULT_TASK_DIR}' does not exist."
    echo "         Reference lines will be omitted from plots."
    DEFAULT_TASK_DIR=""
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
# BUILD ARGUMENT STRING
# ==============================================================================

ARGS=(
    --runs_dir   "${RUNS_DIR}"
    --output_dir "${BASE_OUT_DIR}"
    --vmax_pct   "${VMAX_PCT}"
    --fig_dpi    "${FIG_DPI}"
    --drop_all_zero_params
)

# Optional: manifest (informational)
if [ -f "${MANIFEST}" ]; then
    ARGS+=(--manifest "${MANIFEST}")
fi

# Optional: default reference run
if [ -n "${DEFAULT_TASK_DIR}" ]; then
    ARGS+=(--default_dir "${DEFAULT_TASK_DIR}")
fi

# Optional: energy truncation for spectra
if [ -n "${MAX_GAMMA_ENERGY_MEV}" ]; then
    ARGS+=(--max_gamma_energy_MeV "${MAX_GAMMA_ENERGY_MEV}")
fi
if [ -n "${MAX_NEUTRON_ENERGY_MEV}" ]; then
    ARGS+=(--max_neutron_energy_MeV "${MAX_NEUTRON_ENERGY_MEV}")
fi

# Optional: debug flag
if [ -n "${DEBUG_FLAG}" ]; then
    ARGS+=("${DEBUG_FLAG}")
fi

# Optional: evaluated/experimental references
if [ -n "${PFNS_FILE}" ]; then
    ARGS+=(--pfns_file "${PFNS_FILE}")
fi
if [ -n "${PFGS_FILE}" ]; then
    ARGS+=(--pfgs_file "${PFGS_FILE}")
fi
if [ -n "${EVAL_NUBAR_N}" ]; then
    ARGS+=(--eval_nubar_n "${EVAL_NUBAR_N}")
fi
if [ -n "${EVAL_NUBAR_N_UNC}" ]; then
    ARGS+=(--eval_nubar_n_unc "${EVAL_NUBAR_N_UNC}")
fi
if [ -n "${EVAL_NUBAR_G}" ]; then
    ARGS+=(--eval_nubar_g "${EVAL_NUBAR_G}")
fi
if [ -n "${EVAL_NUBAR_G_UNC}" ]; then
    ARGS+=(--eval_nubar_g_unc "${EVAL_NUBAR_G_UNC}")
fi

# Optional: χ² energy windows
if [ -n "${PFNS_EMIN}" ]; then
    ARGS+=(--pfns_emin "${PFNS_EMIN}")
fi
if [ -n "${PFNS_EMAX}" ]; then
    ARGS+=(--pfns_emax "${PFNS_EMAX}")
fi
if [ -n "${PFGS_EMIN}" ]; then
    ARGS+=(--pfgs_emin "${PFGS_EMIN}")
fi
if [ -n "${PFGS_EMAX}" ]; then
    ARGS+=(--pfgs_emax "${PFGS_EMAX}")
fi

# χ² weights (always passed – defaults defined in CONFIG block above)
ARGS+=(--w_pfns    "${W_PFNS}")
ARGS+=(--w_pfgs    "${W_PFGS}")
ARGS+=(--w_nubar_n "${W_NUBAR_N}")
ARGS+=(--w_nubar_g "${W_NUBAR_G}")

# Optional: Accepted subset filtering
if [ -n "${N_ACCEPTED}" ]; then
    ARGS+=(--n_accepted "${N_ACCEPTED}")
fi

# ==============================================================================
# STEP 1: Run Sensitivity Analysis
# ==============================================================================

echo ""
echo "[1/1] Running analyse_sensitivity.py..."
echo "      vmax_pct            = ${VMAX_PCT}"
echo "      fig_dpi             = ${FIG_DPI}"
echo "      max_gamma_E  (MeV)  = ${MAX_GAMMA_ENERGY_MEV:-full range}"
echo "      max_neutron_E (MeV) = ${MAX_NEUTRON_ENERGY_MEV:-full range}"
echo "      default_dir         = ${DEFAULT_TASK_DIR:-not set}"
echo "      pfns_file           = ${PFNS_FILE:-not set}"
echo "      pfgs_file           = ${PFGS_FILE:-not set}"
echo "      eval_nubar_n        = ${EVAL_NUBAR_N:-not set} +/- ${EVAL_NUBAR_N_UNC:-not set}"
echo "      eval_nubar_g        = ${EVAL_NUBAR_G:-not set} +/- ${EVAL_NUBAR_G_UNC:-not set}"
echo "      pfns_emin/emax      = ${PFNS_EMIN:-none} / ${PFNS_EMAX:-none} MeV"
echo "      pfgs_emin/emax      = ${PFGS_EMIN:-none} / ${PFGS_EMAX:-none} MeV"
echo "      chi2 weights        = PFNS=${W_PFNS}  PFGS=${W_PFGS}  nubar_n=${W_NUBAR_N}  nubar_g=${W_NUBAR_G}"
echo "      n_accepted          = ${N_ACCEPTED:-disabled}"
echo "      debug               = ${DEBUG_FLAG:-off}"
echo ""

python "${SCRIPT_PATH}" "${ARGS[@]}"

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
    
    # List top-level files
    find "${BASE_OUT_DIR}" -maxdepth 1 -type f | sort | while read -r f; do
        SIZE=$(du -sh "${f}" 2>/dev/null | cut -f1)
        echo "  ${SIZE}  ${f}"
    done
    
    # Check if the accepted sub-directory was generated
    if [ -n "${N_ACCEPTED}" ] &&[ -d "${BASE_OUT_DIR}/accepted_${N_ACCEPTED}" ]; then
        echo ""
        echo "Accepted subset files generated in:"
        echo "  (Dir)  ${BASE_OUT_DIR}/accepted_${N_ACCEPTED}/"
        # List files inside the accepted directory
        find "${BASE_OUT_DIR}/accepted_${N_ACCEPTED}" -maxdepth 1 -type f | sort | while read -r f; do
            SIZE=$(du -sh "${f}" 2>/dev/null | cut -f1)
            echo "    ${SIZE}  ${f}"
        done
    fi
else
    echo "[FAILED] analyse_sensitivity.py exited with code ${EXIT_CODE}."
    echo "         Check the error log:"
    echo "         ${LOG_DIR}/${SLURM_JOB_NAME}_${SLURM_JOB_ID}.err"
fi

echo ""
echo "Finished at: $(date)"
echo "============================================================"

exit ${EXIT_CODE}
