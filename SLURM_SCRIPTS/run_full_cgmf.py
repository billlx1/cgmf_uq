#!/bin/bash --login
#SBATCH --job-name=cgmf_workflow
#SBATCH --partition=serial
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time=04:00:00
#SBATCH --output=workflow_%j.out
#SBATCH --error=workflow_%j.err

#====================================================
# 1. Configuration & Paths
#====================================================
# Physics Parameters
EVENTS=5000
TARGET_ID=98252       # e.g., Cf-252 (Change to 92235 for U-235)
INCIDENT_E=0.0        # Spontaneous fission

# Directory Setup
# SLURM_SUBMIT_DIR is the directory where you ran 'sbatch' (your 'cgmf' folder)
WORK_DIR="$SLURM_SUBMIT_DIR"

# CGMF Installation Path (Hardcoded based on your previous scripts)
CGMF_ROOT="/mnt/iusers01/fatpou01/phy01/mbcxawh2/software/CGMF_MY_VERSION/CGMF_GDR_Params/"
CGMF_EXE="$CGMF_ROOT/build/utils/cgmf/cgmf.x"

# The python analysis script (assumed to be in the same folder)
PYTHON_SCRIPT="$WORK_DIR/Post_Processing_V2.py"

# Unique Filenames for Output
DATE_TAG=$(date +%Y%m%d_%H%M%S)
HIST_FILE="histories.cgmf"  # The default output name from CGMF
NEW_HIST_NAME="histories_${TARGET_ID}_${EVENTS}_${DATE_TAG}.cgmf"
PLOT_NAME="analysis_${TARGET_ID}_${EVENTS}_${DATE_TAG}.png"

#====================================================
# 2. Environment Setup
#====================================================
echo ">> [$(date)] Setting up environment..."

# Load Compilers (for C++ execution if needed)
module purge
module load tools/gcc/git/2.24.0

# Load Python (for analysis)
module load apps/binapps/conda/miniforge3/25.9.1
conda activate cgmf_py

# Export Paths for Python and CGMF
export CGMFPATH=$CGMF_ROOT
export PYTHONPATH=$CGMFPATH/tools:$PYTHONPATH
export LD_LIBRARY_PATH=$CGMFPATH/lib:$LD_LIBRARY_PATH

# Go to the working directory (should already be there, but just in case)
cd "$WORK_DIR" || exit 1

echo "   Host: $(hostname)"
echo "   Work Dir: $(pwd)"
echo "   CGMF Exe: $CGMF_EXE"
echo "   Py Script: $PYTHON_SCRIPT"

# Check inputs
if [ ! -x "$CGMF_EXE" ]; then echo "ERROR: CGMF exe not found at $CGMF_EXE"; exit 1; fi
if [ ! -f "$PYTHON_SCRIPT" ]; then echo "ERROR: Python script not found at $PYTHON_SCRIPT"; exit 1; fi

#====================================================
# 3. Execution Step 1: Run CGMF
#====================================================
echo -e "\n===================================================="
echo ">> [$(date)] Step 1: Running CGMF Simulation ($EVENTS events)"
echo "===================================================="

start_time_sim=$(date +%s)

# Run CGMF
time "$CGMF_EXE" -i $TARGET_ID -e $INCIDENT_E -n $EVENTS

end_time_sim=$(date +%s)
duration_sim=$((end_time_sim - start_time_sim))

#====================================================
# 4. Execution Step 2: Rename Output
#====================================================
echo -e "\n===================================================="
echo ">> [$(date)] Step 2: Renaming output files"
echo "===================================================="

start_time_mv=$(date +%s)

# Check for output file (CGMF sometimes adds .0 to the end)
if [ -f "${HIST_FILE}.0" ]; then
    mv "${HIST_FILE}.0" "$NEW_HIST_NAME"
    echo "   Renamed ${HIST_FILE}.0 -> $NEW_HIST_NAME"
elif [ -f "$HIST_FILE" ]; then
    mv "$HIST_FILE" "$NEW_HIST_NAME"
    echo "   Renamed $HIST_FILE -> $NEW_HIST_NAME"
else
    echo "ERROR: Could not find history file ($HIST_FILE or ${HIST_FILE}.0)"
    exit 1
fi

end_time_mv=$(date +%s)

#====================================================
# 5. Execution Step 3: Python Analysis
#====================================================
echo -e "\n===================================================="
echo ">> [$(date)] Step 3: Processing data with Python"
echo "===================================================="

start_time_py=$(date +%s)

# Run the python script
time python3 "$PYTHON_SCRIPT" "$NEW_HIST_NAME" --output "$PLOT_NAME"

end_time_py=$(date +%s)
duration_py=$((end_time_py - start_time_py))

#====================================================
# 6. Final Summary
#====================================================
echo -e "\n===================================================="
echo "   WORKFLOW SUMMARY"
echo "===================================================="
echo "   Simulation Time: $duration_sim sec"
echo "   Analysis Time:   $duration_py sec"
echo "   Total Time:      $((duration_sim + duration_py)) sec"
echo "   Generated File:  $NEW_HIST_NAME"
echo "   Generated Plot:  $PLOT_NAME"
echo "   Completed at:    $(date)"
