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

# Hardcoded paths for first test
PROJECT_DIR="/mnt/iusers01/fatpou01/phy01/mbcxawh2/software/PROJECT_ROOT"
WORK_DIR="$SLURM_SUBMIT_DIR"

# CGMF Installation Path (Hardcoded based on your previous scripts)
CGMF_ROOT="/mnt/iusers01/fatpou01/phy01/mbcxawh2/software/CGMF_MY_VERSION/CGMF_GDR_Params/"
CGMF_EXE="$CGMF_ROOT/build/utils/cgmf/cgmf.x"

# Data and configuration paths
CGMF_DEFAULT_DATA="$PROJECT_DIR/CGMF_Data_Default"
SCALE_FACTOR_JSON_PATH="$PROJECT_DIR/Config/Default_Scale_Factors.json"
OUTPUT_SCALED_DIR="$WORK_DIR/SCALE_TEST_001"
POST_PROCESSING_SCRIPT="$PROJECT_DIR/POST_PROCESSING_SCRIPTS/Post_Processing_V2.py"
DAT_GENERATOR_SCRIPT="$PROJECT_DIR/cgmf_uq/io/dat_generator.py"

# Unique Filenames for Output
DATE_TAG=$(date +%Y%m%d_%H%M%S)
HIST_FILE="histories.cgmf"
NEW_HIST_NAME="histories_${TARGET_ID}_${EVENTS}_${DATE_TAG}.cgmf"
PLOT_NAME="analysis_${EVENTS}_${DATE_TAG}.png"

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
export CGMFPATH="$CGMF_ROOT"
export PYTHONPATH="$CGMFPATH/tools:$PYTHONPATH"
export LD_LIBRARY_PATH="$CGMFPATH/lib:$LD_LIBRARY_PATH"

# Go to the working directory (should already be there, but just in case)
cd "$WORK_DIR" || exit 1

echo "   Host: $(hostname)"
echo "   Work Dir: $(pwd)"
echo "   CGMF Exe: $CGMF_EXE"
echo "   Dat Generator: $DAT_GENERATOR_SCRIPT"
echo "   Post-Processing Script: $POST_PROCESSING_SCRIPT"

# Check inputs
if [ ! -x "$CGMF_EXE" ]; then 
    echo "ERROR: CGMF exe not found at $CGMF_EXE"
    exit 1
fi

if [ ! -f "$DAT_GENERATOR_SCRIPT" ]; then 
    echo "ERROR: dat_generator.py not found at $DAT_GENERATOR_SCRIPT"
    exit 1
fi

if [ ! -f "$POST_PROCESSING_SCRIPT" ]; then 
    echo "ERROR: Python script not found at $POST_PROCESSING_SCRIPT"
    exit 1
fi

if [ ! -f "$SCALE_FACTOR_JSON_PATH" ]; then 
    echo "ERROR: Scale factor JSON not found at $SCALE_FACTOR_JSON_PATH"
    exit 1
fi

if [ ! -d "$CGMF_DEFAULT_DATA" ]; then 
    echo "ERROR: CGMF default data directory not found at $CGMF_DEFAULT_DATA"
    exit 1
fi

#====================================================
# 3. Execution Step 1: Generate Scaled Data Files
#====================================================

echo -e "\n===================================================="
echo ">> [$(date)] Step 1: Generating scaled data files"
echo "===================================================="
start_time_datgen=$(date +%s)

# Call dat_generator.py (your existing tested component)
time python "$DAT_GENERATOR_SCRIPT" \
    "$OUTPUT_SCALED_DIR" \
     "-$TARGET_ID" \
    --source-dir "$CGMF_DEFAULT_DATA" \
    --scales-json "$SCALE_FACTOR_JSON_PATH"

if [ $? -ne 0 ]; then
    echo "ERROR: dat_generator.py failed"
    exit 1
fi

end_time_datgen=$(date +%s)
duration_datgen=$((end_time_datgen - start_time_datgen))

#====================================================
# 4. Execution Step 2: Run CGMF Simulation
#====================================================

echo -e "\n===================================================="
echo ">> [$(date)] Step 2: Running CGMF Simulation ($EVENTS events)"
echo "===================================================="
start_time_sim=$(date +%s)

# Run CGMF
time "$CGMF_EXE" -d "$OUTPUT_SCALED_DIR" -i $TARGET_ID -e $INCIDENT_E -n $EVENTS

if [ $? -ne 0 ]; then
    echo "ERROR: CGMF simulation failed"
    exit 1
fi

end_time_sim=$(date +%s)
duration_sim=$((end_time_sim - start_time_sim))

#====================================================
# 5. Execution Step 3: Rename Output
#====================================================

echo -e "\n===================================================="
echo ">> [$(date)] Step 3: Renaming output files"
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
duration_mv=$((end_time_mv - start_time_mv))

#====================================================
# 6. Execution Step 4: Python Analysis
#====================================================

echo -e "\n===================================================="
echo ">> [$(date)] Step 4: Processing data with Python"
echo "===================================================="
start_time_py=$(date +%s)

# Run the python script
time python3 "$POST_PROCESSING_SCRIPT" "$NEW_HIST_NAME" --output "$PLOT_NAME"

if [ $? -ne 0 ]; then
    echo "ERROR: Post-processing script failed"
    exit 1
fi

end_time_py=$(date +%s)
duration_py=$((end_time_py - start_time_py))

#====================================================
# 7. Final Summary
#====================================================

echo -e "\n===================================================="
echo "   WORKFLOW SUMMARY"
echo "===================================================="
echo "   Data Generation Time: $duration_datgen sec"
echo "   Simulation Time:      $duration_sim sec"
echo "   File Rename Time:     $duration_mv sec"
echo "   Analysis Time:        $duration_py sec"
echo "   Total Time:           $((duration_datgen + duration_sim + duration_mv + duration_py)) sec"
echo "===================================================="
echo "   Generated Scaled Dir: $OUTPUT_SCALED_DIR"
echo "   Generated File:       $NEW_HIST_NAME"
echo "   Generated Plot:       $PLOT_NAME"
echo "   Completed at:         $(date)"
echo "===================================================="
