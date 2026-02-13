#!/bin/bash
#SBATCH --job-name={{JOB_NAME}}
#SBATCH --partition={{PARTITION}}
#SBATCH --time={{TIME_LIMIT}}
#SBATCH --array=0-{{MAX_TASK_ID}}%{{MAX_CONCURRENT}}
#SBATCH --output={{LOG_DIR}}/{{JOB_NAME}}_%A_%a.out
#SBATCH --error={{LOG_DIR}}/{{JOB_NAME}}_%A_%a.err

# Extract config path and metadata from manifest
MANIFEST_LINE=$(awk -F',' -v id="${SLURM_ARRAY_TASK_ID}" '$1==id' {{MANIFEST}})
PARAM_NAME=$(echo "$MANIFEST_LINE" | cut -d',' -f2)
SCALE=$(echo "$MANIFEST_LINE" | cut -d',' -f3)
CONFIG_PATH=$(echo "$MANIFEST_LINE" | cut -d',' -f4)

echo "=========================================="
echo "CGMF Sensitivity Run"
echo "Task ID:    ${SLURM_ARRAY_TASK_ID}"
echo "Parameter:  ${PARAM_NAME}"
echo "Scale:      ${SCALE}"
echo "Config:     ${CONFIG_PATH}"
echo "=========================================="
#====================================================
# CGMF Sensitivity Study - Array Job
# Generated: {{TIMESTAMP}}
#====================================================

set -e  # Exit on any error
set -u  # Exit on undefined variable

#----------------------------------------------------
# Configuration (injected by orchestrator)
#----------------------------------------------------
PROJECT_DIR="{{PROJECT_DIR}}"
MANIFEST="{{MANIFEST}}"
OUTPUT_BASE="{{OUTPUT_BASE}}"
EVENTS={{EVENTS}}
TARGET_ID={{TARGET_ID}}
INCIDENT_E={{INCIDENT_E}}

CGMF_ROOT="{{CGMF_ROOT}}"
CGMF_EXE="$CGMF_ROOT/build/utils/cgmf/cgmf.x"
CGMF_DEFAULT_DATA="{{CGMF_DEFAULT_DATA}}"

DAT_GENERATOR="$PROJECT_DIR/cgmf_uq/io/dat_generator.py"
POST_PROCESSOR="{{POST_PROCESSOR}}"

CONDA_ROOT="{{CONDA_ROOT}}"
CONDA_ENV="{{CONDA_ENV}}"

#----------------------------------------------------
# Task Header
#----------------------------------------------------
echo "=========================================="
echo "Task ID: $SLURM_ARRAY_TASK_ID / {{MAX_TASK_ID}}"
echo "Job ID: $SLURM_ARRAY_JOB_ID"
echo "Node: $(hostname)"
echo "Started: $(date)"
echo "=========================================="

#----------------------------------------------------
# Environment Setup
#----------------------------------------------------
module purge
module load tools/gcc/git/2.24.0
module load apps/binapps/conda/miniforge3/25.9.1

# Explicit conda initialization (handles non-interactive shells)
#source "$CONDA_ROOT/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV"

if [ "$CONDA_DEFAULT_ENV" != "$CONDA_ENV" ]; then
    echo "ERROR: Failed to activate conda environment '$CONDA_ENV'"
    echo "Current environment: $CONDA_DEFAULT_ENV"
    exit 1
fi

#----------------------------------------------------
# Configuration Lookup (manifest-based)
#----------------------------------------------------
CONFIG_JSON=$(awk -F',' -v id="$SLURM_ARRAY_TASK_ID" '$1==id {print $4}' "$MANIFEST")

if [ -z "$CONFIG_JSON" ]; then
    echo "ERROR: Failed to retrieve config for task $SLURM_ARRAY_TASK_ID"
    echo "Manifest: $MANIFEST"
    exit 1
fi

if [ ! -f "$CONFIG_JSON" ]; then
    echo "ERROR: Config file does not exist: $CONFIG_JSON"
    exit 1
fi

echo "Config: $CONFIG_JSON"

# Create task-specific run directory
RUN_DIR="$OUTPUT_BASE/task_${SLURM_ARRAY_TASK_ID}"
mkdir -p "$RUN_DIR"
cd "$RUN_DIR" || exit 1

echo "Run directory: $(pwd)"

#====================================================
# Step 1: Generate Perturbed .dat Files
#====================================================
echo ""
echo ">> [$(date)] Step 1/4: Generating perturbed data files"
echo "----------------------------------------"
start_time_datgen=$(date +%s)

DAT_DIR="$RUN_DIR/dat_files"

python "$DAT_GENERATOR" \
    "$DAT_DIR" \
    "$TARGET_ID" \
    --source-dir "$CGMF_DEFAULT_DATA" \
    --scales-json "$CONFIG_JSON" 2>&1 | tee dat_generator.log

if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "ERROR: dat_generator failed"
    exit 1
fi

end_time_datgen=$(date +%s)
duration_datgen=$((end_time_datgen - start_time_datgen))
echo "✓ Data files generated (${duration_datgen}s)"

#====================================================
# Step 2: Run CGMF Simulation
#====================================================
echo ""
echo ">> [$(date)] Step 2/4: Running CGMF simulation"
echo "----------------------------------------"
echo "Events: $EVENTS"
echo "Target: $TARGET_ID"
echo "Incident Energy: $INCIDENT_E eV"
start_time_sim=$(date +%s)

HIST_FILE="histories.cgmf"

"$CGMF_EXE" -d "$DAT_DIR" -i "$TARGET_ID" -e "$INCIDENT_E" -n "$EVENTS" 2>&1 | tee cgmf.log

if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "ERROR: CGMF simulation failed"
    cat cgmf.log
    exit 1
fi

# Handle CGMF's .0 suffix quirk
if [ -f "${HIST_FILE}.0" ]; then
    mv "${HIST_FILE}.0" "$HIST_FILE"
elif [ ! -f "$HIST_FILE" ]; then
    echo "ERROR: CGMF did not produce history file"
    ls -lh
    exit 1
fi

end_time_sim=$(date +%s)
duration_sim=$((end_time_sim - start_time_sim))
HIST_SIZE=$(du -h "$HIST_FILE" | cut -f1)
echo "✓ History file: $HIST_FILE ($HIST_SIZE, ${duration_sim}s)"

#----------------------------------------------------
# Step 2-Post Selective Cleanup - Keep only perturbed files
#----------------------------------------------------
echo ""
echo ">> Cleaning up unmodified data files..."
if [ -d "$DAT_DIR" ]; then
    SIZE_BEFORE=$(du -sh "$DAT_DIR" 2>/dev/null | cut -f1 || echo "unknown")
    
    # Define the 7-9 critical files that might be perturbed
    # These correspond to your FILE_PARSERS
    CRITICAL_FILES=(
        "tkemodel.dat"
        "yamodel.dat"
        "kcksyst.dat"
        "deformations.dat"
        "rta.dat"
        "spinscalingmodel.dat"
        "gstrength_gdr_params.dat"
    )
    
    # Move to temporary location
    KEEP_DIR="${DAT_DIR}_keep"
    mkdir -p "$KEEP_DIR"
    
    # Copy only critical files
    FILES_KEPT=0
    for file in "${CRITICAL_FILES[@]}"; do
        if [ -f "$DAT_DIR/$file" ]; then
            cp -p "$DAT_DIR/$file" "$KEEP_DIR/"
            FILES_KEPT=$((FILES_KEPT + 1))
        fi
    done
    
    # Count before deletion
    FILES_TOTAL=$(find "$DAT_DIR" -name "*.dat" 2>/dev/null | wc -l)
    FILES_DELETED=$((FILES_TOTAL - FILES_KEPT))
    
    # Replace dat_files with kept files only
    rm -rf "$DAT_DIR"
    mv "$KEEP_DIR" "$DAT_DIR"
    
    SIZE_AFTER=$(du -sh "$DAT_DIR" 2>/dev/null | cut -f1 || echo "unknown")
    
    echo "✓ Selective cleanup complete"
    echo "  Kept:    $FILES_KEPT critical files"
    echo "  Removed: $FILES_DELETED unmodified files"
    echo "  Before:  $SIZE_BEFORE → After: $SIZE_AFTER"
else
    echo "⚠ No .dat directory found to clean"
fi



#====================================================
# Step 3: Post-Process Results
#====================================================
echo ""
echo ">> [$(date)] Step 3/4: Post-processing"
echo "----------------------------------------"
start_time_py=$(date +%s)

DATE_TAG=$(date +%Y%m%d_%H%M%S)
PLOT_NAME="analysis_${SLURM_ARRAY_TASK_ID}_${DATE_TAG}.png"

python3 "$POST_PROCESSOR" "$HIST_FILE" --output "$PLOT_NAME" 2>&1 | tee postproc.log

if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "ERROR: Post-processing failed"
    cat postproc.log
    exit 1
fi

end_time_py=$(date +%s)
duration_py=$((end_time_py - start_time_py))
echo "✓ Analysis complete (${duration_py}s)"

# Compress history file (saves ~90% space)
echo ">> Compressing history file..."
gzip -f "$HIST_FILE"
COMPRESSED_SIZE=$(du -h "${HIST_FILE}.gz" | cut -f1)
echo "✓ Compressed: ${HIST_FILE}.gz ($COMPRESSED_SIZE)"

#====================================================
# Step 4: Record Metadata
#====================================================
echo ""
echo ">> [$(date)] Step 4/4: Recording metadata"
echo "----------------------------------------"

# Extract parameter info from manifest (CSV format: task_id,parameter,scale,config_file)
PARAM_INFO=$(awk -F',' -v task="$SLURM_ARRAY_TASK_ID" '$1==task {print $2","$3}' "$MANIFEST")

TOTAL_DURATION=$((duration_datgen + duration_sim + duration_py))

cat > metadata.json <<EOF
{
  "task_id": $SLURM_ARRAY_TASK_ID,
  "job_id": "$SLURM_ARRAY_JOB_ID",
  "config_json": "$CONFIG_JSON",
  "parameter_info": "$PARAM_INFO",
  "events": $EVENTS,
  "target_id": $TARGET_ID,
  "incident_energy": $INCIDENT_E,
  "hostname": "$(hostname)",
  "started": "$(date -Iseconds)",
  "completed": "$(date -Iseconds)",
  "timings": {
    "dat_generation_s": $duration_datgen,
    "simulation_s": $duration_sim,
    "postprocessing_s": $duration_py,
    "total_s": $TOTAL_DURATION
  },
  "outputs": {
    "history_compressed": "${HIST_FILE}.gz",
    "history_size": "$COMPRESSED_SIZE",
    "plot": "$PLOT_NAME"
  }
}
EOF

echo "✓ Metadata saved"

#====================================================
# Final Summary
#====================================================
echo ""
echo "=========================================="
echo "✓ Task $SLURM_ARRAY_TASK_ID completed"
echo "=========================================="
echo "Data Generation:  ${duration_datgen}s"
echo "Simulation:       ${duration_sim}s"
echo "Post-processing:  ${duration_py}s"
echo "Total Duration:   ${TOTAL_DURATION}s"
echo "=========================================="
