#!/usr/bin/env python3
"""
Orchestrate sampling study: generate sampled configs, populate template, submit array job
"""
import argparse
import csv
import subprocess
from pathlib import Path
import sys
import re
from types import SimpleNamespace

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from cgmf_uq.slurm.SLURM_Single_Job_Generator import SlurmScriptGenerator
from cgmf_uq.io.generate_sampling_json import generate_sampling


def generate_configurations(
    registry_path: Path,
    sampling_path: Path,
    output_dir: Path,
    project_dir: Path,
    cgmf_default_data: Path,
    target_id: int,
) -> Path:
    print("[1/5] Generating sampling configurations...")
    args_ns = SimpleNamespace(
        registry=registry_path,
        sampling=sampling_path,
        output=output_dir,
        cgmf_default_data=cgmf_default_data,
        target_id=target_id,
        force=True,
    )

    try:
        generate_sampling(args_ns)
    except Exception as exc:
        print("ERROR: Configuration generation failed")
        print(f"{exc}")
        sys.exit(1)

    manifest_path = output_dir / "manifest.csv"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not created: {manifest_path}")
    return manifest_path


def validate_configurations(manifest_path: Path) -> int:
    print("\n[2/5] Validating configurations...")
    if not manifest_path.exists():
        print(f"✗ Manifest missing: {manifest_path}")
        sys.exit(1)

    total = 0
    valid = True
    try:
        with open(manifest_path, "r") as f:
            reader = csv.DictReader(f)
            required_cols = {"task_id", "config_file"}
            if not required_cols.issubset(reader.fieldnames or []):
                print(f"✗ Manifest missing required columns: {required_cols}")
                print(f"  Found columns: {reader.fieldnames}")
                sys.exit(1)
            for row in reader:
                task_id = row.get("task_id", "?")
                config_file = row.get("config_file", "")
                config_path = Path(config_file)
                if not config_path.is_absolute():
                    print(f"✗ Config path is not absolute for task {task_id}: {config_path}")
                    valid = False
                    total += 1
                    continue
                if not config_path.exists():
                    print(f"✗ Config missing for task {task_id}: {config_path}")
                    valid = False
                total += 1
    except Exception as e:
        print(f"✗ Error reading manifest: {e}")
        sys.exit(1)

    if not valid or total == 0:
        print("ERROR: Manifest validation failed. See errors above.")
        sys.exit(1)

    print(f"✓ {total} configurations validated")
    return total


def calculate_resources(total_tasks: int, max_concurrent: int, events: int, time_limit: str) -> None:
    print("\n[3/5] Resource allocation:")
    print(f"  Total tasks:       {total_tasks}")
    print(f"  Max concurrent:    {max_concurrent} cores")
    print(f"  Events per task:   {events:,}")
    print(f"  Time per task:     {time_limit}")
    batches = (total_tasks + max_concurrent - 1) // max_concurrent
    print(f"  Sequential batches: ~{batches}")


def generate_slurm_script(
    template_path: Path,
    output_dir: Path,
    manifest_path: Path,
    total_tasks: int,
    config: dict,
) -> Path:
    print("\n[4/5] Generating SLURM job script...")

    generator = SlurmScriptGenerator(template_path)

    variables = {
        "JOB_NAME": config["job_name"],
        "PARTITION": config["partition"],
        "TIME_LIMIT": config["time_limit"],
        "MAX_TASK_ID": str(total_tasks - 1),
        "MAX_CONCURRENT": str(config["max_concurrent"]),
        "LOG_DIR": str((output_dir / "logs").resolve()),
        "PROJECT_DIR": str(Path(config["project_dir"]).resolve()),
        "MANIFEST": str(Path(manifest_path).resolve()),
        "OUTPUT_BASE": str((output_dir / "runs").resolve()),
        "CGMF_ROOT": str(Path(config["cgmf_root"]).resolve()),
        "CGMF_DEFAULT_DATA": str(Path(config["cgmf_default_data"]).resolve()),
        "POST_PROCESSOR": str(Path(config["post_processor"]).resolve()),
        "CONDA_ROOT": str(Path(config["conda_root"]).resolve()),
        "CONDA_ENV": config["conda_env"],
        "EVENTS": str(config["events"]),
        "TARGET_ID": str(config["target_id"]),
        "INCIDENT_E": str(config["incident_e"]),
    }

    script_path = output_dir / f"{config['job_name']}.sbatch"
    generator.generate(script_path, variables)

    print(f"✓ SLURM script generated: {script_path}")
    return script_path


def submit_job(script_path: Path, dry_run: bool) -> None:
    print("\n[5/5] Submission:")

    if dry_run:
        print("DRY RUN - Script generated but not submitted")
        print("\nTo submit manually:")
        print(f"  sbatch {script_path}")
        return

    print("Submitting to SLURM...")
    result = subprocess.run(
        ["sbatch", str(script_path)],
        capture_output=True,
        text=True,
        cwd=script_path.parent,
    )

    if result.returncode == 0:
        print(result.stdout.strip())
        print("✓ Job submitted successfully")

        match = re.search(r"Submitted batch job (\d+)", result.stdout)
        if match:
            job_id = match.group(1)
            print("\nMonitoring commands:")
            print(f"  squeue -j {job_id}")
            print(f"  tail -f {script_path.parent}/logs/*_{job_id}_0.out")
    else:
        print("ERROR: Submission failed")
        print(result.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Generate and submit CGMF sampling study as SLURM array job",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--registry", required=True, help="Path to Parameter_Registry.yaml")
    parser.add_argument("--sampling", required=True, help="Path to Sampling_Config.yaml")
    parser.add_argument("--output", required=True, help="Directory where results/logs will be saved")

    parser.add_argument(
        "--project-dir",
        default=str(PROJECT_ROOT),
        help="Root directory of the project",
    )

    parser.add_argument(
        "--cgmf-root",
        help="Location of CGMF installation",
        required=True,
    )

    parser.add_argument(
        "--conda-root",
        help="Root of Conda install (containing etc/profile.d/conda.sh)",
        required=True,
    )

    parser.add_argument(
        "--cgmf-default-data",
        help="Path to clean/unperturbed CGMF data",
        required=True,
    )

    parser.add_argument(
        "--post-processor",
        help="Path to analysis Python script",
        required=True,
    )

    parser.add_argument("--events", type=int, default=5000, help="CGMF events per task")
    parser.add_argument("--target-id", type=int, default=92235, help="Target ZAID (e.g. 92235)")
    parser.add_argument("--incident-e", type=float, default=0.0, help="Incident energy (eV)")
    parser.add_argument("--job-name", default="cgmf_sample", help="Name in SLURM queue")
    parser.add_argument("--conda-env", default="cgmf_py", help="Conda environment name")

    parser.add_argument("--max-concurrent", type=int, default=50, help="Max simultaneous tasks")
    parser.add_argument("--partition", default="serial", help="Queue partition name")
    parser.add_argument("--time-limit", default="04:00:00", help="Time limit (HH:MM:SS)")

    parser.add_argument("--submit", action="store_true", help="Actually submit the job (default is dry-run)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output directory")

    args = parser.parse_args()

    project_dir = Path(args.project_dir)
    output_dir = Path(args.output)

    if output_dir.exists() and not args.force:
        print(f"ERROR: Output directory exists: {output_dir}")
        print("Use --force to overwrite")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "logs").mkdir(exist_ok=True)
    (output_dir / "runs").mkdir(exist_ok=True)

    template_path = project_dir / "cgmf_uq" / "slurm" / "sampling_job_template.sh"
    if not template_path.exists():
        print(f"ERROR: Template not found at {template_path}")
        sys.exit(1)

    print("=" * 60)
    print(f"CGMF ORCHESTRATOR | {args.job_name}")
    print("=" * 60)

    manifest_path = generate_configurations(
        Path(args.registry),
        Path(args.sampling),
        output_dir,
        project_dir,
        Path(args.cgmf_default_data),
        args.target_id,
    )
    total_tasks = validate_configurations(manifest_path)

    calculate_resources(total_tasks, args.max_concurrent, args.events, args.time_limit)

    config_dict = {
        "job_name": args.job_name,
        "partition": args.partition,
        "time_limit": args.time_limit,
        "max_concurrent": args.max_concurrent,
        "project_dir": project_dir,
        "cgmf_root": args.cgmf_root,
        "cgmf_default_data": args.cgmf_default_data,
        "post_processor": args.post_processor,
        "conda_root": args.conda_root,
        "conda_env": args.conda_env,
        "events": args.events,
        "target_id": args.target_id,
        "incident_e": args.incident_e,
    }

    script_path = generate_slurm_script(template_path, output_dir, manifest_path, total_tasks, config_dict)

    submit_job(script_path, not args.submit)


if __name__ == "__main__":
    main()
