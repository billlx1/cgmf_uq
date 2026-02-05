#!/usr/bin/env python3
"""
Dry-run test for manifest validation in scripts/submit_sensitivity.py.
"""
from __future__ import annotations

import importlib.util
import io
import json
import sys
import tempfile
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _load_submit_module():
    script_path = PROJECT_ROOT / "scripts" / "submit_sensitivity.py"
    spec = importlib.util.spec_from_file_location("submit_sensitivity", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_manifest(path: Path, rows: list[tuple[str, str, str, str]]) -> None:
    lines = ["task_id,parameter,scale,config_file"]
    for task_id, parameter, scale, config_file in rows:
        lines.append(f"{task_id},{parameter},{scale},{config_file}")
    path.write_text("\n".join(lines))


def run() -> None:
    module = _load_submit_module()
    validate_configurations = module.validate_configurations

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        configs_dir = tmp_dir / "configs"
        configs_dir.mkdir()

        rel_cfg = configs_dir / "rel.json"
        abs_cfg = configs_dir / "abs.json"
        rel_cfg.write_text(json.dumps({"ok": True}))
        abs_cfg.write_text(json.dumps({"ok": True}))

        manifest_path = tmp_dir / "manifest.txt"
        rows = [
            ("0", "param_a", "1.0", "configs/rel.json"),
            ("1", "param_b", "0.9", str(abs_cfg)),
        ]
        _write_manifest(manifest_path, rows)

        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            total = validate_configurations(manifest_path)
        if total != len(rows):
            raise AssertionError(f"Expected {len(rows)} tasks, got {total}")

        abs_cfg.unlink()
        failure_out = io.StringIO()
        with redirect_stdout(failure_out), redirect_stderr(failure_out):
            try:
                validate_configurations(manifest_path)
            except SystemExit as exc:
                if exc.code != 1:
                    raise AssertionError(f"Expected exit code 1, got {exc.code}") from exc
            else:
                raise AssertionError("Expected validation failure for missing config file")
        output = failure_out.getvalue()
        if "Config missing for task 1" not in output:
            raise AssertionError("Missing expected 'Config missing' message in output")
        if "Manifest validation failed" not in output:
            raise AssertionError("Missing expected failure summary in output")

    print("Manifest validation test: OK")


if __name__ == "__main__":
    run()
