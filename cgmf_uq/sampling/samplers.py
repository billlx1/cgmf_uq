"""
Sampling registry and built-in samplers for Phase II.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Callable, Any
import importlib.util
import json

import numpy as np


@dataclass
class SamplerContext:
    parameters: List[str]
    value_space: str  # "scale" or "absolute"
    params: Dict[str, Any]


class BaseSampler:
    def __init__(self, ctx: SamplerContext):
        self.ctx = ctx

    def sample(self, n: int, rng: np.random.Generator) -> Dict[str, np.ndarray]:
        raise NotImplementedError


class IndependentGaussianSampler(BaseSampler):
    def sample(self, n: int, rng: np.random.Generator) -> Dict[str, np.ndarray]:
        params = self.ctx.params or {}
        stddev = params.get("stddev", None)
        mean = params.get("mean", None)

        if stddev is None:
            raise ValueError("independent_gaussian requires 'stddev' in params")

        stds = _expand_param_vector(stddev, self.ctx.parameters, "stddev")

        if mean is None:
            means = np.ones(len(self.ctx.parameters), dtype=float)
        else:
            means = _expand_param_vector(mean, self.ctx.parameters, "mean")

        out: Dict[str, np.ndarray] = {}
        for i, p in enumerate(self.ctx.parameters):
            out[p] = rng.normal(loc=means[i], scale=stds[i], size=n)
        return out


class MVNCholeskySampler(BaseSampler):
    def sample(self, n: int, rng: np.random.Generator) -> Dict[str, np.ndarray]:
        params = self.ctx.params or {}
        json_file = params.get("json_file")
        mu_file = params.get("mu_file")
        chol_file = params.get("chol_file")
        mu_key = params.get("mu_key")
        chol_key = params.get("chol_key")

        if json_file:
            mu, chol, labels = _load_mvn_json(json_file)
            if labels is not None and labels != self.ctx.parameters:
                raise ValueError("mvn_cholesky param_labels do not match parameters list")
        else:
            if not mu_file or not chol_file:
                raise ValueError("mvn_cholesky requires 'json_file' or both 'mu_file' and 'chol_file'")
            mu = _load_array(mu_file, key=mu_key)
            chol = _load_array(chol_file, key=chol_key)

        if mu.ndim != 1:
            raise ValueError("mvn_cholesky mu must be 1D")
        if chol.ndim != 2:
            raise ValueError("mvn_cholesky chol must be 2D")
        if chol.shape[0] != chol.shape[1]:
            raise ValueError("mvn_cholesky chol must be square")
        if mu.shape[0] != chol.shape[0]:
            raise ValueError("mvn_cholesky mu/chol size mismatch")
        if mu.shape[0] != len(self.ctx.parameters):
            raise ValueError("mvn_cholesky size does not match parameters list")

        z = rng.standard_normal(size=(len(self.ctx.parameters), n))
        samples = (mu[:, None] + chol @ z).T  # (n, p)
        return {p: samples[:, i] for i, p in enumerate(self.ctx.parameters)}


class CustomSampler(BaseSampler):
    def sample(self, n: int, rng: np.random.Generator) -> Dict[str, np.ndarray]:
        params = self.ctx.params or {}
        module_path = params.get("custom_module")
        func_name = params.get("custom_function", "sample")
        if not module_path:
            raise ValueError("custom sampler requires 'custom_module' in params")

        module = _load_module_from_path(Path(module_path))
        if not hasattr(module, func_name):
            raise ValueError(f"custom module missing function '{func_name}'")

        func: Callable[..., Any] = getattr(module, func_name)
        result = func(n=n, rng=rng, parameters=self.ctx.parameters, params=params)

        if isinstance(result, dict):
            return {k: np.asarray(v) for k, v in result.items()}

        arr = np.asarray(result)
        if arr.ndim != 2 or arr.shape[1] != len(self.ctx.parameters):
            raise ValueError("custom sampler must return dict or (n, p) array")
        return {p: arr[:, i] for i, p in enumerate(self.ctx.parameters)}


SAMPLER_REGISTRY: Dict[str, Callable[[SamplerContext], BaseSampler]] = {
    "independent_gaussian": IndependentGaussianSampler,
    "mvn_cholesky": MVNCholeskySampler,
    "custom": CustomSampler,
}


def build_sampler(sampler_name: str, ctx: SamplerContext) -> BaseSampler:
    if sampler_name not in SAMPLER_REGISTRY:
        raise ValueError(f"Unknown sampler '{sampler_name}'")
    return SAMPLER_REGISTRY[sampler_name](ctx)


def _expand_param_vector(value: Any, parameters: List[str], label: str) -> np.ndarray:
    if isinstance(value, (int, float)):
        return np.full(len(parameters), float(value), dtype=float)
    if isinstance(value, list):
        if len(value) != len(parameters):
            raise ValueError(f"{label} length must match parameters")
        return np.array(value, dtype=float)
    if isinstance(value, dict):
        return np.array([float(value[p]) for p in parameters], dtype=float)
    raise ValueError(f"Unsupported {label} format")


def _load_array(path: str, key: str | None = None) -> np.ndarray:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Array file not found: {p}")
    if p.suffix == ".npy":
        return np.load(p)
    if p.suffix == ".npz":
        data = np.load(p)
        if key is None:
            if len(data.files) != 1:
                raise ValueError(f"NPZ file has multiple arrays; specify key for {p}")
            key = data.files[0]
        if key not in data:
            raise ValueError(f"NPZ key '{key}' not found in {p}")
        return data[key]
    if p.suffix == ".json":
        with open(p, "r") as f:
            return np.array(json.load(f), dtype=float)
    if p.suffix in {".txt", ".csv"}:
        return np.loadtxt(p, dtype=float, delimiter="," if p.suffix == ".csv" else None)
    raise ValueError(f"Unsupported array file extension: {p.suffix}")

def _load_mvn_json(path: str) -> tuple[np.ndarray, np.ndarray, list[str] | None]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"MVN JSON file not found: {p}")
    text = p.read_text(encoding="utf-8").strip()
    if not text:
        raise ValueError(f"MVN JSON file is empty: {p}")
    data = json.loads(text)
    required = {"description", "n_params", "param_labels", "gauss_mu", "gauss_chol"}
    if not required.issubset(set(data.keys())):
        missing = required - set(data.keys())
        raise ValueError(f"MVN JSON missing keys: {sorted(missing)}")
    mu = np.array(data["gauss_mu"], dtype=float)
    chol = np.array(data["gauss_chol"], dtype=float)
    labels = list(data["param_labels"])
    n_params = int(data["n_params"])
    if mu.shape[0] != n_params or chol.shape[0] != n_params or chol.shape[1] != n_params:
        raise ValueError("MVN JSON dimensions do not match n_params")
    return mu, chol, labels

def _load_module_from_path(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Custom module not found: {path}")
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Failed to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
