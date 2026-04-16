# Artifact Guide

This artifact contains the PredictiveVaranus monitor and the benchmark harness
used to generate the rover and synthetic evaluation data.

## Contents

- `monitor.py`: online/offline PredictiveVaranus monitor.
- `predictive_ltl.py`: predictive LTL runtime.
- `hoa_projection.py`: HOA and LTL formula projection utilities.
- `experiments/`: benchmark generators, runners, aggregation, and plotting.
- `README_experiments.md`: detailed experiment commands and output schema.

Generated benchmark inputs and outputs are not committed. They are reproducible
from the tracked scripts and are ignored by Git.

## Dependencies

The artifact expects:

- Linux.
- Python 3 with `spot`, `buddy`, and `websockets`.
- A Varanus checkout containing `varanus.py`.
- A Python executable compatible with the Varanus/FDR installation.
- `matplotlib` if PNG plots are required.

Quick dependency check:

```bash
python3 - <<'PY'
import spot, buddy, websockets
print("core dependencies ok")
PY
```

Optional plotting check:

```bash
python3 - <<'PY'
import matplotlib
print("matplotlib ok")
PY
```

## Reproducing Inputs

Generate deterministic benchmark inputs:

```bash
python3 -m experiments.run_benchmarks prepare-inputs \
  --varanus-script /path/to/varanus.py \
  --varanus-python /path/to/python-for-varanus \
  --refresh-inputs
```

This writes generated CSP models, Varanus configs, rover traces, and the
manifest under `experiments/generated/`.

## Smoke Run

Use this command to validate the full pipeline quickly:

```bash
python3 -m experiments.run_benchmarks run-all \
  --results-dir /tmp/pv-smoke-results \
  --suite-order decision_tail,rover,dense \
  --varanus-script /path/to/varanus.py \
  --varanus-python /path/to/python-for-varanus \
  --refresh-inputs \
  --dense-sizes 10,50 \
  --dense-lengths 100 \
  --warmup-seeds=-1 \
  --measured-seeds=0 \
  --decision-branching 2 \
  --decision-depths 4 \
  --decision-tails 1,5 \
  --decision-trace-seeds=0 \
  --keep-worker-artifacts
```

Expected high-level outcome:

- rover summaries are produced;
- dense runs remain inconclusive on the selected cost formula;
- decision-tail runs conclude at the commitment event and show positive
  anticipation gain;
- CSV outputs are written under `/tmp/pv-smoke-results`.

## Paper-Scale Runs

The full default grid is intentionally large. For a bounded but informative
paper-draft run, use:

```bash
python3 -m experiments.run_benchmarks run-all \
  --results-dir /tmp/pv-paper-medium \
  --suite-order decision_tail,rover,dense \
  --varanus-script /path/to/varanus.py \
  --varanus-python /path/to/python-for-varanus \
  --refresh-inputs \
  --dense-sizes 10,50,100,200 \
  --dense-lengths 100,1000,10000 \
  --warmup-seeds=-1 \
  --measured-seeds=0,1,2 \
  --decision-branching 2,4,8 \
  --decision-depths 4,6 \
  --decision-tails 1,5,10,20 \
  --decision-trace-seeds=0,1,2,3,4
```

The unrestricted default run is:

```bash
python3 -m experiments.run_benchmarks run-all \
  --varanus-script /path/to/varanus.py \
  --varanus-python /path/to/python-for-varanus \
  --refresh-inputs
```

Use the unrestricted run only when the machine can be left running for a long
time.

## Main Outputs

Each run writes CSVs under the selected results directory:

- `rover_summary.csv` and `table_rover_eval.csv`.
- `dense_summary.csv` and `table_stress_test_costs.csv`.
- `decision_tail_summary.csv` and `table_stress_test_benefit.csv`.
- `benchmark_metadata.json`.
- `plots/*.png` when plotting is enabled and `matplotlib` is available.

The raw per-event logs are:

- `rover_event_log.csv`.
- `dense_event_log.csv`.
- `decision_tail_event_log.csv`.

## Methodological Notes

- Varanus provides the CSP conformance gate.
- PredictiveRuntime provides the predictive LTL verdict.
- Rover and decision-tail suites stop at the first conclusive verdict.
- Dense runs process the full trace to measure per-event cost.
- Predictive benefit is measured against the terminal reference event in the
  trace, not against a separately implemented reactive LTL monitor.
- The implementation uses `tick` for terminal stuttering. If the paper uses
  `skip` notation, it denotes the same proposition.

## Debugging

Add `--keep-worker-artifacts` to preserve worker specs, logs, and scratch data
under `<results-dir>/.tmp/`.

If a worker fails, the runner prints a `rerun_command`. Use it to reproduce the
exact failing run after inspecting the preserved logs.
