# Experiments

This repository now includes a benchmark harness for the paper experiments under [`experiments/`](./experiments).

The runner is built around the current `PredictiveVaranus` architecture and reuses the same core functions already used by [`monitor.py`](./monitor.py):

- `run_varanus_buchi(...)`
- `project_hoa_file(...)`
- `project_ltl_formula(...)`
- `PredictiveRuntime(...)`
- `connect_varanus_ws(...)`
- `gate_with_varanus(...)`
- `extract_parsed_event(...)`
- `resolve_projected_event(...)`
- `normalize_gate_verdict(...)`

It supports:

- rover case-study runs
- dense synthetic cost runs
- decision-tail synthetic benefit runs
- isolated worker-process execution for measured runs
- raw CSV event logs
- suite summaries
- paper-table CSVs
- plot-ready aggregate CSVs
- plot generation with `matplotlib`

## Files Added

- [`experiments/run_benchmarks.py`](./experiments/run_benchmarks.py): main entry point
- [`experiments/run_rover_evaluation.py`](./experiments/run_rover_evaluation.py): rover case-study driver for Table `tab:rover-eval`
- [`experiments/run_stress_test_evaluation.py`](./experiments/run_stress_test_evaluation.py): synthetic stress-test driver for the stress-test subsection
- [`experiments/benchmark_lib.py`](./experiments/benchmark_lib.py): generators, worker logic, aggregation, plotting

## Prerequisites

You need the same runtime stack as the main monitor, plus `matplotlib` if you want the PNG plots:

```bash
python3 - <<'PY'
import spot, buddy, websockets
print("core dependencies ok")
PY
```

Optional plot dependency:

```bash
python3 - <<'PY'
import matplotlib
print("matplotlib ok")
PY
```

You also need a working Varanus checkout and a Python executable that matches its FDR setup.

## What Gets Generated

`prepare-inputs` creates:

- rover trace files under `experiments/generated/rover/traces/`
- a benchmark-specific rover config with absolute model paths
- dense synthetic CSP models/configs
- decision-tail synthetic CSP models/configs
- `experiments/generated/manifest.json`

Measured runs write results under `results/`:

- `rover_event_log.csv`
- `rover_summary.csv`
- `table_rover_eval.csv`
- `dense_event_log.csv`
- `dense_summary.csv`
- `decision_tail_event_log.csv`
- `decision_tail_summary.csv`
- `table_stress_test_costs.csv`
- `table_stress_test_benefit.csv`
- `stress_test_setup.json`
- `stress_test_setup.csv`
- `plots_export_vs_states.csv`
- `plots_gate_cost_vs_trace_length.csv`
- `plots_pred_cost_vs_trace_length.csv`
- `plots_gain_vs_tail_length.csv`
- `plots_gain_vs_branching.csv`
- `paper_rover_table.csv`
- `paper_stress_cost_table.csv`
- `paper_stress_benefit_table.csv`
- `benchmark_metadata.json`
- `results/plots/*.png` if plotting is enabled and `matplotlib` is installed

## Recommended Commands

Replace `/path/to/varanus.py` and, if needed, `python3.8` with the Varanus/FDR-compatible values on your machine.

### 1. Generate Benchmark Inputs

```bash
python3 -m experiments.run_benchmarks prepare-inputs \
  --varanus-script /path/to/varanus.py \
  --varanus-python python3.8
```

### 2. Run The Rover Evaluation

This is the paper-facing command for Table `tab:rover-eval`:

```bash
python3 -m experiments.run_rover_evaluation \
  --varanus-script /path/to/varanus.py \
  --varanus-python python3.8
```

The table-ready CSV is:

- `results/table_rover_eval.csv`

### 3. Run The Stress-Test Evaluation

This is the paper-facing command for the full stress-test subsection:

```bash
python3 -m experiments.run_stress_test_evaluation \
  --varanus-script /path/to/varanus.py \
  --varanus-python python3.8
```

The main subsection outputs are:

- `results/table_stress_test_costs.csv`
- `results/table_stress_test_benefit.csv`
- `results/stress_test_setup.json`
- `results/stress_test_setup.csv`
- plot-ready aggregate CSVs and generated plots

### 4. Run Everything

```bash
python3 -m experiments.run_benchmarks run-all \
  --varanus-script /path/to/varanus.py \
  --varanus-python python3.8
```

That command:

- regenerates derived CSVs
- runs rover, dense, and decision-tail suites
- records per-event and preprocessing timings separately
- writes the paper-table CSVs
- generates plots unless `--skip-plots` is set

### 5. Run a Single Internal Suite

Rover only:

```bash
python3 -m experiments.run_benchmarks run-rover \
  --varanus-script /path/to/varanus.py \
  --varanus-python python3.8
```

Dense cost suite only:

```bash
python3 -m experiments.run_benchmarks run-dense \
  --varanus-script /path/to/varanus.py \
  --varanus-python python3.8
```

Decision-tail benefit suite only:

```bash
python3 -m experiments.run_benchmarks run-decision-tail \
  --varanus-script /path/to/varanus.py \
  --varanus-python python3.8
```

### 6. Rebuild Derived CSVs and Plots Without Rerunning Measurements

```bash
python3 -m experiments.run_benchmarks build-plots
```

## Default Parameter Grid

Dense family:

- `N = 10, 50, 100, 200, 500, 1000`
- `L = 100, 1000, 10000, 100000`
- warm-up seeds: `-2, -1`
- measured seeds: `0..9`

Decision-tail family:

- `B = 2, 4, 8`
- `D = 4, 6, 8`
- `T = 1, 5, 10, 20`
- trace seeds: `0..19`
- warm-up seeds: `-2, -1`
- measured seeds: `0..9`

Implementation note:

- the decision-tail generator uses a compact CSP encoding of the intended trace family, so it preserves the controlled choice/commit/tail behaviour needed for predictive-gain measurements without textually expanding the full `B^D` tree in the source file

You can override any of these from the command line, for example:

```bash
python3 -m experiments.run_benchmarks run-dense \
  --varanus-script /path/to/varanus.py \
  --varanus-python python3.8 \
  --dense-sizes 10,50,100,200 \
  --dense-lengths 100,1000,10000
```

## Notes On Methodology

- Rover and decision-tail suites use stop-on-conclusion mode.
- Dense runs use full-trace mode.
- Each measured run is executed in a fresh worker process.
- Warm-up runs are executed and discarded.
- Peak RSS is collected with `resource.getrusage(...).ru_maxrss`.
- Synthetic traces are generated deterministically from fixed seeds at run time.

## Rover Trace Inputs

The harness generates these rover traces:

- `rover_nominal_green.trace`
- `rover_red_abort.trace`
- `rover_orange_abort.trace`
- `rover_red_continue_invalid.trace`
- `rover_mismatched_move_invalid.trace`
- optional `rover_abort_after_k.trace` for `k = 1..4`

These are stored in `experiments/generated/rover/traces/`.

## Important Assumption

The default rover traces generated here follow the event naming and control-flow pattern currently present in `rover_model3.csp`. If your final paper run uses a slightly different rover CSP asset from the earlier Varanus evaluation, regenerate or edit the rover traces before the final measurement run.

The benchmark harness itself does not depend on the rover example being exactly these defaults; it only needs:

- a Varanus config
- a trace file or synthetic trace generator
- a property formula

## Internal Worker Entry Point

The `run-one` subcommand is internal and used automatically by `run-all` / `run-rover` / `run-dense` / `run-decision-tail`. It should not normally be invoked directly.
