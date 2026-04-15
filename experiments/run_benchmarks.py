import argparse
import json
import shutil
import sys
from pathlib import Path


if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


from experiments.benchmark_lib import (
    DEFAULT_DECISION_BRANCHING,
    DEFAULT_DECISION_DEPTHS,
    DEFAULT_DECISION_TAILS,
    DEFAULT_DENSE_LENGTHS,
    DEFAULT_DENSE_SIZES,
    DEFAULT_GENERATED_DIR,
    DEFAULT_MEASURED_SEEDS,
    DEFAULT_RESULTS_DIR,
    DEFAULT_TMP_DIR,
    DEFAULT_WARMUP_SEEDS,
    EVENT_LOG_FIELDS,
    REPO_ROOT,
    SUMMARY_FIELDS,
    append_csv_rows,
    build_decision_tail_run_specs,
    build_dense_run_specs,
    build_rover_run_specs,
    cleanup_worker_artifacts,
    collect_worker_outputs,
    ensure_dir,
    generate_plots,
    machine_metadata,
    prepare_inputs,
    read_csv_rows,
    read_json,
    run_worker,
    run_worker_subprocess,
    suite_result_paths,
    suite_runs,
    write_csv,
    write_json,
    write_plot_and_table_csvs,
)


RUNNER_MODULE = "experiments.run_benchmarks"


def parse_int_list(value):
    if value in (None, ""):
        return None
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def build_manifest(args):
    return prepare_inputs(
        generated_dir=args.generated_dir,
        rover_config_path=args.rover_config,
        rover_model_path=args.rover_model,
        dense_sizes=args.dense_sizes,
        decision_branching=args.decision_branching,
        decision_depths=args.decision_depths,
        decision_tails=args.decision_tails,
    )


def manifest_path(generated_dir):
    return Path(generated_dir) / "manifest.json"


def resolve_executable(executable):
    executable = str(executable)
    if "/" in executable:
        path = Path(executable).expanduser()
        if path.exists():
            return str(path.resolve())
        return None
    return shutil.which(executable)


def validate_runtime_paths(args, parser):
    resolved_varanus_python = resolve_executable(args.varanus_python)
    if not resolved_varanus_python:
        parser.error(
            "The configured Varanus Python executable was not found: {value}. "
            "Install it or pass a valid interpreter via --varanus-python.".format(value=args.varanus_python)
        )
    args.varanus_python = resolved_varanus_python

    script_path = Path(args.varanus_script).expanduser()
    if not script_path.is_absolute():
        script_path = (Path.cwd() / script_path).resolve()
    if not script_path.exists():
        parser.error(
            "The configured Varanus script was not found: {value}. "
            "Pass the path to varanus.py via --varanus-script.".format(value=args.varanus_script)
        )
    args.varanus_script = str(script_path)
    return args


def load_or_prepare_manifest(args):
    path = manifest_path(args.generated_dir)
    if args.command == "prepare-inputs" or args.refresh_inputs or not path.exists():
        return build_manifest(args)
    return read_json(path)


def suite_specs(manifest, suite_id, args):
    if suite_id == "rover":
        return build_rover_run_specs(manifest)
    if suite_id == "dense":
        return build_dense_run_specs(
            manifest,
            trace_lengths=args.dense_lengths,
            warmup_seeds=args.warmup_seeds,
            measured_seeds=args.measured_seeds,
        )
    if suite_id == "decision_tail":
        return build_decision_tail_run_specs(
            manifest,
            warmup_seeds=args.warmup_seeds,
            measured_seeds=args.measured_seeds,
            trace_seeds=args.decision_trace_seeds,
        )
    raise ValueError(f"Unsupported suite: {suite_id}")


def load_all_summary_rows(results_dir):
    rows = []
    for suite_id in ("rover", "dense", "decision_tail"):
        path = Path(results_dir) / f"{suite_id}_summary.csv"
        if path.exists():
            rows.extend(read_csv_rows(path))
    return rows


def populate_common_arguments(parser):
    parser.add_argument("--generated-dir", default=str(DEFAULT_GENERATED_DIR))
    parser.add_argument("--results-dir", default=str(DEFAULT_RESULTS_DIR))
    parser.add_argument("--rover-config", default=str((REPO_ROOT / "rover_model3.yaml").resolve()))
    parser.add_argument("--rover-model", default=str((REPO_ROOT / "rover_model3.csp").resolve()))
    parser.add_argument("--varanus-script", default="varanus.py")
    parser.add_argument("--varanus-python", default="python3")
    parser.add_argument("--varanus-host", default="127.0.0.1")
    parser.add_argument("--varanus-port", type=int, default=5087)
    parser.add_argument("--refresh-inputs", action="store_true")
    parser.add_argument("--skip-plots", action="store_true")
    parser.add_argument("--dense-sizes", type=parse_int_list, default=list(DEFAULT_DENSE_SIZES))
    parser.add_argument("--dense-lengths", type=parse_int_list, default=list(DEFAULT_DENSE_LENGTHS))
    parser.add_argument("--decision-branching", type=parse_int_list, default=list(DEFAULT_DECISION_BRANCHING))
    parser.add_argument("--decision-depths", type=parse_int_list, default=list(DEFAULT_DECISION_DEPTHS))
    parser.add_argument("--decision-tails", type=parse_int_list, default=list(DEFAULT_DECISION_TAILS))
    parser.add_argument("--warmup-seeds", type=parse_int_list, default=list(DEFAULT_WARMUP_SEEDS))
    parser.add_argument("--measured-seeds", type=parse_int_list, default=list(DEFAULT_MEASURED_SEEDS))
    parser.add_argument("--decision-trace-seeds", type=parse_int_list, default=list(range(20)))
    parser.add_argument(
        "--keep-worker-artifacts",
        action="store_true",
        help="Keep per-run spec, scratch directory, and worker stdout/stderr files under results/.tmp.",
    )
    return parser


def build_common_parser():
    parser = argparse.ArgumentParser(description="Benchmark runner for PredictiveVaranus experiments.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(subparser):
        populate_common_arguments(subparser)

    return parser, subparsers, add_common


def write_metadata(results_dir, args, manifest, suite_ids):
    payload = machine_metadata(
        varanus_script=args.varanus_script,
        varanus_python=args.varanus_python,
        measured_seeds=args.measured_seeds,
        decision_trace_seeds=args.decision_trace_seeds,
    )
    payload.update(
        {
            "command": " ".join(sys.argv),
            "generated_manifest": str(manifest_path(args.generated_dir).resolve()),
            "generated_dir": str(Path(args.generated_dir).resolve()),
            "results_dir": str(Path(results_dir).resolve()),
            "suites": list(suite_ids),
            "dense_sizes": list(args.dense_sizes),
            "dense_lengths": list(args.dense_lengths),
            "decision_branching": list(args.decision_branching),
            "decision_depths": list(args.decision_depths),
            "decision_tails": list(args.decision_tails),
            "warmup_seeds": list(args.warmup_seeds),
            "measured_seeds": list(args.measured_seeds),
        }
    )
    write_json(Path(results_dir) / "benchmark_metadata.json", payload)


def execute_suite(args, manifest, suite_id):
    results_dir = ensure_dir(args.results_dir)
    tmp_suite_dir = Path(args.results_dir) / ".tmp" / suite_id
    if tmp_suite_dir.exists():
        shutil.rmtree(tmp_suite_dir)
    ensure_dir(tmp_suite_dir)

    paths = suite_result_paths(results_dir, suite_id)
    for path in paths.values():
        if path.exists():
            path.unlink()

    measured_summary_rows = []
    specs = suite_specs(manifest, suite_id, args)
    total_runs = len(specs)

    for index, spec in enumerate(specs, start=1):
        spec = dict(spec)
        run_dir = tmp_suite_dir / spec["run_id"]
        spec["scratch_dir"] = str(run_dir / "scratch")
        spec["event_csv_path"] = str(run_dir / "events.csv")
        spec["summary_json_path"] = str(run_dir / "summary.json")
        spec["worker_stdout_path"] = str(run_dir / "worker.stdout.log")
        spec["worker_stderr_path"] = str(run_dir / "worker.stderr.log")
        spec["varanus_script"] = str(Path(args.varanus_script).resolve())
        spec["varanus_python"] = args.varanus_python
        spec["varanus_host"] = args.varanus_host
        spec["varanus_port"] = args.varanus_port

        print(
            f"[{suite_id}] {index}/{total_runs} run_id={spec['run_id']} warmup={spec['warmup']}",
            flush=True,
        )

        try:
            run_worker_subprocess(spec, RUNNER_MODULE, sys.executable)
            event_rows, summary_row = collect_worker_outputs(spec)
        except Exception as error:
            spec["_preserve_artifacts_on_failure"] = True
            stderr = getattr(error, "stderr", None)
            stdout = getattr(error, "stdout", None)
            if stdout:
                print(stdout, file=sys.stderr, flush=True)
            if stderr:
                print(stderr, file=sys.stderr, flush=True)
            print(
                "Worker artifacts preserved under {path}".format(path=run_dir.resolve()),
                file=sys.stderr,
                flush=True,
            )
            for key in ("spec_path", "worker_stdout_path", "worker_stderr_path", "scratch_dir"):
                value = spec.get(key)
                if value:
                    print("{key}={value}".format(key=key, value=value), file=sys.stderr, flush=True)
            spec_path = spec.get("spec_path")
            if spec_path:
                print(
                    "rerun_command={python} -m {module} run-one --spec {spec}".format(
                        python=sys.executable,
                        module=RUNNER_MODULE,
                        spec=spec_path,
                    ),
                    file=sys.stderr,
                    flush=True,
                )
            raise
        finally:
            if spec.get("_preserve_artifacts_on_failure") or args.keep_worker_artifacts:
                pass
            else:
                cleanup_worker_artifacts(spec)

        if spec["warmup"]:
            continue

        append_csv_rows(paths["event_log"], event_rows, EVENT_LOG_FIELDS)
        measured_summary_rows.append(summary_row)

    write_csv(paths["summary"], measured_summary_rows, SUMMARY_FIELDS)
    return measured_summary_rows


def refresh_derived_outputs(results_dir, args, manifest, suite_ids):
    summary_rows = load_all_summary_rows(results_dir)
    write_plot_and_table_csvs(results_dir, summary_rows)
    write_metadata(results_dir, args, manifest, suite_ids)
    if not args.skip_plots:
        generate_plots(results_dir)


def configure_parser():
    parser, subparsers, add_common = build_common_parser()
    prepare_inputs_cmd = subparsers.add_parser("prepare-inputs", help="Generate rover traces and synthetic model/config inputs.")
    add_common(prepare_inputs_cmd)

    for name in ("run-all", "run-rover", "run-dense", "run-decision-tail", "build-plots"):
        cmd = subparsers.add_parser(name, help=f"{name} benchmark suite.")
        add_common(cmd)

    run_one = subparsers.add_parser("run-one", help="Internal worker entry point.")
    run_one.add_argument("--spec", required=True)

    return parser


def resolve_suite_ids(command):
    if command == "run-all":
        return ["rover", "dense", "decision_tail"]
    if command == "run-rover":
        return ["rover"]
    if command == "run-dense":
        return ["dense"]
    if command == "run-decision-tail":
        return ["decision_tail"]
    raise ValueError(f"Unsupported command: {command}")


def run_selected_suites(args, suite_ids):
    manifest = load_or_prepare_manifest(args)
    for suite_id in suite_ids:
        execute_suite(args, manifest, suite_id)
    refresh_derived_outputs(args.results_dir, args, manifest, suite_ids)
    return manifest


def main():
    parser = configure_parser()
    args = parser.parse_args()

    if args.command == "run-one":
        spec = read_json(args.spec)
        summary_row = run_worker(spec)
        print(json.dumps({"run_id": summary_row["run_id"], "final_verdict": summary_row["final_verdict"]}), flush=True)
        return

    args = validate_runtime_paths(args, parser)

    manifest = load_or_prepare_manifest(args)

    if args.command == "prepare-inputs":
        print(f"Prepared benchmark inputs under {Path(args.generated_dir).resolve()}", flush=True)
        return

    if args.command == "build-plots":
        refresh_derived_outputs(args.results_dir, args, manifest, [])
        print(f"Updated derived CSVs and plots in {Path(args.results_dir).resolve()}", flush=True)
        return

    suite_ids = resolve_suite_ids(args.command)
    for suite_id in suite_ids:
        execute_suite(args, manifest, suite_id)
    refresh_derived_outputs(args.results_dir, args, manifest, suite_ids)
    print(f"Benchmark outputs written to {Path(args.results_dir).resolve()}", flush=True)


if __name__ == "__main__":
    main()
