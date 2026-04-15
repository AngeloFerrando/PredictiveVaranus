import argparse
import sys
from pathlib import Path


if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


from experiments.benchmark_lib import (
    read_csv_rows,
    read_json,
    write_stress_publication_outputs,
    write_stress_setup_summary,
)
from experiments.run_benchmarks import (
    populate_common_arguments,
    run_selected_suites,
    validate_runtime_paths,
    write_metadata,
)


def configure_parser():
    parser = argparse.ArgumentParser(
        description="Run the synthetic stress-test evaluation and generate the cost/benefit tables, plots, and setup metadata."
    )
    populate_common_arguments(parser)
    return parser


def main():
    parser = configure_parser()
    args = parser.parse_args()
    args = validate_runtime_paths(args, parser)

    manifest = run_selected_suites(args, ["dense", "decision_tail"])

    summary_rows = []
    for filename in ("dense_summary.csv", "decision_tail_summary.csv"):
        path = Path(args.results_dir) / filename
        if path.exists():
            summary_rows.extend(read_csv_rows(path))

    written = write_stress_publication_outputs(args.results_dir, summary_rows)
    write_metadata(args.results_dir, args, manifest, ["dense", "decision_tail"])
    metadata = read_json(Path(args.results_dir) / "benchmark_metadata.json")
    written.extend(write_stress_setup_summary(args.results_dir, metadata))

    print("Stress-test evaluation outputs written to {path}".format(path=Path(args.results_dir).resolve()), flush=True)
    for filename in written:
        print(filename, flush=True)


if __name__ == "__main__":
    main()
