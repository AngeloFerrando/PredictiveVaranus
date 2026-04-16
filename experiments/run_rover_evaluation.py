"""Paper-facing entry point for the rover case-study benchmark."""

import argparse
import sys
from pathlib import Path


if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


from experiments.benchmark_lib import read_csv_rows, write_rover_publication_outputs
from experiments.run_benchmarks import populate_common_arguments, run_selected_suites, validate_runtime_paths, write_metadata


def configure_parser():
    parser = argparse.ArgumentParser(
        description="Run the rover case-study evaluation and generate the table-ready rover outputs."
    )
    populate_common_arguments(parser)
    return parser


def main():
    parser = configure_parser()
    args = parser.parse_args()
    args = validate_runtime_paths(args, parser)

    manifest = run_selected_suites(args, ["rover"])

    summary_rows = read_csv_rows(Path(args.results_dir) / "rover_summary.csv")
    written = write_rover_publication_outputs(args.results_dir, summary_rows)
    write_metadata(args.results_dir, args, manifest, ["rover"])

    print("Rover evaluation outputs written to {path}".format(path=Path(args.results_dir).resolve()), flush=True)
    for filename in written:
        print(filename, flush=True)


if __name__ == "__main__":
    main()
