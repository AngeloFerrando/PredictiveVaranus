import asyncio
import csv
import json
import math
import os
import platform
import random
import resource
import shutil
import statistics
import subprocess
import sys
import textwrap
import time
from pathlib import Path

from monitor import (
    connect_varanus_ws,
    extract_parsed_event,
    find_generated_hoa,
    gate_with_varanus,
    normalize_gate_verdict,
    parse_hoa_metadata,
    project_ltl_formula,
    resolve_projected_event,
    run_varanus_buchi,
    start_varanus_online,
    stop_varanus_online,
)
from hoa_projection import project_hoa_file


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_GENERATED_DIR = REPO_ROOT / "experiments" / "generated"
DEFAULT_RESULTS_DIR = REPO_ROOT / "results"
DEFAULT_TMP_DIR = DEFAULT_RESULTS_DIR / ".tmp"

DEFAULT_DENSE_SIZES = [10, 50, 100, 200, 500, 1000]
DEFAULT_DENSE_LENGTHS = [100, 1000, 10000, 100000]
DEFAULT_DECISION_BRANCHING = [2, 4, 8]
DEFAULT_DECISION_DEPTHS = [4, 6, 8]
DEFAULT_DECISION_TAILS = [1, 5, 10, 20]
DEFAULT_MEASURED_SEEDS = list(range(10))
DEFAULT_DECISION_TRACE_SEEDS = list(range(20))
DEFAULT_WARMUP_SEEDS = [-2, -1]

ROVER_PROPERTIES = [
    {
        "property_id": "complete",
        "formula": "F(!tick & mission_complete)",
        "label": "phi_complete",
    },
    {
        "property_id": "abort",
        "formula": "G((radiation_level.Red | radiation_level.Orange) -> F(!tick & mission_abort))",
        "label": "phi_abort",
    },
    {
        "property_id": "cover",
        "formula": "F(!tick & move.1) & F(!tick & move.2) & F(!tick & move.3) & F(!tick & move.4) & F(!tick & move.5)",
        "label": "phi_cover",
    },
]

EVENT_LOG_FIELDS = [
    "family",
    "suite_id",
    "run_id",
    "scenario_id",
    "trace_id",
    "property_id",
    "property_label",
    "formula",
    "parameter_label",
    "trace_class",
    "seed",
    "trace_seed",
    "repetition",
    "event_index",
    "raw_event",
    "parsed_event",
    "projected_event",
    "gate_verdict",
    "predictive_verdict",
    "predictive_reason",
    "decision_source",
    "final_verdict",
    "t_gate_ms",
    "t_projection_lookup_ms",
    "t_predictive_ms",
    "t_total_ms",
]

SUMMARY_FIELDS = [
    "family",
    "suite_id",
    "run_id",
    "scenario_id",
    "trace_id",
    "property_id",
    "property_label",
    "formula",
    "parameter_label",
    "trace_class",
    "seed",
    "trace_seed",
    "repetition",
    "warmup",
    "model_parameter",
    "trace_length",
    "branching_factor",
    "decision_depth",
    "tail_length",
    "model_size_n",
    "final_verdict",
    "decision_source",
    "first_conclusive_index",
    "first_conclusive_event",
    "reference_index",
    "reference_event",
    "anticipation_gain_events",
    "events_processed",
    "t_export_hoa_ms",
    "t_project_hoa_ms",
    "t_formula_projection_ms",
    "t_runtime_init_ms",
    "t_preproc_total_ms",
    "total_wall_clock_ms",
    "mean_gate_ms_per_event",
    "mean_predictive_ms_per_event",
    "mean_total_ms_per_event",
    "p95_gate_ms_per_event",
    "p95_predictive_ms_per_event",
    "peak_rss_kb",
    "source_hoa_states",
    "source_hoa_ap_count",
    "source_hoa_transitions",
    "projected_hoa_states",
    "projected_hoa_ap_count",
    "projected_hoa_transitions",
    "product_phi_states",
    "product_phi_transitions",
    "product_not_phi_states",
    "product_not_phi_transitions",
    "expected_pattern",
    "expected_pattern_observed",
]


def ensure_dir(path):
    Path(path).mkdir(parents=True, exist_ok=True)
    return Path(path)


def write_text(path, text):
    path = Path(path)
    ensure_dir(path.parent)
    path.write_text(text, encoding="utf-8")
    return path


def write_json(path, payload):
    path = Path(path)
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write_csv(path, rows, fieldnames):
    path = Path(path)
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})
    return path


def append_csv_rows(path, rows, fieldnames):
    path = Path(path)
    ensure_dir(path.parent)
    write_header = (not path.exists()) or path.stat().st_size == 0
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})
    return path


def read_csv_rows(path):
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def ns_to_ms(value):
    return round(value / 1_000_000.0, 6)


def maybe_int(value):
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def safe_mean(values):
    if not values:
        return 0.0
    return round(sum(values) / len(values), 6)


def safe_p95(values):
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 6)
    rank = math.ceil(0.95 * len(ordered)) - 1
    rank = max(0, min(rank, len(ordered) - 1))
    return round(ordered[rank], 6)


def simplify_number(value):
    if value in ("", None):
        return value
    try:
        number = float(value)
    except (TypeError, ValueError):
        return value
    if number.is_integer():
        return int(number)
    return round(number, 6)


def first_matching_index(events, wanted_event):
    for index, event in enumerate(events, start=1):
        if event == wanted_event:
            return index
    return None


def count_automaton_transitions(automaton):
    count = 0
    for state in range(automaton.num_states()):
        for _ in automaton.out(state):
            count += 1
    return count


def get_spot_runtime_dependencies():
    from monitor import import_predictive_runtime_dependencies

    return import_predictive_runtime_dependencies()


def git_commit_hash(path):
    try:
        completed = subprocess.run(
            ["git", "-C", str(path), "rev-parse", "HEAD"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        return completed.stdout.strip()
    except Exception:
        return None


def spot_version_string():
    try:
        import spot

        if hasattr(spot, "__version__"):
            return str(spot.__version__)
        if hasattr(spot, "version"):
            version = spot.version()
            return str(version() if callable(version) else version)
    except Exception:
        return None
    return None


def machine_metadata(varanus_script, varanus_python, measured_seeds, decision_trace_seeds):
    varanus_path = Path(varanus_script).resolve()
    varanus_repo = varanus_path.parent
    return {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "predictive_varanus_repo": str(REPO_ROOT),
        "predictive_varanus_commit": git_commit_hash(REPO_ROOT),
        "varanus_script": str(varanus_path),
        "varanus_repo": str(varanus_repo),
        "varanus_commit": git_commit_hash(varanus_repo),
        "varanus_python": varanus_python,
        "python_executable": sys.executable,
        "python_version": sys.version,
        "spot_version": spot_version_string(),
        "platform": platform.platform(),
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "measured_seeds": list(measured_seeds),
        "decision_trace_seeds": list(decision_trace_seeds),
    }


def build_rover_prefix(visited_waypoints):
    events = []
    for waypoint in visited_waypoints:
        events.extend(
            [
                f"move.{waypoint}",
                f"arrived_at.{waypoint}",
                f"inspect.{waypoint}",
                f"inspected.{waypoint}",
            ]
        )
    return events


def rover_trace_definitions():
    nominal = build_rover_prefix([1, 2, 3, 4, 5]) + ["mission_complete"]
    red_abort = build_rover_prefix([1, 2]) + ["radiation_level.Red", "move.0", "mission_abort"]
    orange_abort = build_rover_prefix([1, 2, 3]) + ["radiation_level.Orange", "move.0", "mission_abort"]
    red_continue_invalid = build_rover_prefix([1, 2]) + ["radiation_level.Red", "move.3"]
    mismatched_move_invalid = ["move.3", "arrived_at.3", "inspect.3", "move.5"]

    definitions = {
        "rover_nominal_green": {
            "trace_id": "rover_nominal_green",
            "trace_class": "nominal",
            "events": nominal,
        },
        "rover_red_abort": {
            "trace_id": "rover_red_abort",
            "trace_class": "hazard_abort_red",
            "events": red_abort,
        },
        "rover_orange_abort": {
            "trace_id": "rover_orange_abort",
            "trace_class": "hazard_abort_orange",
            "events": orange_abort,
        },
        "rover_red_continue_invalid": {
            "trace_id": "rover_red_continue_invalid",
            "trace_class": "invalid_hazard_continue",
            "events": red_continue_invalid,
        },
        "rover_mismatched_move_invalid": {
            "trace_id": "rover_mismatched_move_invalid",
            "trace_class": "invalid_mismatched_move",
            "events": mismatched_move_invalid,
        },
    }

    for waypoint_count in range(1, 5):
        trace_id = f"rover_abort_after_{waypoint_count}"
        definitions[trace_id] = {
            "trace_id": trace_id,
            "trace_class": "hazard_abort_red",
            "events": build_rover_prefix(list(range(1, waypoint_count + 1)))
            + ["radiation_level.Red", "move.0", "mission_abort"],
        }
    return definitions


def write_trace_file(path, events):
    return write_text(path, "\n".join(events) + "\n")


def yaml_list(items):
    return "[" + ", ".join(items) + "]"


def write_varanus_config(path, alphabet, common_alphabet, main_process, model_path, name, trace_file="log.json"):
    body = textwrap.dedent(
        f"""\
        ---
        alphabet: {yaml_list(alphabet)}
        common_alphabet: {yaml_list(common_alphabet)}
        main_process: "{main_process}"
        model: "{Path(model_path).resolve()}"
        trace_file: "{trace_file}"
        name: "{name}"
        mode: "permissive"
        """
    )
    return write_text(path, body)


def dense_model_text(size):
    max_index = size - 1
    return textwrap.dedent(
        f"""\
        channel e : {{0..{max_index}}}

        S(i) = [] j : {{0..{max_index}}} @ e.j -> S(j)

        MAIN = S(0)

        assert MAIN :[deadlock free]
        assert MAIN :[divergence free]
        assert MAIN :[deterministic]
        """
    )


def leaf_id_from_path(path, branching_factor):
    leaf_id = 0
    for choice in path:
        leaf_id = (leaf_id * branching_factor) + choice
    return leaf_id


def decision_tail_model_text(branching_factor, decision_depth, tail_length):
    max_branch = branching_factor - 1
    max_depth = decision_depth - 1
    even_choices = [str(choice) for choice in range(branching_factor) if choice % 2 == 0]
    odd_choices = [str(choice) for choice in range(branching_factor) if choice % 2 == 1]
    lines = [
        "datatype commit_type = ok | fail",
        "channel start",
        f"channel choose : {{0..{max_depth}}}.{{0..{max_branch}}}",
        "channel commit : commit_type",
        f"channel tail : {{1..{tail_length}}}",
        "channel mission_complete",
        "channel mission_abort",
        "",
    ]

    if decision_depth > 1:
        lines.append(f"NODE(d) = if d < {max_depth} then [] j : {{0..{max_branch}}} @ choose.d.j -> NODE(d + 1) else LAST")
    else:
        lines.append("NODE(d) = LAST")
    lines.append(
        "LAST = "
        + "[] j : {"
        + ",".join(even_choices)
        + "} @ choose."
        + str(max_depth)
        + ".j -> COMMIT_OK"
        + " [] j : {"
        + ",".join(odd_choices)
        + "} @ choose."
        + str(max_depth)
        + ".j -> COMMIT_FAIL"
    )
    lines.append("COMMIT_OK = commit.ok -> SUCC_TAIL_1")
    lines.append("COMMIT_FAIL = commit.fail -> FAIL_TAIL_1")
    lines.append("")
    for step in range(1, tail_length + 1):
        lines.append(f"SUCC_TAIL_{step} = tail.{step} -> SUCC_TAIL_{step + 1}")
    lines.append(f"SUCC_TAIL_{tail_length + 1} = mission_complete -> STOP")
    lines.append("")
    for step in range(1, tail_length + 1):
        lines.append(f"FAIL_TAIL_{step} = tail.{step} -> FAIL_TAIL_{step + 1}")
    lines.append(f"FAIL_TAIL_{tail_length + 1} = mission_abort -> STOP")
    lines.append("")
    lines.append("MAIN = start -> NODE(0)")
    lines.append("")
    lines.append("assert MAIN :[deadlock free]")
    lines.append("assert MAIN :[divergence free]")
    lines.append("assert MAIN :[deterministic]")
    lines.append("")
    return "\n".join(lines)


def prepare_inputs(
    generated_dir=DEFAULT_GENERATED_DIR,
    rover_config_path=REPO_ROOT / "rover_model3.yaml",
    rover_model_path=REPO_ROOT / "rover_model3.csp",
    dense_sizes=None,
    decision_branching=None,
    decision_depths=None,
    decision_tails=None,
):
    generated_dir = ensure_dir(generated_dir)
    dense_sizes = list(dense_sizes or DEFAULT_DENSE_SIZES)
    decision_branching = list(decision_branching or DEFAULT_DECISION_BRANCHING)
    decision_depths = list(decision_depths or DEFAULT_DECISION_DEPTHS)
    decision_tails = list(decision_tails or DEFAULT_DECISION_TAILS)

    manifest = {
        "generated_dir": str(generated_dir),
        "rover": {"traces": {}, "config_path": None, "model_path": str(Path(rover_model_path).resolve())},
        "dense": {"models": {}},
        "decision_tail": {"models": {}},
    }

    rover_dir = ensure_dir(generated_dir / "rover")
    rover_trace_dir = ensure_dir(rover_dir / "traces")
    rover_cfg = write_varanus_config(
        rover_dir / "rover_benchmark.yaml",
        alphabet=["inspect", "inspected", "arrived_at", "radiation_level", "mission_complete", "mission_abort", "mission_start", "move"],
        common_alphabet=["inspect", "inspected", "arrived_at", "radiation_level", "move", "mission_complete", "mission_abort", "mission_start"],
        main_process="ROVER_SYSTEM",
        model_path=rover_model_path,
        name="rover_benchmark",
    )
    manifest["rover"]["config_path"] = str(rover_cfg.resolve())
    manifest["rover"]["source_config_path"] = str(Path(rover_config_path).resolve())

    for trace_id, spec in rover_trace_definitions().items():
        trace_path = write_trace_file(rover_trace_dir / f"{trace_id}.trace", spec["events"])
        manifest["rover"]["traces"][trace_id] = {
            "trace_id": trace_id,
            "trace_path": str(trace_path.resolve()),
            "trace_class": spec["trace_class"],
            "events": list(spec["events"]),
        }

    dense_dir = ensure_dir(generated_dir / "dense")
    dense_model_dir = ensure_dir(dense_dir / "models")
    for size in dense_sizes:
        model_path = write_text(dense_model_dir / f"dense_n{size}.csp", dense_model_text(size))
        config_path = write_varanus_config(
            dense_model_dir / f"dense_n{size}.yaml",
            alphabet=["e"],
            common_alphabet=["e"],
            main_process="MAIN",
            model_path=model_path,
            name=f"dense_n{size}",
        )
        manifest["dense"]["models"][str(size)] = {
            "model_size_n": size,
            "model_path": str(model_path.resolve()),
            "config_path": str(config_path.resolve()),
        }

    decision_dir = ensure_dir(generated_dir / "decision_tail")
    decision_model_dir = ensure_dir(decision_dir / "models")
    for branching_factor in decision_branching:
        for decision_depth in decision_depths:
            for tail_length in decision_tails:
                key = f"b{branching_factor}_d{decision_depth}_t{tail_length}"
                model_path = write_text(
                    decision_model_dir / f"decision_tail_{key}.csp",
                    decision_tail_model_text(branching_factor, decision_depth, tail_length),
                )
                config_path = write_varanus_config(
                    decision_model_dir / f"decision_tail_{key}.yaml",
                    alphabet=["start", "choose", "commit", "tail", "mission_complete", "mission_abort"],
                    common_alphabet=["start", "choose", "commit", "tail", "mission_complete", "mission_abort"],
                    main_process="MAIN",
                    model_path=model_path,
                    name=f"decision_tail_{key}",
                )
                manifest["decision_tail"]["models"][key] = {
                    "branching_factor": branching_factor,
                    "decision_depth": decision_depth,
                    "tail_length": tail_length,
                    "model_path": str(model_path.resolve()),
                    "config_path": str(config_path.resolve()),
                }

    write_json(generated_dir / "manifest.json", manifest)
    return manifest


def generate_dense_trace(size, trace_length, seed):
    rng = random.Random(seed)
    return [f"e.{rng.randrange(size)}" for _ in range(trace_length)]


def generate_decision_tail_trace(branching_factor, decision_depth, tail_length, trace_seed):
    rng = random.Random(trace_seed)
    path = [rng.randrange(branching_factor) for _ in range(decision_depth)]
    leaf_id = leaf_id_from_path(path, branching_factor)
    success = (leaf_id % 2) == 0
    events = ["start"]
    for depth, choice in enumerate(path):
        events.append(f"choose.{depth}.{choice}")
    events.append("commit.ok" if success else "commit.fail")
    for step in range(1, tail_length + 1):
        events.append(f"tail.{step}")
    events.append("mission_complete" if success else "mission_abort")
    return {
        "events": events,
        "trace_class": "success" if success else "failure",
        "leaf_id": leaf_id,
        "path": path,
    }


def rover_reference_rule(trace_id, property_id):
    if "invalid" in trace_id:
        return {"type": "first_illegal_event"}

    if property_id == "complete":
        if "abort" in trace_id:
            return {"type": "event_name", "event": "mission_abort"}
        return {"type": "event_name", "event": "mission_complete"}

    if property_id == "abort":
        if "red_abort" in trace_id or "orange_abort" in trace_id or "abort_after_" in trace_id:
            return {"type": "event_name", "event": "mission_abort"}
        return {"type": "event_name", "event": "mission_complete"}

    if property_id == "cover":
        if "abort" in trace_id:
            return {"type": "event_name", "event": "mission_abort"}
        return {"type": "event_name", "event": "mission_complete"}

    return {"type": "none"}


def rover_expected_pattern(trace_id, property_id):
    if trace_id == "rover_red_abort" and property_id == "complete":
        return "predictive false at radiation_level.Red"
    if trace_id == "rover_red_abort" and property_id == "abort":
        return "predictive true at radiation_level.Red"
    if trace_id == "rover_red_abort" and property_id == "cover":
        return "predictive false at radiation_level.Red"
    if trace_id == "rover_orange_abort" and property_id == "complete":
        return "predictive false at radiation_level.Orange"
    if trace_id == "rover_orange_abort" and property_id == "abort":
        return "predictive true at radiation_level.Orange"
    if trace_id == "rover_orange_abort" and property_id == "cover":
        return "predictive false at radiation_level.Orange"
    if trace_id in {"rover_red_continue_invalid", "rover_mismatched_move_invalid"}:
        return "first conclusive verdict from Varanus"
    return ""


def decision_reference_rule(property_id, trace_class):
    if property_id == "succ":
        return {"type": "event_name", "event": "mission_complete" if trace_class == "success" else "mission_abort"}
    return {"type": "event_name", "event": "mission_abort" if trace_class == "failure" else "mission_complete"}


def decision_expected_pattern(property_id, trace_class):
    if property_id == "succ" and trace_class == "success":
        return "predictive true at commit.ok"
    if property_id == "succ" and trace_class == "failure":
        return "predictive false at commit.fail"
    if property_id == "fail" and trace_class == "failure":
        return "predictive true at commit.fail"
    if property_id == "fail" and trace_class == "success":
        return "predictive false at commit.ok"
    return ""


def build_rover_run_specs(manifest):
    specs = []
    rover_cfg = manifest["rover"]["config_path"]
    for property_spec in ROVER_PROPERTIES:
        for trace_id, trace_spec in manifest["rover"]["traces"].items():
            specs.append(
                {
                    "family": "rover",
                    "suite_id": "rover",
                    "run_id": f"{trace_id}_{property_spec['property_id']}",
                    "scenario_id": trace_id,
                    "trace_id": trace_id,
                    "trace_class": trace_spec["trace_class"],
                    "property_id": property_spec["property_id"],
                    "property_label": property_spec["label"],
                    "formula": property_spec["formula"],
                    "config_path": rover_cfg,
                    "trace_kind": "file",
                    "trace_path": trace_spec["trace_path"],
                    "stop_on_conclusion": True,
                    "warmup": False,
                    "seed": "",
                    "trace_seed": "",
                    "repetition": 0,
                    "parameter_label": trace_id,
                    "model_parameter": trace_id,
                    "trace_length": len(trace_spec["events"]),
                    "reference_rule": rover_reference_rule(trace_id, property_spec["property_id"]),
                    "expected_pattern": rover_expected_pattern(trace_id, property_spec["property_id"]),
                    "branching_factor": "",
                    "decision_depth": "",
                    "tail_length": "",
                    "model_size_n": "",
                }
            )
    return specs


def build_dense_run_specs(manifest, trace_lengths=None, warmup_seeds=None, measured_seeds=None):
    trace_lengths = list(trace_lengths or DEFAULT_DENSE_LENGTHS)
    warmup_seeds = list(DEFAULT_WARMUP_SEEDS if warmup_seeds is None else warmup_seeds)
    measured_seeds = list(DEFAULT_MEASURED_SEEDS if measured_seeds is None else measured_seeds)
    specs = []
    for size_key, model_spec in manifest["dense"]["models"].items():
        size = int(size_key)
        for trace_length in trace_lengths:
            scenario_id = f"dense_n{size}_l{trace_length}"
            for seed in warmup_seeds + measured_seeds:
                warmup = seed in warmup_seeds
                repetition = measured_seeds.index(seed) if not warmup else warmup_seeds.index(seed)
                specs.append(
                    {
                        "family": "dense",
                        "suite_id": "dense",
                        "run_id": f"{scenario_id}_seed{seed}",
                        "scenario_id": scenario_id,
                        "trace_id": scenario_id,
                        "trace_class": "valid",
                        "property_id": "dense",
                        "property_label": "phi_dense",
                        "formula": "G(F(e.0))",
                        "config_path": model_spec["config_path"],
                        "trace_kind": "dense_generated",
                        "trace_path": "",
                        "trace_params": {"size": size, "trace_length": trace_length, "seed": seed},
                        "stop_on_conclusion": False,
                        "warmup": warmup,
                        "seed": seed,
                        "trace_seed": seed,
                        "repetition": repetition,
                        "parameter_label": f"N={size}",
                        "model_parameter": f"N={size}",
                        "trace_length": trace_length,
                        "reference_rule": {"type": "none"},
                        "expected_pattern": "formula should remain inconclusive on finite valid traces",
                        "branching_factor": "",
                        "decision_depth": "",
                        "tail_length": "",
                        "model_size_n": size,
                    }
                )
    return specs


def build_decision_tail_run_specs(
    manifest,
    warmup_seeds=None,
    measured_seeds=None,
    trace_seeds=None,
):
    warmup_seeds = list(DEFAULT_WARMUP_SEEDS if warmup_seeds is None else warmup_seeds)
    measured_seeds = list(DEFAULT_MEASURED_SEEDS if measured_seeds is None else measured_seeds)
    trace_seeds = list(DEFAULT_DECISION_TRACE_SEEDS if trace_seeds is None else trace_seeds)
    specs = []
    properties = [
        {"property_id": "succ", "property_label": "phi_succ", "formula": "F(!tick & mission_complete)"},
        {"property_id": "fail", "property_label": "phi_fail", "formula": "F(!tick & mission_abort)"},
    ]
    for key, model_spec in manifest["decision_tail"]["models"].items():
        branching_factor = model_spec["branching_factor"]
        decision_depth = model_spec["decision_depth"]
        tail_length = model_spec["tail_length"]
        for trace_seed in trace_seeds:
            trace_info = generate_decision_tail_trace(branching_factor, decision_depth, tail_length, trace_seed)
            for property_spec in properties:
                scenario_id = f"decision_{key}_{property_spec['property_id']}_trace{trace_seed}"
                for seed in warmup_seeds + measured_seeds:
                    warmup = seed in warmup_seeds
                    repetition = measured_seeds.index(seed) if not warmup else warmup_seeds.index(seed)
                    specs.append(
                        {
                            "family": "decision_tail",
                            "suite_id": "decision_tail",
                            "run_id": f"{scenario_id}_rep{seed}",
                            "scenario_id": scenario_id,
                            "trace_id": f"{key}_trace{trace_seed}",
                            "trace_class": trace_info["trace_class"],
                            "property_id": property_spec["property_id"],
                            "property_label": property_spec["property_label"],
                            "formula": property_spec["formula"],
                            "config_path": model_spec["config_path"],
                            "trace_kind": "decision_tail_generated",
                            "trace_path": "",
                            "trace_params": {
                                "branching_factor": branching_factor,
                                "decision_depth": decision_depth,
                                "tail_length": tail_length,
                                "trace_seed": trace_seed,
                            },
                            "stop_on_conclusion": True,
                            "warmup": warmup,
                            "seed": seed,
                            "trace_seed": trace_seed,
                            "repetition": repetition,
                            "parameter_label": f"B={branching_factor},D={decision_depth},T={tail_length}",
                            "model_parameter": f"B={branching_factor},D={decision_depth},T={tail_length}",
                            "trace_length": len(trace_info["events"]),
                            "reference_rule": decision_reference_rule(property_spec["property_id"], trace_info["trace_class"]),
                            "expected_pattern": decision_expected_pattern(property_spec["property_id"], trace_info["trace_class"]),
                            "branching_factor": branching_factor,
                            "decision_depth": decision_depth,
                            "tail_length": tail_length,
                            "model_size_n": "",
                        }
                    )
    return specs


def trace_events_for_spec(spec):
    trace_kind = spec["trace_kind"]
    if trace_kind == "file":
        trace_path = Path(spec["trace_path"])
        events = [line.rstrip("\n") for line in trace_path.read_text(encoding="utf-8").splitlines() if line != ""]
        return {"events": events, "trace_class": spec["trace_class"]}
    if trace_kind == "dense_generated":
        params = spec["trace_params"]
        return {
            "events": generate_dense_trace(params["size"], params["trace_length"], params["seed"]),
            "trace_class": "valid",
        }
    if trace_kind == "decision_tail_generated":
        params = spec["trace_params"]
        return generate_decision_tail_trace(
            params["branching_factor"],
            params["decision_depth"],
            params["tail_length"],
            params["trace_seed"],
        )
    raise ValueError(f"Unsupported trace kind: {trace_kind}")


def normalize_predictive_verdict(verdict, Verdict):
    if verdict == Verdict.tt:
        return "true"
    if verdict == Verdict.ff:
        return "false"
    return "?"


def is_conclusive_verdict(final_verdict):
    return final_verdict in {"true", "false"}


def resolve_reference_event(spec, trace_events, event_rows):
    rule = spec.get("reference_rule", {"type": "none"})
    rule_type = rule.get("type", "none")

    if rule_type == "none":
        return None, None

    if rule_type == "event_name":
        event_name = rule["event"]
        return first_matching_index(trace_events, event_name), event_name

    if rule_type == "first_illegal_event":
        for row in event_rows:
            if row.get("decision_source") == "varanus" and row.get("gate_verdict") == "false":
                return int(row["event_index"]), row["raw_event"]
        return None, None

    raise ValueError(f"Unsupported reference rule: {rule_type}")


def check_expected_pattern(spec, event_rows, summary_row):
    expected = spec.get("expected_pattern", "")
    if not expected:
        return ""

    trace_id = spec.get("trace_id", "")
    property_id = spec.get("property_id", "")
    if trace_id in {"rover_red_continue_invalid", "rover_mismatched_move_invalid"}:
        return str(summary_row.get("decision_source") == "varanus").lower()

    if trace_id == "rover_red_abort":
        target_event = "radiation_level.Red"
        if property_id == "complete":
            return str(
                any(
                    row["raw_event"] == target_event
                    and row["predictive_verdict"] == "false"
                    and row["decision_source"] == "ltl"
                    for row in event_rows
                )
            ).lower()
        if property_id == "abort":
            return str(
                any(
                    row["raw_event"] == target_event
                    and row["predictive_verdict"] == "true"
                    and row["decision_source"] == "ltl"
                    for row in event_rows
                )
            ).lower()
        if property_id == "cover":
            return str(
                any(
                    row["raw_event"] == target_event
                    and row["predictive_verdict"] == "false"
                    and row["decision_source"] == "ltl"
                    for row in event_rows
                )
            ).lower()

    if trace_id == "rover_orange_abort":
        target_event = "radiation_level.Orange"
        if property_id == "complete":
            return str(any(row["raw_event"] == target_event and row["predictive_verdict"] == "false" for row in event_rows)).lower()
        if property_id == "abort":
            return str(any(row["raw_event"] == target_event and row["predictive_verdict"] == "true" for row in event_rows)).lower()
        if property_id == "cover":
            return str(any(row["raw_event"] == target_event and row["predictive_verdict"] == "false" for row in event_rows)).lower()

    if spec.get("family") == "decision_tail":
        target_event = "commit.ok" if spec.get("trace_class") == "success" else "commit.fail"
        expected_verdict = "true"
        if (property_id == "succ" and spec.get("trace_class") == "failure") or (
            property_id == "fail" and spec.get("trace_class") == "success"
        ):
            expected_verdict = "false"
        return str(any(row["raw_event"] == target_event and row["predictive_verdict"] == expected_verdict for row in event_rows)).lower()

    if spec.get("family") == "dense":
        return str(summary_row.get("final_verdict") in {"?", "currently_true"}).lower()

    return ""


async def monitor_trace_async(spec, runtime, projection_map, trace_events, varanus_host, varanus_port):
    _, websockets, _, Verdict = get_spot_runtime_dependencies()
    projected_symbols = set(projection_map.values())
    ws = await connect_varanus_ws(websockets, f"ws://{varanus_host}:{varanus_port}")

    event_rows = []
    first_conclusive = None
    last_final_verdict = "?"
    last_decision_source = "varanus"

    try:
        for event_index, raw_event in enumerate(trace_events, start=1):
            gate_start = time.perf_counter_ns()
            gate_reply = await gate_with_varanus(ws, raw_event)
            gate_end = time.perf_counter_ns()
            gate_verdict = normalize_gate_verdict(gate_reply.get("verdict", ""))

            projection_start = gate_end
            projection_end = gate_end
            predictive_start = gate_end
            predictive_end = gate_end
            predictive_text = ""
            predictive_reason = ""
            projected_event = ""
            parsed_event = extract_parsed_event(gate_reply) or ""
            decision_source = "varanus"
            final_verdict = gate_verdict or "false"

            if gate_verdict == "currently_true" and parsed_event:
                projection_start = time.perf_counter_ns()
                projected_event = resolve_projected_event(parsed_event, projection_map, projected_symbols)
                projection_end = time.perf_counter_ns()

                predictive_start = time.perf_counter_ns()
                predictive_verdict = runtime.step(projected_event)
                predictive_end = time.perf_counter_ns()
                predictive_text = normalize_predictive_verdict(predictive_verdict, Verdict)
                predictive_reason = runtime.get_last_step_info().get("reason", "")
                if predictive_text == "?":
                    final_verdict = "currently_true"
                    decision_source = "varanus"
                else:
                    final_verdict = predictive_text
                    decision_source = "ltl"
            elif gate_verdict == "currently_true":
                final_verdict = "currently_true"
                decision_source = "varanus"
            elif gate_verdict == "ignored":
                final_verdict = "ignored"
                decision_source = "varanus"
            else:
                final_verdict = "false"
                decision_source = "varanus"

            row = {
                "family": spec["family"],
                "suite_id": spec["suite_id"],
                "run_id": spec["run_id"],
                "scenario_id": spec["scenario_id"],
                "trace_id": spec["trace_id"],
                "property_id": spec["property_id"],
                "property_label": spec["property_label"],
                "formula": spec["formula"],
                "parameter_label": spec["parameter_label"],
                "trace_class": spec["trace_class"],
                "seed": spec["seed"],
                "trace_seed": spec["trace_seed"],
                "repetition": spec["repetition"],
                "event_index": event_index,
                "raw_event": raw_event,
                "parsed_event": parsed_event,
                "projected_event": projected_event,
                "gate_verdict": gate_verdict,
                "predictive_verdict": predictive_text,
                "predictive_reason": predictive_reason,
                "decision_source": decision_source,
                "final_verdict": final_verdict,
                "t_gate_ms": ns_to_ms(gate_end - gate_start),
                "t_projection_lookup_ms": ns_to_ms(projection_end - projection_start),
                "t_predictive_ms": ns_to_ms(predictive_end - predictive_start),
                "t_total_ms": ns_to_ms((gate_end - gate_start) + (projection_end - projection_start) + (predictive_end - predictive_start)),
            }
            event_rows.append(row)
            last_final_verdict = final_verdict
            last_decision_source = decision_source

            if first_conclusive is None and is_conclusive_verdict(final_verdict):
                first_conclusive = {
                    "index": event_index,
                    "event": raw_event,
                    "decision_source": decision_source,
                    "final_verdict": final_verdict,
                }
                if spec.get("stop_on_conclusion", True):
                    break

            if gate_verdict == "false":
                break
    finally:
        await ws.close()

    return {
        "event_rows": event_rows,
        "first_conclusive": first_conclusive,
        "last_final_verdict": last_final_verdict,
        "last_decision_source": last_decision_source,
    }


def run_worker(spec):
    spec = dict(spec)
    scratch_dir = ensure_dir(spec["scratch_dir"])
    event_csv_path = Path(spec["event_csv_path"])
    summary_json_path = Path(spec["summary_json_path"])
    trace_info = trace_events_for_spec(spec)
    trace_events = list(trace_info["events"])
    spec["trace_class"] = trace_info.get("trace_class", spec.get("trace_class", ""))
    trace_path = write_trace_file(scratch_dir / "trace.txt", trace_events)

    run_start_ns = time.perf_counter_ns()
    old_cwd = Path.cwd()
    varanus_process = None

    try:
        os.chdir(scratch_dir)
        spot, _, PredictiveRuntime, _ = get_spot_runtime_dependencies()

        export_start = time.perf_counter_ns()
        run_varanus_buchi(spec["config_path"], spec["varanus_script"], spec["varanus_python"], verbose_varanus=False)
        export_end = time.perf_counter_ns()

        source_hoa = Path(find_generated_hoa()).resolve()
        source_metadata = parse_hoa_metadata(str(source_hoa))
        source_automaton = spot.automaton(str(source_hoa))
        source_transition_count = count_automaton_transitions(source_automaton)

        project_start = time.perf_counter_ns()
        projected_hoa_path, projection_map = project_hoa_file(
            input_hoa_path=str(source_hoa),
            output_hoa_path=str((scratch_dir / "automaton_projected.hoa").resolve()),
            mapping_output_path=str((scratch_dir / "event_projection_map.json").resolve()),
            prefix="p",
        )
        project_end = time.perf_counter_ns()

        formula_start = time.perf_counter_ns()
        projected_formula = project_ltl_formula(spec["formula"], projection_map)
        formula_end = time.perf_counter_ns()

        runtime_start = time.perf_counter_ns()
        projected_automaton = spot.automaton(projected_hoa_path)
        runtime = PredictiveRuntime(projected_formula, projected_automaton)
        runtime_end = time.perf_counter_ns()

        projected_metadata = parse_hoa_metadata(projected_hoa_path)
        projected_transition_count = count_automaton_transitions(projected_automaton)
        runtime_stats = runtime.get_static_stats()

        varanus_process = start_varanus_online(
            spec["config_path"],
            spec["varanus_script"],
            spec["varanus_python"],
            verbose_varanus=False,
        )
        monitored = asyncio.run(
            monitor_trace_async(
                spec=spec,
                runtime=runtime,
                projection_map=projection_map,
                trace_events=trace_events,
                varanus_host=spec["varanus_host"],
                varanus_port=spec["varanus_port"],
            )
        )

        event_rows = monitored["event_rows"]
        first_conclusive = monitored["first_conclusive"]
        reference_index, reference_event = resolve_reference_event(spec, trace_events, event_rows)
        first_conclusive_index = first_conclusive["index"] if first_conclusive else None
        first_conclusive_event = first_conclusive["event"] if first_conclusive else None
        final_verdict = first_conclusive["final_verdict"] if first_conclusive else monitored["last_final_verdict"]
        decision_source = first_conclusive["decision_source"] if first_conclusive else monitored["last_decision_source"]

        gate_times = [float(row["t_gate_ms"]) for row in event_rows]
        predictive_times = [float(row["t_predictive_ms"]) for row in event_rows]
        total_times = [float(row["t_total_ms"]) for row in event_rows]

        anticipation_gain = None
        if reference_index is not None and first_conclusive_index is not None:
            anticipation_gain = reference_index - first_conclusive_index
        if spec.get("reference_rule", {}).get("type") == "first_illegal_event":
            anticipation_gain = 0

        summary_row = {
            "family": spec["family"],
            "suite_id": spec["suite_id"],
            "run_id": spec["run_id"],
            "scenario_id": spec["scenario_id"],
            "trace_id": spec["trace_id"],
            "property_id": spec["property_id"],
            "property_label": spec["property_label"],
            "formula": spec["formula"],
            "parameter_label": spec["parameter_label"],
            "trace_class": spec["trace_class"],
            "seed": spec["seed"],
            "trace_seed": spec["trace_seed"],
            "repetition": spec["repetition"],
            "warmup": str(bool(spec.get("warmup", False))).lower(),
            "model_parameter": spec["model_parameter"],
            "trace_length": spec["trace_length"],
            "branching_factor": spec["branching_factor"],
            "decision_depth": spec["decision_depth"],
            "tail_length": spec["tail_length"],
            "model_size_n": spec["model_size_n"],
            "final_verdict": final_verdict if final_verdict != "currently_true" else "?",
            "decision_source": decision_source,
            "first_conclusive_index": first_conclusive_index or "",
            "first_conclusive_event": first_conclusive_event or "",
            "reference_index": reference_index or "",
            "reference_event": reference_event or "",
            "anticipation_gain_events": anticipation_gain if anticipation_gain is not None else "",
            "events_processed": len(event_rows),
            "t_export_hoa_ms": ns_to_ms(export_end - export_start),
            "t_project_hoa_ms": ns_to_ms(project_end - project_start),
            "t_formula_projection_ms": ns_to_ms(formula_end - formula_start),
            "t_runtime_init_ms": ns_to_ms(runtime_end - runtime_start),
            "t_preproc_total_ms": ns_to_ms((export_end - export_start) + (project_end - project_start) + (formula_end - formula_start) + (runtime_end - runtime_start)),
            "total_wall_clock_ms": ns_to_ms(time.perf_counter_ns() - run_start_ns),
            "mean_gate_ms_per_event": safe_mean(gate_times),
            "mean_predictive_ms_per_event": safe_mean(predictive_times),
            "mean_total_ms_per_event": safe_mean(total_times),
            "p95_gate_ms_per_event": safe_p95(gate_times),
            "p95_predictive_ms_per_event": safe_p95(predictive_times),
            "peak_rss_kb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "source_hoa_states": maybe_int(source_metadata.get("states")),
            "source_hoa_ap_count": len(source_metadata.get("aps", [])),
            "source_hoa_transitions": source_transition_count,
            "projected_hoa_states": maybe_int(projected_metadata.get("states")),
            "projected_hoa_ap_count": len(projected_metadata.get("aps", [])),
            "projected_hoa_transitions": projected_transition_count,
            "product_phi_states": runtime_stats.get("product_phi_states", ""),
            "product_phi_transitions": runtime_stats.get("product_phi_transitions", ""),
            "product_not_phi_states": runtime_stats.get("product_not_phi_states", ""),
            "product_not_phi_transitions": runtime_stats.get("product_not_phi_transitions", ""),
            "expected_pattern": spec.get("expected_pattern", ""),
            "expected_pattern_observed": "",
        }
        summary_row["expected_pattern_observed"] = check_expected_pattern(spec, event_rows, summary_row)

        write_csv(event_csv_path, event_rows, EVENT_LOG_FIELDS)
        write_json(summary_json_path, summary_row)
        return summary_row
    finally:
        if varanus_process is not None:
            stop_varanus_online(varanus_process)
        os.chdir(old_cwd)


def suite_result_paths(results_dir, suite_id):
    results_dir = Path(results_dir)
    return {
        "event_log": results_dir / f"{suite_id}_event_log.csv",
        "summary": results_dir / f"{suite_id}_summary.csv",
    }


def group_rows(rows, keys):
    grouped = {}
    for row in rows:
        group_key = tuple(row.get(key, "") for key in keys)
        grouped.setdefault(group_key, []).append(row)
    return grouped


def numeric(row, key):
    value = row.get(key, "")
    if value in ("", None):
        return None
    return float(value)


def aggregate_plot_rows(rows):
    dense_rows = [row for row in rows if row.get("family") == "dense"]
    decision_rows = [row for row in rows if row.get("family") == "decision_tail"]
    rover_rows = [row for row in rows if row.get("family") == "rover"]

    export_vs_states = []
    for (parameter_label, model_size_n), items in group_rows(dense_rows, ["parameter_label", "model_size_n"]).items():
        export_vs_states.append(
            {
                "model_parameter": parameter_label,
                "model_size_n": model_size_n,
                "source_hoa_states": safe_mean([numeric(item, "source_hoa_states") for item in items if numeric(item, "source_hoa_states") is not None]),
                "t_export_hoa_ms_mean": safe_mean([numeric(item, "t_export_hoa_ms") for item in items if numeric(item, "t_export_hoa_ms") is not None]),
                "t_project_plus_runtime_ms_mean": safe_mean(
                    [
                        (numeric(item, "t_project_hoa_ms") or 0.0) + (numeric(item, "t_runtime_init_ms") or 0.0)
                        for item in items
                    ]
                ),
                "peak_rss_kb_mean": safe_mean([numeric(item, "peak_rss_kb") for item in items if numeric(item, "peak_rss_kb") is not None]),
            }
        )

    gate_cost_vs_trace = []
    pred_cost_vs_trace = []
    for (parameter_label, trace_length), items in group_rows(dense_rows, ["parameter_label", "trace_length"]).items():
        gate_cost_vs_trace.append(
            {
                "model_parameter": parameter_label,
                "trace_length": trace_length,
                "mean_gate_ms_per_event_mean": safe_mean([numeric(item, "mean_gate_ms_per_event") for item in items if numeric(item, "mean_gate_ms_per_event") is not None]),
                "mean_total_ms_per_event_mean": safe_mean([numeric(item, "mean_total_ms_per_event") for item in items if numeric(item, "mean_total_ms_per_event") is not None]),
            }
        )
        pred_cost_vs_trace.append(
            {
                "model_parameter": parameter_label,
                "trace_length": trace_length,
                "mean_predictive_ms_per_event_mean": safe_mean([numeric(item, "mean_predictive_ms_per_event") for item in items if numeric(item, "mean_predictive_ms_per_event") is not None]),
                "mean_total_ms_per_event_mean": safe_mean([numeric(item, "mean_total_ms_per_event") for item in items if numeric(item, "mean_total_ms_per_event") is not None]),
            }
        )

    gain_vs_tail = []
    for (tail_length, property_id, trace_class), items in group_rows(decision_rows, ["tail_length", "property_id", "trace_class"]).items():
        gain_vs_tail.append(
            {
                "tail_length": tail_length,
                "property_id": property_id,
                "trace_class": trace_class,
                "anticipation_gain_mean": safe_mean([numeric(item, "anticipation_gain_events") for item in items if numeric(item, "anticipation_gain_events") is not None]),
            }
        )

    gain_vs_branching = []
    for (branching_factor, property_id, trace_class), items in group_rows(decision_rows, ["branching_factor", "property_id", "trace_class"]).items():
        gain_vs_branching.append(
            {
                "branching_factor": branching_factor,
                "property_id": property_id,
                "trace_class": trace_class,
                "anticipation_gain_mean": safe_mean([numeric(item, "anticipation_gain_events") for item in items if numeric(item, "anticipation_gain_events") is not None]),
            }
        )

    rover_table = []
    for row in rover_rows:
        rover_table.append(
            {
                "scenario": row.get("scenario_id", ""),
                "property": row.get("property_id", ""),
                "verdict": row.get("final_verdict", ""),
                "predictive_position": row.get("first_conclusive_index", ""),
                "reference_position": row.get("reference_index", ""),
                "gain": row.get("anticipation_gain_events", ""),
            }
        )

    dense_table = []
    for (model_parameter, trace_length), items in group_rows(dense_rows, ["model_parameter", "trace_length"]).items():
        dense_table.append(
            {
                "model_parameter": model_parameter,
                "trace_length": trace_length,
                "preproc_time": safe_mean([numeric(item, "t_preproc_total_ms") for item in items if numeric(item, "t_preproc_total_ms") is not None]),
                "csp_cost_per_event": safe_mean([numeric(item, "mean_gate_ms_per_event") for item in items if numeric(item, "mean_gate_ms_per_event") is not None]),
                "pred_cost_per_event": safe_mean([numeric(item, "mean_predictive_ms_per_event") for item in items if numeric(item, "mean_predictive_ms_per_event") is not None]),
                "total_cost_per_event": safe_mean([numeric(item, "mean_total_ms_per_event") for item in items if numeric(item, "mean_total_ms_per_event") is not None]),
            }
        )

    decision_table = []
    for (model_parameter, property_id, trace_class), items in group_rows(decision_rows, ["model_parameter", "property_id", "trace_class"]).items():
        decision_table.append(
            {
                "model_parameter": model_parameter,
                "property": property_id,
                "trace_class": trace_class,
                "pred_latency": safe_mean([numeric(item, "first_conclusive_index") for item in items if numeric(item, "first_conclusive_index") is not None]),
                "reference_latency": safe_mean([numeric(item, "reference_index") for item in items if numeric(item, "reference_index") is not None]),
                "anticipation_gain": safe_mean([numeric(item, "anticipation_gain_events") for item in items if numeric(item, "anticipation_gain_events") is not None]),
            }
        )

    return {
        "plots_export_vs_states.csv": (export_vs_states, ["model_parameter", "model_size_n", "source_hoa_states", "t_export_hoa_ms_mean", "t_project_plus_runtime_ms_mean", "peak_rss_kb_mean"]),
        "plots_gate_cost_vs_trace_length.csv": (gate_cost_vs_trace, ["model_parameter", "trace_length", "mean_gate_ms_per_event_mean", "mean_total_ms_per_event_mean"]),
        "plots_pred_cost_vs_trace_length.csv": (pred_cost_vs_trace, ["model_parameter", "trace_length", "mean_predictive_ms_per_event_mean", "mean_total_ms_per_event_mean"]),
        "plots_gain_vs_tail_length.csv": (gain_vs_tail, ["tail_length", "property_id", "trace_class", "anticipation_gain_mean"]),
        "plots_gain_vs_branching.csv": (gain_vs_branching, ["branching_factor", "property_id", "trace_class", "anticipation_gain_mean"]),
        "paper_rover_table.csv": (rover_table, ["scenario", "property", "verdict", "predictive_position", "reference_position", "gain"]),
        "paper_stress_cost_table.csv": (dense_table, ["model_parameter", "trace_length", "preproc_time", "csp_cost_per_event", "pred_cost_per_event", "total_cost_per_event"]),
        "paper_stress_benefit_table.csv": (decision_table, ["model_parameter", "property", "trace_class", "pred_latency", "reference_latency", "anticipation_gain"]),
    }


def averaged_rows(rows, group_keys, numeric_keys):
    averaged = []
    for group_key, items in group_rows(rows, group_keys).items():
        row = {key: value for key, value in zip(group_keys, group_key)}
        for numeric_key in numeric_keys:
            values = [numeric(item, numeric_key) for item in items if numeric(item, numeric_key) is not None]
            row[numeric_key] = safe_mean(values)
        averaged.append(row)
    return averaged


def build_rover_publication_rows(summary_rows):
    rover_rows = [row for row in summary_rows if row.get("family") == "rover"]
    averaged = averaged_rows(
        rover_rows,
        ["scenario_id", "property_id"],
        [
            "first_conclusive_index",
            "reference_index",
            "anticipation_gain_events",
            "mean_gate_ms_per_event",
            "mean_predictive_ms_per_event",
            "mean_total_ms_per_event",
            "total_wall_clock_ms",
        ],
    )
    finals = {}
    for row in rover_rows:
        finals[(row.get("scenario_id", ""), row.get("property_id", ""))] = row

    publication_rows = []
    for row in averaged:
        key = (row["scenario_id"], row["property_id"])
        source = finals.get(key, {})
        publication_rows.append(
            {
                "scenario": row["scenario_id"],
                "property": row["property_id"],
                "verdict": source.get("final_verdict", ""),
                "predictive_position": simplify_number(row.get("first_conclusive_index", "")),
                "reactive_position": simplify_number(row.get("reference_index", "")),
                "gain": simplify_number(row.get("anticipation_gain_events", "")),
                "decision_source": source.get("decision_source", ""),
                "first_conclusive_event": source.get("first_conclusive_event", ""),
                "reference_event": source.get("reference_event", ""),
                "mean_gate_ms_per_event": simplify_number(row.get("mean_gate_ms_per_event", "")),
                "mean_predictive_ms_per_event": simplify_number(row.get("mean_predictive_ms_per_event", "")),
                "mean_total_ms_per_event": simplify_number(row.get("mean_total_ms_per_event", "")),
                "total_wall_clock_ms": simplify_number(row.get("total_wall_clock_ms", "")),
                "expected_pattern": source.get("expected_pattern", ""),
                "expected_pattern_observed": source.get("expected_pattern_observed", ""),
            }
        )
    publication_rows.sort(key=lambda item: (item["scenario"], item["property"]))
    return publication_rows


def build_stress_cost_publication_rows(summary_rows):
    dense_rows = [row for row in summary_rows if row.get("family") == "dense"]
    publication_rows = averaged_rows(
        dense_rows,
        ["model_parameter", "trace_length"],
        [
            "t_preproc_total_ms",
            "mean_gate_ms_per_event",
            "mean_predictive_ms_per_event",
            "mean_total_ms_per_event",
            "p95_gate_ms_per_event",
            "p95_predictive_ms_per_event",
            "peak_rss_kb",
            "source_hoa_states",
            "projected_hoa_states",
            "product_phi_states",
            "product_not_phi_states",
        ],
    )
    normalized_rows = []
    for row in publication_rows:
        normalized_rows.append(
            {
                "model_parameter": row["model_parameter"],
                "trace_length": simplify_number(row["trace_length"]),
                "preproc_time": simplify_number(row.get("t_preproc_total_ms", "")),
                "csp_cost_per_event": simplify_number(row.get("mean_gate_ms_per_event", "")),
                "pred_cost_per_event": simplify_number(row.get("mean_predictive_ms_per_event", "")),
                "total_cost_per_event": simplify_number(row.get("mean_total_ms_per_event", "")),
                "p95_gate_ms_per_event": simplify_number(row.get("p95_gate_ms_per_event", "")),
                "p95_predictive_ms_per_event": simplify_number(row.get("p95_predictive_ms_per_event", "")),
                "peak_rss_kb": simplify_number(row.get("peak_rss_kb", "")),
                "source_hoa_states": simplify_number(row.get("source_hoa_states", "")),
                "projected_hoa_states": simplify_number(row.get("projected_hoa_states", "")),
                "product_phi_states": simplify_number(row.get("product_phi_states", "")),
                "product_not_phi_states": simplify_number(row.get("product_not_phi_states", "")),
            }
        )
    normalized_rows.sort(key=lambda item: (item["model_parameter"], int(item["trace_length"])))
    return normalized_rows


def build_stress_benefit_publication_rows(summary_rows):
    decision_rows = [row for row in summary_rows if row.get("family") == "decision_tail"]
    publication_rows = averaged_rows(
        decision_rows,
        ["model_parameter", "property_id", "trace_class"],
        [
            "first_conclusive_index",
            "reference_index",
            "anticipation_gain_events",
            "mean_gate_ms_per_event",
            "mean_predictive_ms_per_event",
            "mean_total_ms_per_event",
            "tail_length",
            "branching_factor",
            "decision_depth",
        ],
    )
    normalized_rows = []
    for row in publication_rows:
        normalized_rows.append(
            {
                "model_parameter": row["model_parameter"],
                "property": row["property_id"],
                "trace_class": row["trace_class"],
                "pred_latency": simplify_number(row.get("first_conclusive_index", "")),
                "react_latency": simplify_number(row.get("reference_index", "")),
                "anticipation_gain": simplify_number(row.get("anticipation_gain_events", "")),
                "branching_factor": simplify_number(row.get("branching_factor", "")),
                "decision_depth": simplify_number(row.get("decision_depth", "")),
                "tail_length": simplify_number(row.get("tail_length", "")),
                "mean_gate_ms_per_event": simplify_number(row.get("mean_gate_ms_per_event", "")),
                "mean_predictive_ms_per_event": simplify_number(row.get("mean_predictive_ms_per_event", "")),
                "mean_total_ms_per_event": simplify_number(row.get("mean_total_ms_per_event", "")),
            }
        )
    normalized_rows.sort(key=lambda item: (item["model_parameter"], item["property"], item["trace_class"]))
    return normalized_rows


def write_rover_publication_outputs(results_dir, summary_rows):
    rows = build_rover_publication_rows(summary_rows)
    outputs = {
        "table_rover_eval.csv": (
            rows,
            [
                "scenario",
                "property",
                "verdict",
                "predictive_position",
                "reactive_position",
                "gain",
                "decision_source",
                "first_conclusive_event",
                "reference_event",
                "mean_gate_ms_per_event",
                "mean_predictive_ms_per_event",
                "mean_total_ms_per_event",
                "total_wall_clock_ms",
                "expected_pattern",
                "expected_pattern_observed",
            ],
        ),
    }
    written = []
    for filename, (table_rows, fieldnames) in outputs.items():
        write_csv(Path(results_dir) / filename, table_rows, fieldnames)
        written.append(filename)
    return written


def write_stress_publication_outputs(results_dir, summary_rows):
    cost_rows = build_stress_cost_publication_rows(summary_rows)
    benefit_rows = build_stress_benefit_publication_rows(summary_rows)
    outputs = {
        "table_stress_test_costs.csv": (
            cost_rows,
            [
                "model_parameter",
                "trace_length",
                "preproc_time",
                "csp_cost_per_event",
                "pred_cost_per_event",
                "total_cost_per_event",
                "p95_gate_ms_per_event",
                "p95_predictive_ms_per_event",
                "peak_rss_kb",
                "source_hoa_states",
                "projected_hoa_states",
                "product_phi_states",
                "product_not_phi_states",
            ],
        ),
        "table_stress_test_benefit.csv": (
            benefit_rows,
            [
                "model_parameter",
                "property",
                "trace_class",
                "pred_latency",
                "react_latency",
                "anticipation_gain",
                "branching_factor",
                "decision_depth",
                "tail_length",
                "mean_gate_ms_per_event",
                "mean_predictive_ms_per_event",
                "mean_total_ms_per_event",
            ],
        ),
    }
    written = []
    for filename, (table_rows, fieldnames) in outputs.items():
        write_csv(Path(results_dir) / filename, table_rows, fieldnames)
        written.append(filename)
    return written


def write_stress_setup_summary(results_dir, metadata):
    summary = {
        "platform": metadata.get("platform", ""),
        "system": metadata.get("system", ""),
        "release": metadata.get("release", ""),
        "machine": metadata.get("machine", ""),
        "processor": metadata.get("processor", ""),
        "cpu_count": metadata.get("cpu_count", ""),
        "python_version": metadata.get("python_version", ""),
        "spot_version": metadata.get("spot_version", ""),
        "predictive_varanus_commit": metadata.get("predictive_varanus_commit", ""),
        "varanus_commit": metadata.get("varanus_commit", ""),
        "warmup_runs": len(metadata.get("warmup_seeds", [])),
        "measured_runs": len(metadata.get("measured_seeds", [])),
        "decision_trace_count": len(metadata.get("decision_trace_seeds", [])),
        "dense_sizes": ",".join(str(item) for item in metadata.get("dense_sizes", [])),
        "dense_lengths": ",".join(str(item) for item in metadata.get("dense_lengths", [])),
        "decision_branching": ",".join(str(item) for item in metadata.get("decision_branching", [])),
        "decision_depths": ",".join(str(item) for item in metadata.get("decision_depths", [])),
        "decision_tails": ",".join(str(item) for item in metadata.get("decision_tails", [])),
    }
    write_json(Path(results_dir) / "stress_test_setup.json", summary)
    write_csv(
        Path(results_dir) / "stress_test_setup.csv",
        [summary],
        list(summary.keys()),
    )
    return ["stress_test_setup.json", "stress_test_setup.csv"]


def write_plot_and_table_csvs(results_dir, summary_rows):
    outputs = aggregate_plot_rows(summary_rows)
    written = []
    for filename, (rows, fieldnames) in outputs.items():
        write_csv(Path(results_dir) / filename, rows, fieldnames)
        written.append(filename)
    return written


def generate_plots(results_dir):
    try:
        import matplotlib.pyplot as plt
    except ModuleNotFoundError as error:
        raise RuntimeError("matplotlib is required to generate plots.") from error

    results_dir = Path(results_dir)
    plot_dir = ensure_dir(results_dir / "plots")

    def load(path):
        return read_csv_rows(results_dir / path)

    export_rows = load("plots_export_vs_states.csv")
    if export_rows:
        xs = [float(row["source_hoa_states"]) for row in export_rows]
        ys = [float(row["t_export_hoa_ms_mean"]) for row in export_rows]
        plt.figure()
        plt.plot(xs, ys, marker="o")
        plt.xlabel("Number of CSP states")
        plt.ylabel("HOA export time (ms)")
        plt.title("HOA export time vs number of CSP states")
        plt.tight_layout()
        plt.savefig(plot_dir / "hoa_export_vs_states.png")
        plt.close()

        ys = [float(row["t_project_plus_runtime_ms_mean"]) for row in export_rows]
        plt.figure()
        plt.plot(xs, ys, marker="o")
        plt.xlabel("Number of CSP states")
        plt.ylabel("Projection + runtime init (ms)")
        plt.title("Projection + runtime init vs number of CSP states")
        plt.tight_layout()
        plt.savefig(plot_dir / "projection_runtime_vs_states.png")
        plt.close()

        ys = [float(row["peak_rss_kb_mean"]) for row in export_rows]
        plt.figure()
        plt.plot(xs, ys, marker="o")
        plt.xlabel("Number of CSP states")
        plt.ylabel("Peak RSS (KB)")
        plt.title("Peak RSS vs model size")
        plt.tight_layout()
        plt.savefig(plot_dir / "peak_rss_vs_model_size.png")
        plt.close()

    gate_rows = load("plots_gate_cost_vs_trace_length.csv")
    if gate_rows:
        plt.figure()
        for parameter_label, items in group_rows(gate_rows, ["model_parameter"]).items():
            ordered = sorted(items, key=lambda item: int(item["trace_length"]))
            plt.plot(
                [int(item["trace_length"]) for item in ordered],
                [float(item["mean_gate_ms_per_event_mean"]) for item in ordered],
                marker="o",
                label=parameter_label[0],
            )
        plt.xlabel("Trace length")
        plt.ylabel("Mean gate cost/event (ms)")
        plt.title("Mean gate cost/event vs trace length")
        plt.legend()
        plt.tight_layout()
        plt.savefig(plot_dir / "gate_cost_vs_trace_length.png")
        plt.close()

    pred_rows = load("plots_pred_cost_vs_trace_length.csv")
    if pred_rows:
        plt.figure()
        for parameter_label, items in group_rows(pred_rows, ["model_parameter"]).items():
            ordered = sorted(items, key=lambda item: int(item["trace_length"]))
            plt.plot(
                [int(item["trace_length"]) for item in ordered],
                [float(item["mean_predictive_ms_per_event_mean"]) for item in ordered],
                marker="o",
                label=parameter_label[0],
            )
        plt.xlabel("Trace length")
        plt.ylabel("Mean predictive cost/event (ms)")
        plt.title("Mean predictive cost/event vs trace length")
        plt.legend()
        plt.tight_layout()
        plt.savefig(plot_dir / "predictive_cost_vs_trace_length.png")
        plt.close()

    if gate_rows:
        plt.figure()
        grouped = group_rows(gate_rows, ["model_parameter"])
        xs = []
        ys = []
        for parameter_label, items in grouped.items():
            xs.append(float(parameter_label[0].split("=")[1]))
            ys.append(statistics.mean(float(item["mean_total_ms_per_event_mean"]) for item in items))
        xs, ys = zip(*sorted(zip(xs, ys)))
        plt.plot(xs, ys, marker="o")
        plt.xlabel("Model size parameter")
        plt.ylabel("Total cost/event (ms)")
        plt.title("Total cost/event vs model size")
        plt.tight_layout()
        plt.savefig(plot_dir / "total_cost_vs_model_size.png")
        plt.close()

    gain_tail_rows = load("plots_gain_vs_tail_length.csv")
    if gain_tail_rows:
        plt.figure()
        for group_key, items in group_rows(gain_tail_rows, ["property_id", "trace_class"]).items():
            ordered = sorted(items, key=lambda item: int(item["tail_length"]))
            label = f"{group_key[0]}/{group_key[1]}"
            plt.plot(
                [int(item["tail_length"]) for item in ordered],
                [float(item["anticipation_gain_mean"]) for item in ordered],
                marker="o",
                label=label,
            )
        plt.xlabel("Tail length")
        plt.ylabel("Anticipation gain (events)")
        plt.title("Anticipation gain vs tail length")
        plt.legend()
        plt.tight_layout()
        plt.savefig(plot_dir / "gain_vs_tail_length.png")
        plt.close()

    gain_branch_rows = load("plots_gain_vs_branching.csv")
    if gain_branch_rows:
        plt.figure()
        for group_key, items in group_rows(gain_branch_rows, ["property_id", "trace_class"]).items():
            ordered = sorted(items, key=lambda item: int(item["branching_factor"]))
            label = f"{group_key[0]}/{group_key[1]}"
            plt.plot(
                [int(item["branching_factor"]) for item in ordered],
                [float(item["anticipation_gain_mean"]) for item in ordered],
                marker="o",
                label=label,
            )
        plt.xlabel("Branching factor")
        plt.ylabel("Anticipation gain (events)")
        plt.title("Anticipation gain vs branching factor")
        plt.legend()
        plt.tight_layout()
        plt.savefig(plot_dir / "gain_vs_branching.png")
        plt.close()

    return plot_dir


def run_worker_subprocess(spec, runner_module, python_executable):
    tmp_dir = ensure_dir(Path(spec["scratch_dir"]).parent)
    spec_path = tmp_dir / f"{spec['run_id']}.spec.json"
    spec["spec_path"] = str(spec_path)
    write_json(spec_path, spec)
    completed = subprocess.run(
        [python_executable, "-m", runner_module, "run-one", "--spec", str(spec_path)],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if completed.stdout.strip():
        print(completed.stdout.strip())
    return completed


def collect_worker_outputs(spec):
    event_rows = read_csv_rows(spec["event_csv_path"])
    summary_row = read_json(spec["summary_json_path"])
    return event_rows, summary_row


def cleanup_worker_artifacts(spec):
    for key in ("event_csv_path", "summary_json_path"):
        path = Path(spec[key])
        if path.exists():
            path.unlink()
    spec_path = spec.get("spec_path")
    if spec_path:
        path = Path(spec_path)
        if path.exists():
            path.unlink()
    scratch_dir = Path(spec["scratch_dir"])
    if scratch_dir.exists():
        shutil.rmtree(scratch_dir)


def suite_runs(specs, suite_id):
    return [spec for spec in specs if spec["suite_id"] == suite_id]
