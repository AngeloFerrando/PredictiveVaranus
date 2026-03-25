import argparse
import json
import os
import re
from pathlib import Path


AP_LINE_PATTERN = re.compile(r"^AP:\s+(\d+)\s*(.*)$")
QUOTED_AP_PATTERN = re.compile(r'"((?:[^"\\]|\\.)*)"')
LABEL_BLOCK_PATTERN = re.compile(r"\[([^\]]*)\]")


def _unescape_hoa_string(value):
    return value.replace(r"\\", "\\").replace(r"\"", '"')


def _escape_hoa_string(value):
    return value.replace("\\", r"\\").replace('"', r"\"")


def _parse_ap_line(line):
    match = AP_LINE_PATTERN.match(line.strip())
    if match is None:
        raise ValueError("No valid AP line found in HOA input.")

    expected_count = int(match.group(1))
    ap_names = [_unescape_hoa_string(item) for item in QUOTED_AP_PATTERN.findall(match.group(2))]
    if len(ap_names) != expected_count:
        raise ValueError(
            f"AP line declares {expected_count} propositions but {len(ap_names)} were parsed."
        )
    return ap_names


def _format_ap_line(ap_names):
    escaped_names = " ".join(f'"{_escape_hoa_string(name)}"' for name in ap_names)
    return f"AP: {len(ap_names)} {escaped_names}" if escaped_names else "AP: 0"


def build_projection_map(ap_names, prefix="p"):
    return {name: f"{prefix}{index}" for index, name in enumerate(ap_names)}


def _sanitize_properties_line(line):
    stripped = line.strip()
    if not stripped.startswith("properties:"):
        return line, False

    tokens = stripped[len("properties:"):].strip().split()
    filtered = [token for token in tokens if token != "deterministic"]

    if filtered:
        return "properties: " + " ".join(filtered), False
    return None, True


def _replace_ap_tokens_in_formula(formula, projection):
    updated = formula
    # Replace longest names first to avoid partial-prefix substitutions.
    for source_ap, projected_ap in sorted(projection.items(), key=lambda item: len(item[0]), reverse=True):
        # AP names are rendered as identifiers in explicit-label HOA formulas.
        # Use conservative boundaries to avoid touching larger identifiers.
        pattern = r"(?<![A-Za-z0-9_.])" + re.escape(source_ap) + r"(?![A-Za-z0-9_.])"
        updated = re.sub(pattern, projected_ap, updated)
    return updated


def _project_label_blocks_in_line(line, projection):
    def _replace_block(match):
        formula = match.group(1)
        return "[" + _replace_ap_tokens_in_formula(formula, projection) + "]"

    return LABEL_BLOCK_PATTERN.sub(_replace_block, line)


def project_hoa_text(hoa_text, prefix="p"):
    lines = hoa_text.splitlines()
    ap_line_index = None
    ap_names = None

    for index, line in enumerate(lines):
        if line.strip().startswith("AP:"):
            ap_line_index = index
            ap_names = _parse_ap_line(line)
            break

    if ap_line_index is None or ap_names is None:
        raise ValueError("Could not find AP line in HOA file.")

    projection = build_projection_map(ap_names, prefix=prefix)
    projected_ap_names = [projection[name] for name in ap_names]
    lines[ap_line_index] = _format_ap_line(projected_ap_names)

    sanitized_lines = []
    for line in lines:
        sanitized_line, drop_line = _sanitize_properties_line(line)
        if drop_line:
            continue
        projected_line = _project_label_blocks_in_line(sanitized_line, projection)
        sanitized_lines.append(projected_line)

    return "\n".join(sanitized_lines) + ("\n" if hoa_text.endswith("\n") else ""), projection, ap_names


def _default_projected_path(path, suffix):
    input_path = Path(path)
    return str(input_path.with_name(f"{input_path.stem}{suffix}{input_path.suffix}"))


def project_hoa_file(input_hoa_path, output_hoa_path=None, mapping_output_path=None, prefix="p"):
    output_hoa_path = output_hoa_path or _default_projected_path(input_hoa_path, "_projected")
    hoa_text = Path(input_hoa_path).read_text(encoding="utf-8")
    projected_text, projection, ap_names = project_hoa_text(hoa_text, prefix=prefix)
    Path(output_hoa_path).write_text(projected_text, encoding="utf-8")

    if mapping_output_path is not None:
        mapping_payload = {
            "ap_order": ap_names,
            "projection_map": projection,
        }
        Path(mapping_output_path).write_text(
            json.dumps(mapping_payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    return output_hoa_path, projection


def project_trace_file(input_trace_path, projection_map, output_trace_path=None, strict=True):
    output_trace_path = output_trace_path or _default_projected_path(input_trace_path, "_projected")
    projected_events = []

    with open(input_trace_path, "r", encoding="utf-8") as source:
        for line_number, raw_line in enumerate(source, start=1):
            event = raw_line.rstrip("\n")
            if event in projection_map:
                projected_events.append(projection_map[event])
            elif strict and event != "":
                raise ValueError(
                    f"Trace event '{event}' at line {line_number} is not in the HOA AP set."
                )
            else:
                projected_events.append(event)

    with open(output_trace_path, "w", encoding="utf-8") as target:
        for event in projected_events:
            target.write(event + "\n")

    return output_trace_path


def project_model_and_trace(
    input_hoa_path,
    input_trace_path,
    output_hoa_path=None,
    output_trace_path=None,
    mapping_output_path=None,
    prefix="p",
    strict_trace=True,
):
    projected_hoa_path, projection = project_hoa_file(
        input_hoa_path=input_hoa_path,
        output_hoa_path=output_hoa_path,
        mapping_output_path=mapping_output_path,
        prefix=prefix,
    )
    projected_trace_path = project_trace_file(
        input_trace_path=input_trace_path,
        projection_map=projection,
        output_trace_path=output_trace_path,
        strict=strict_trace,
    )
    return projected_hoa_path, projected_trace_path


def main():
    parser = argparse.ArgumentParser(
        description="Project a Varanus HOA and trace to proposition-only event names."
    )
    parser.add_argument("hoa", help="Input HOA file path.")
    parser.add_argument("--trace", help="Input trace file path.")
    parser.add_argument("--hoa-output", help="Projected HOA output path.")
    parser.add_argument("--trace-output", help="Projected trace output path.")
    parser.add_argument("--map-output", help="Optional JSON path for the event projection map.")
    parser.add_argument("--prefix", default="p", help="Prefix used for generated propositions.")
    parser.add_argument(
        "--allow-unknown-trace-events",
        action="store_true",
        help="Do not fail if a trace event is not present in the HOA AP set.",
    )
    args = parser.parse_args()

    if not os.path.exists(args.hoa):
        raise FileNotFoundError(f"HOA file not found: {args.hoa}")

    projected_hoa_path, projection = project_hoa_file(
        input_hoa_path=args.hoa,
        output_hoa_path=args.hoa_output,
        mapping_output_path=args.map_output,
        prefix=args.prefix,
    )
    print(f"Projected HOA written to: {projected_hoa_path}")

    if args.trace:
        if not os.path.exists(args.trace):
            raise FileNotFoundError(f"Trace file not found: {args.trace}")
        projected_trace_path = project_trace_file(
            input_trace_path=args.trace,
            projection_map=projection,
            output_trace_path=args.trace_output,
            strict=not args.allow_unknown_trace_events,
        )
        print(f"Projected trace written to: {projected_trace_path}")


if __name__ == "__main__":
    main()
