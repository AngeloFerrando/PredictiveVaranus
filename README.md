# PredictiveVaranus

Varanus-first predictive runtime verification for LTL over event streams.

This project combines:

- a **Varanus conformance gate** (first decision layer), and
- a **predictive LTL monitor** (second decision layer).

The monitor only applies predictive reasoning to events that Varanus accepts as consistent.

## Overview

The pipeline performs:

1. Build Büchi automaton from a Varanus CSP model.
2. Project model AP names (domain labels) to compact proposition symbols (`p0`, `p1`, ...).
3. Project the input LTL formula to the same AP space.
4. Start Varanus in online mode (websocket gate).
5. Evaluate incoming events through:
   - Varanus verdict first,
   - predictive LTL verdict second (when applicable).
6. Return a merged verdict per event.

## Repository layout

- `monitor.py`: main orchestrator (offline + online modes).
- `hoa_projection.py`: HOA/AP projection and optional trace projection.
- `predictive_ltl.py`: predictive monitor runtime and standalone CLI.
- `gen_model.py`: minimal Spot example (auxiliary).

## Runtime architecture

Event producer -> `monitor.py` websocket -> Varanus gate -> predictive LTL runtime -> merged response to producer.

Defaults:

- Predictive endpoint: `ws://127.0.0.1:5088`
- Varanus gate endpoint: `ws://127.0.0.1:5087`

Important:

- Producers must connect to the **predictive** endpoint (`5088` by default).
- If a producer connects directly to Varanus (`5087`), predictive verdicts are bypassed.

## Requirements

- Linux environment.
- Python 3 for this repository.
- Python packages in the monitor interpreter:
  - `spot`
  - `buddy`
  - `websockets`
- A Varanus checkout containing `varanus.py`.
- A Python executable for Varanus (`--varanus-python`).

Note: Varanus may require a different Python than `monitor.py` depending on your FDR/Varanus environment.

## Quick dependency check

```bash
python3 - <<'PY'
import spot, buddy, websockets
print("ok")
PY
```

## Usage

### Online mode

```bash
python3 monitor.py <config.yaml> "<ltl_formula>" \
  --online \
  --host 0.0.0.0 --port 5088 \
  --varanus-script <path/to/varanus.py> \
  --varanus-python <python-for-varanus>
```

Expected startup output:

- `Standalone Varanus gate started ...`
- `Online predictive monitor listening on ...`
- `Waiting for events. Each event will print as: [EVENT N] ...`

### Offline mode

Offline is default unless `--online` is passed.

```bash
python3 monitor.py <config.yaml> "<ltl_formula>" <trace.txt> \
  --varanus-script <path/to/varanus.py> \
  --varanus-python <python-for-varanus>
```

Output ends with a `RES:` summary line.

### Diagnostics

- `--debug`: pipeline metadata and predictive-step reasons.
- `--verbose-varanus`: show raw Varanus output in terminal.
- `--verbose`: enables both `--debug` and `--verbose-varanus`.

## CLI reference

```bash
python3 monitor.py -h
```

Key options:

- `--offline` / `--online`
- `--host`, `--port`
- `--varanus-script`, `--varanus-python`
- `--varanus-host`, `--varanus-port`
- `--debug`, `--verbose-varanus`, `--verbose`

## Generated artifacts

During runs, monitor may generate:

- `buchi_automaton.hoa` (or latest Varanus HOA)
- `automaton_projected.hoa`
- `event_projection_map.json`
- `log/varanus_buchi.log`
- `log/varanus_online.log`

## Event output format

Per event, monitor prints:

```text
[EVENT N] topic=<...> parsed=<...> ... varanus=<...> ltl=<true|false|?> final=<...> source=<varanus|ltl> reason=<...>
```

Field meaning:

- `varanus`: verdict from the conformance gate.
- `ltl`: predictive LTL verdict.
- `final`: merged verdict returned to the client.
- `source`: which subsystem decided the final verdict.
- `reason`: diagnostic reason from gating or predictive step.

## Online response format

Typical successful response:

```json
{
  "status": "ok",
  "gateway_id": "pm-...",
  "verdict": "currently_true",
  "decision_source": "varanus",
  "varanus": {"verdict": "currently_true", "parsed_event": "..."},
  "projected_event": "pN",
  "predictive_verdict": "?",
  "ltl_verdict": "?",
  "predictive_reason": "undecided"
}
```

Blocked response:

```json
{
  "status": "blocked",
  "verdict": "false",
  "decision_source": "varanus",
  "reason": "varanus_rejected_or_ignored"
}
```

## Standalone tools

### HOA projection

```bash
python3 hoa_projection.py -h
```

Example:

```bash
python3 hoa_projection.py <input.hoa> \
  --trace <trace.txt> \
  --hoa-output <projected.hoa> \
  --trace-output <projected_trace.txt> \
  --map-output <event_projection_map.json>
```

### Predictive LTL prototype

```bash
python3 predictive_ltl.py -h
```

Example:

```bash
python3 predictive_ltl.py "<projected_ltl_formula>" <projected_trace.txt> --model <projected.hoa>
```

## Troubleshooting

### No `[EVENT ...]` lines

- Check producer is connected to predictive endpoint (`--host/--port`).
- Check `monitor.py` startup completed and Varanus gate is running.

### `missing_parsed_event`

- Varanus reply does not include parsed event fields.
- Ensure your Varanus websocket response includes parsed event metadata.

### Spot parse errors in projected HOA

- Run with `--verbose` for diagnostics around failing HOA lines.
- Re-run projection and inspect `automaton_projected.hoa`.

### Varanus/FDR Python mismatch

- Use `--varanus-python` matching your Varanus/FDR environment.

### Websocket disconnects (`Broken pipe`, connection closed)

- Usually means remote endpoint closed due startup/runtime failure.
- Check terminal output and `log/varanus_online.log`.

## Semantics note

Depending on model export, terminal completion may be encoded with permissive omega-tail transitions (`[true]` / `[t]`). This can over-approximate behavior after process termination for some formulas.

If you need mission/session-bounded semantics, use an explicit end-of-run convention and evaluate properties with bounded scope.
