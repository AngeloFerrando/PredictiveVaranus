#!/usr/bin/env python
# coding=utf-8

from __future__ import print_function

import argparse
import json
import os
import sys
import traceback
from contextlib import contextmanager

import yaml


PROTOCOL_STDOUT = sys.stdout


def log(message):
    sys.stderr.write(message + "\n")
    sys.stderr.flush()


@contextmanager
def redirect_prints_to_stderr():
    original_stdout = sys.stdout
    sys.stdout = sys.stderr
    try:
        yield
    finally:
        sys.stdout = original_stdout


def load_config(config_path):
    config_path = os.path.abspath(config_path)
    config_dir = os.path.dirname(config_path)
    with open(config_path, "r") as handle:
        config = yaml.safe_load(handle)
    model_path = os.path.abspath(os.path.join(config_dir, config["model"]))
    event_map_path = None
    if config.get("map") is not None:
        event_map_path = os.path.abspath(os.path.join(config_dir, config["map"]))
    return {
        "config_path": config_path,
        "model_path": model_path,
        "event_map_path": event_map_path,
        "main_process": config["main_process"],
        "common_alphabet": config.get("common_alphabet"),
        "mode": config.get("mode", "permissive"),
    }


def build_monitor(varanus_dir, config_info):
    log("bridge: importing Varanus monitor modules from " + str(varanus_dir))
    with redirect_prints_to_stderr():
        sys.path.insert(0, varanus_dir)
        from monitor import Monitor  # pylint: disable=import-error

        log("bridge: creating Monitor for model " + str(config_info["model_path"]))
        monitor = Monitor(
            config_info["model_path"],
            config_info["config_path"],
            config_info["event_map_path"],
            config_info["mode"],
        )
        log("bridge: building CSP state machine for " + str(config_info["main_process"]))
        if config_info["common_alphabet"]:
            monitor.build_state_machine(config_info["main_process"], config_info["common_alphabet"])
        else:
            monitor.build_state_machine(config_info["main_process"])
        log("bridge: starting state machine")
        monitor.process.start()
    log("bridge: ready")
    return monitor


def send_message(payload):
    PROTOCOL_STDOUT.write(json.dumps(payload) + "\n")
    PROTOCOL_STDOUT.flush()


def main():
    parser = argparse.ArgumentParser(description="Benchmark bridge for Varanus state-machine gating.")
    parser.add_argument("--varanus-dir", required=True)
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    try:
        log("bridge: loading config " + str(args.config))
        config_info = load_config(args.config)
        monitor = build_monitor(os.path.abspath(args.varanus_dir), config_info)
        send_message({"status": "ready"})
    except Exception as error:
        print("bridge_startup_error: {0}".format(error), file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return 1

    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        try:
            request = json.loads(line)
            event_name = str(request["event"])
            log("bridge: received event " + event_name)
            with redirect_prints_to_stderr():
                log("bridge: before transition " + event_name)
                resulting_state = monitor.process.transition(event_name)
                log("bridge: after transition " + event_name)
                passed = monitor.check_result(event_name, resulting_state)
            payload = {
                "event": event_name,
                "parsed_event": event_name,
            }
            if passed:
                payload["verdict"] = "currently_true" if resulting_state is not None else "ignored"
            else:
                payload["verdict"] = "false"
            send_message(payload)
            log("bridge: reply sent for " + event_name + " verdict=" + str(payload["verdict"]))
        except Exception as error:
            print("bridge_event_error: {0}".format(error), file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            send_message({"verdict": "error", "error": str(error)})
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
