import argparse
import importlib.util
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone

import yaml

from src.logger import get_logger
from src.metrics import write_metric

logger = get_logger("pipeline")


def load_config(path="configs/pipeline.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def validate_config(config):
    errors = []

    pipeline = config.get("pipeline") if isinstance(config, dict) else None
    if not isinstance(pipeline, dict):
        return ["Missing top-level pipeline configuration"]

    if not pipeline.get("name"):
        errors.append("pipeline.name is required")

    steps = pipeline.get("steps")
    if not isinstance(steps, list) or not steps:
        errors.append("pipeline.steps must be a non-empty list")
        return errors

    step_names = []
    step_name_set = set()

    for index, step in enumerate(steps):
        if not isinstance(step, dict):
            errors.append(f"Step {index} must be a mapping")
            continue

        name = step.get("name")
        module = step.get("module")
        retries = step.get("retries", 0)
        depends_on = step.get("depends_on", [])

        if not name:
            errors.append(f"Step {index} is missing name")
        elif name in step_name_set:
            errors.append(f"Duplicate step name: {name}")
        else:
            step_names.append(name)
            step_name_set.add(name)

        if not module:
            errors.append(f"Step {name or index} is missing module")
        elif importlib.util.find_spec(module) is None:
            errors.append(f"Step {name or index} module not found: {module}")

        if "enabled" in step and not isinstance(step["enabled"], bool):
            errors.append(f"Step {name or index} enabled must be true or false")

        if not isinstance(retries, int) or retries < 0:
            errors.append(
                f"Step {name or index} retries must be a non-negative integer"
            )

        if not isinstance(depends_on, list):
            errors.append(f"Step {name or index} depends_on must be a list")
            continue

        for dependency in depends_on:
            if dependency not in step_name_set:
                errors.append(
                    f"Step {name or index} depends on unknown or later step: {dependency}"
                )

    errors.extend(find_dependency_cycles(steps))
    return errors


def find_dependency_cycles(steps):
    graph = {
        step["name"]: step.get("depends_on", [])
        for step in steps
        if isinstance(step, dict) and step.get("name")
    }
    visited = set()
    visiting = set()
    cycles = []

    def visit(step_name, path):
        if step_name in visiting:
            cycle_start = path.index(step_name)
            cycle = " -> ".join(path[cycle_start:] + [step_name])
            cycles.append(f"Dependency cycle detected: {cycle}")
            return

        if step_name in visited:
            return

        visiting.add(step_name)
        for dependency in graph.get(step_name, []):
            if dependency in graph:
                visit(dependency, path + [dependency])
        visiting.remove(step_name)
        visited.add(step_name)

    for step_name in graph:
        visit(step_name, [step_name])

    return cycles


def run_step(step, pipeline_name, run_id):
    name = step["name"]
    module = step["module"]
    retries = step.get("retries", 0)

    for attempt in range(1, retries + 2):
        logger.info("=" * 80)
        logger.info(f"Running step: {name} ({module})")
        logger.info(f"Attempt {attempt} of {retries + 1}")

        start_time = time.time()
        result = subprocess.run([sys.executable, "-m", module], text=True)
        duration_seconds = round(time.time() - start_time, 2)
        status = "success" if result.returncode == 0 else "failed"

        write_metric(
            "pipeline_steps",
            {
                "pipeline_name": pipeline_name,
                "run_id": run_id,
                "step": name,
                "module": module,
                "status": status,
                "attempt": attempt,
                "max_attempts": retries + 1,
                "duration_seconds": duration_seconds,
                "return_code": result.returncode,
            },
        )

        if result.returncode == 0:
            logger.info(f"Completed step: {name} in {duration_seconds} seconds")
            return True

        logger.error(f"Step failed: {name} on attempt {attempt}")

    logger.error(f"Step failed after all retries: {name}")
    return False


def dependencies_satisfied(step, completed_steps):
    dependencies = step.get("depends_on", [])

    for dependency in dependencies:
        if dependency not in completed_steps:
            return False, dependency

    return True, None


def main():
    parser = argparse.ArgumentParser(description="Run the ecommerce lakehouse pipeline")
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate pipeline configuration without running pipeline steps",
    )
    args = parser.parse_args()

    config = load_config()
    config_errors = validate_config(config)

    if config_errors:
        for error in config_errors:
            logger.error(f"Invalid pipeline config: {error}")
        sys.exit(1)

    if args.validate_only:
        logger.info("Pipeline config validation passed")
        return

    pipeline_name = config["pipeline"]["name"]
    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        + "_"
        + uuid.uuid4().hex[:8]
    )

    os.environ["PIPELINE_RUN_ID"] = run_id
    os.environ["PIPELINE_NAME"] = pipeline_name

    logger.info(f"Starting pipeline: {pipeline_name}")
    logger.info(f"Run ID: {run_id}")

    pipeline_start = time.time()
    completed_steps = set()
    skipped_steps_list = []
    failed_step_name = None
    total_steps = 0
    successful_steps = 0
    failed_steps = 0
    skipped_steps = 0

    for step in config["pipeline"]["steps"]:
        step_name = step["name"]

        if not step.get("enabled", True):
            logger.info(f"Skipping disabled step: {step_name}")
            skipped_steps += 1
            skipped_steps_list.append(step_name)
            continue

        dependency_ok, missing_dependency = dependencies_satisfied(
            step,
            completed_steps,
        )

        if not dependency_ok:
            logger.error(
                f"Skipping step {step_name}. Missing dependency: {missing_dependency}"
            )
            skipped_steps += 1
            skipped_steps_list.append(step_name)
            failed_step_name = step_name
            failed_steps += 1
            break

        total_steps += 1
        success = run_step(step, pipeline_name, run_id)

        if success:
            completed_steps.add(step_name)
            successful_steps += 1
        else:
            failed_steps += 1
            failed_step_name = step_name
            break

    total_duration = round(time.time() - pipeline_start, 2)
    pipeline_status = "success" if failed_steps == 0 else "failed"

    write_metric(
        "pipeline_runs",
        {
            "pipeline_name": pipeline_name,
            "run_id": run_id,
            "status": pipeline_status,
            "duration_seconds": total_duration,
            "total_steps": total_steps,
            "successful_steps": successful_steps,
            "failed_steps": failed_steps,
            "skipped_steps": skipped_steps,
            "skipped_steps_list": skipped_steps_list,
            "failed_step": failed_step_name,
        },
    )

    if pipeline_status == "failed":
        logger.error(f"Pipeline failed at step: {failed_step_name}")
        sys.exit(1)

    logger.info(f"Pipeline completed successfully in {total_duration} seconds")


if __name__ == "__main__":
    main()
