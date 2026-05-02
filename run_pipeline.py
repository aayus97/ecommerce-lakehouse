import argparse
import importlib.util
import os
import re
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone

import yaml

from src.logger import get_logger
from src.metrics import write_metric

logger = get_logger("pipeline")

EXIT_SUCCESS = 0
EXIT_VALIDATION_FAILURE = 1
EXIT_JOB_FAILURE = 2
EXIT_CONFIG_FAILURE = 3

STEP_PATH_KEYS = {
    "ingest_orders_bronze": {
        "input": "orders_raw",
        "output": "orders_bronze",
    },
    "ingest_customers_products_bronze": {
        "input": ["customers_raw", "products_raw"],
        "output": ["customers_bronze", "products_bronze"],
    },
    "bronze_merge": {
        "input": "orders_batch_2",
        "output": "orders_bronze",
    },
    "validate_orders": {
        "input": "orders_bronze",
        "output": ["orders_validated", "orders_quarantine"],
    },
    "silver_orders": {
        "input": "orders_validated",
        "output": "orders_silver",
    },
    "silver_customers_products": {
        "input": ["customers_bronze", "products_bronze"],
        "output": ["customers_silver", "products_silver"],
    },
    "gold_daily_sales": {
        "input": "orders_silver",
        "output": "daily_sales_summary",
    },
    "gold_revenue": {
        "input": ["orders_silver", "customers_silver", "products_silver"],
        "output": "revenue_by_category_country",
    },
    "collect_gold_metrics": {
        "input": [
            "daily_sales_summary",
            "revenue_by_category_country",
            "orders_silver",
        ],
        "output": "metrics/*.jsonl",
    },
}


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


def extract_error_type(output):
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    for line in reversed(lines):
        match = re.match(r"([A-Za-z_][\w.]*?(?:Error|Exception)):", line)
        if match:
            return match.group(1).split(".")[-1]

    return None


def extract_stack_trace(output):
    if not output:
        return None

    lines = output.splitlines()
    for index, line in enumerate(lines):
        if line.startswith("Traceback (most recent call last):"):
            return "\n".join(lines[index:])

    return output.strip()


def resolve_step_paths(step_name):
    path_keys = STEP_PATH_KEYS.get(step_name)
    if not path_keys:
        return None, None

    try:
        from src.config import load_app_config, path_value, table_path

        app_config = load_app_config()

        def resolve(value):
            if isinstance(value, list):
                return [resolve(item) for item in value]
            if value == "metrics/*.jsonl":
                return f"{path_value(app_config, 'metrics')}/*.jsonl"
            return table_path(app_config, value)

        return resolve(path_keys["input"]), resolve(path_keys["output"])
    except Exception:
        return path_keys["input"], path_keys["output"]


def classify_failure_exit_code(step_name, failure_reason):
    reason = (failure_reason or "").lower()
    if step_name and "validat" in step_name:
        return EXIT_VALIDATION_FAILURE
    if "validation failed" in reason or "data quality failed" in reason:
        return EXIT_VALIDATION_FAILURE
    return EXIT_JOB_FAILURE


def run_step(step, pipeline_name, run_id):
    name = step["name"]
    module = step["module"]
    retries = step.get("retries", 0)
    last_failure_reason = None
    attempts_used = 0
    input_path, output_path = resolve_step_paths(name)

    for attempt in range(1, retries + 2):
        attempts_used = attempt
        log_context = {
            "pipeline_name": pipeline_name,
            "run_id": run_id,
            "step": name,
            "attempt": attempt,
            "max_attempts": retries + 1,
            "step_module": module,
            "input_path": input_path,
            "output_path": output_path,
        }
        failure_context = {
            **log_context,
            "failed_input_path": input_path,
            "failed_output_path": output_path,
        }
        logger.info("Running step", extra=log_context)

        start_time = time.time()
        started_at = datetime.now(timezone.utc).isoformat()
        result = subprocess.run(
            [sys.executable, "-m", module],
            text=True,
            capture_output=True,
        )
        ended_at = datetime.now(timezone.utc).isoformat()
        duration_seconds = round(time.time() - start_time, 2)
        status = "success" if result.returncode == 0 else "failed"

        if result.stdout:
            logger.info(
                "Step stdout",
                extra={**log_context, "stdout": result.stdout.strip()},
            )

        if result.stderr:
            logger.error(
                "Step stderr",
                extra={
                    **failure_context,
                    "stderr": result.stderr.strip(),
                    "stack_trace": extract_stack_trace(result.stderr),
                    "error_type": extract_error_type(result.stderr),
                    "return_code": result.returncode,
                },
            )

        stderr_lines = [line.strip() for line in (result.stderr or "").splitlines()]
        stdout_lines = [line.strip() for line in (result.stdout or "").splitlines()]
        failure_reason = None
        if result.returncode != 0:
            failure_reason = next(
                (
                    line
                    for line in reversed(stderr_lines + stdout_lines)
                    if line and not line.startswith("INFO:")
                ),
                f"Process exited with return code {result.returncode}",
            )
            last_failure_reason = failure_reason

        write_metric(
            "pipeline_steps",
            {
                "pipeline_name": pipeline_name,
                "run_id": run_id,
                "step": name,
                "module": module,
                "status": status,
                "started_at": started_at,
                "ended_at": ended_at,
                "attempt": attempt,
                "max_attempts": retries + 1,
                "retries_configured": retries,
                "retries_used": max(0, attempt - 1),
                "duration_seconds": duration_seconds,
                "return_code": result.returncode,
                "failure_reason": failure_reason,
            },
        )

        if result.returncode == 0:
            logger.info(
                "Completed step",
                extra={
                    **log_context,
                    "duration_seconds": duration_seconds,
                    "return_code": result.returncode,
                },
            )
            return {
                "success": True,
                "attempts_used": attempts_used,
                "retries_used": max(0, attempt - 1),
                "failure_reason": None,
            }

        logger.error(
            "Step failed",
            extra={
                **failure_context,
                "duration_seconds": duration_seconds,
                "return_code": result.returncode,
                "failure_reason": failure_reason,
                "error_type": extract_error_type(result.stderr or result.stdout),
                "stack_trace": extract_stack_trace(result.stderr),
            },
        )

    logger.error("Step failed after all retries", extra=failure_context)
    return {
        "success": False,
        "attempts_used": attempts_used,
        "retries_used": max(0, attempts_used - 1),
        "failure_reason": last_failure_reason,
        "exit_code": classify_failure_exit_code(name, last_failure_reason),
    }


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

    try:
        config = load_config()
    except Exception:
        logger.exception("Failed to load pipeline config")
        return EXIT_CONFIG_FAILURE

    config_errors = validate_config(config)

    if config_errors:
        for error in config_errors:
            logger.error(f"Invalid pipeline config: {error}")
        return EXIT_VALIDATION_FAILURE

    if args.validate_only:
        logger.info("Pipeline config validation passed")
        return EXIT_SUCCESS

    pipeline_name = config["pipeline"]["name"]
    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        + "_"
        + uuid.uuid4().hex[:8]
    )

    os.environ["PIPELINE_RUN_ID"] = run_id
    os.environ["PIPELINE_NAME"] = pipeline_name

    logger.info(
        "Starting pipeline",
        extra={"pipeline_name": pipeline_name, "run_id": run_id},
    )

    pipeline_start = time.time()
    pipeline_started_at = datetime.now(timezone.utc).isoformat()
    completed_steps = set()
    skipped_steps_list = []
    failed_step_name = None
    failure_reason = None
    total_steps = 0
    successful_steps = 0
    failed_steps = 0
    skipped_steps = 0
    retries_configured = 0
    retries_used = 0
    exit_code = EXIT_SUCCESS

    for step in config["pipeline"]["steps"]:
        step_name = step["name"]
        retries_configured += step.get("retries", 0)

        if not step.get("enabled", True):
            logger.info(
                "Skipping disabled step",
                extra={"run_id": run_id, "step": step_name},
            )
            skipped_steps += 1
            skipped_steps_list.append(step_name)
            continue

        dependency_ok, missing_dependency = dependencies_satisfied(
            step,
            completed_steps,
        )

        if not dependency_ok:
            logger.error(
                "Skipping step because dependency is missing",
                extra={
                    "pipeline_name": pipeline_name,
                    "run_id": run_id,
                    "step": step_name,
                    "missing_dependency": missing_dependency,
                },
            )
            skipped_steps += 1
            skipped_steps_list.append(step_name)
            failed_step_name = step_name
            failure_reason = f"Missing dependency: {missing_dependency}"
            failed_steps += 1
            exit_code = EXIT_JOB_FAILURE
            break

        total_steps += 1
        step_result = run_step(step, pipeline_name, run_id)
        retries_used += step_result["retries_used"]

        if step_result["success"]:
            completed_steps.add(step_name)
            successful_steps += 1
        else:
            failed_steps += 1
            failed_step_name = step_name
            failure_reason = step_result["failure_reason"]
            exit_code = step_result["exit_code"]
            break

    total_duration = round(time.time() - pipeline_start, 2)
    pipeline_ended_at = datetime.now(timezone.utc).isoformat()
    pipeline_status = "success" if failed_steps == 0 else "failed"

    write_metric(
        "pipeline_runs",
        {
            "pipeline_name": pipeline_name,
            "run_id": run_id,
            "status": pipeline_status,
            "duration_seconds": total_duration,
            "started_at": pipeline_started_at,
            "ended_at": pipeline_ended_at,
            "total_steps": total_steps,
            "successful_steps": successful_steps,
            "failed_steps": failed_steps,
            "skipped_steps": skipped_steps,
            "skipped_steps_list": skipped_steps_list,
            "failed_step": failed_step_name,
            "failure_reason": failure_reason,
            "retries_configured": retries_configured,
            "retries_used": retries_used,
        },
    )

    if pipeline_status == "failed":
        failed_input_path, failed_output_path = resolve_step_paths(failed_step_name)
        logger.error(
            "Pipeline failed",
            extra={
                "pipeline_name": pipeline_name,
                "run_id": run_id,
                "step": failed_step_name,
                "failed_input_path": failed_input_path,
                "failed_output_path": failed_output_path,
                "failure_reason": failure_reason,
                "duration_seconds": total_duration,
            },
        )
        return exit_code

    logger.info(
        "Pipeline completed successfully",
        extra={
            "pipeline_name": pipeline_name,
            "run_id": run_id,
            "duration_seconds": total_duration,
        },
    )
    return EXIT_SUCCESS


if __name__ == "__main__":
    sys.exit(main())
