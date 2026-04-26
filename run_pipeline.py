# import subprocess
# import sys
# import yaml
# import argparse

# def load_config(path="configs/pipeline.yaml"):
#     with open(path, "r") as f:
#         return yaml.safe_load(f)


# def run_step(step):
#     name = step["name"]
#     module = step["module"]

#     print("=" * 80)
#     print(f"Running step: {name} ({module})")
#     print("=" * 80)

#     result = subprocess.run(
#         [sys.executable, "-m", module],
#         text=True
#     )

#     if result.returncode != 0:
#         print(f"Pipeline failed at step: {name}")
#         sys.exit(result.returncode)

#     print(f"Completed step: {name}")


# def main():
#     config = load_config()

#     parser = argparse.ArgumentParser()
#     parser.add_argument("--skip-gold", action="store_true")
#     args = parser.parse_args()

#     print(f"Starting pipeline: {config['pipeline']['name']}")

#     # for step in config["pipeline"]["steps"]:
#     #     if not step.get("enabled", True):
#     #         print(f"Skipping step: {step['name']}")
#     #         continue

#     #     run_step(step)
        
#     for step in config["pipeline"]["steps"]:
#         if args.skip_gold and "gold" in step["name"]:
#             print(f"Skipping gold step: {step['name']}")
#             continue
#         run_step(step)
    

#     print("=" * 80)
#     print("Pipeline completed successfully")
#     print("=" * 80)


# if __name__ == "__main__":
#     main()


import subprocess
import sys
import time
import yaml

from src.logger import get_logger
from src.metrics import write_metric


logger = get_logger("pipeline")


def load_config(path="configs/pipeline.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def run_step(step):
    name = step["name"]
    module = step["module"]

    logger.info("=" * 80)
    logger.info(f"Running step: {name} ({module})")

    start_time = time.time()

    result = subprocess.run(
        [sys.executable, "-m", module],
        text=True
    )

    duration_seconds = round(time.time() - start_time, 2)

    if result.returncode != 0:
        logger.error(f"Pipeline failed at step: {name}")

        write_metric(
            "pipeline_steps",
            {
                "step": name,
                "module": module,
                "status": "failed",
                "duration_seconds": duration_seconds,
                "return_code": result.returncode,
            },
        )

        sys.exit(result.returncode)

    logger.info(f"Completed step: {name} in {duration_seconds} seconds")

    write_metric(
        "pipeline_steps",
        {
            "step": name,
            "module": module,
            "status": "success",
            "duration_seconds": duration_seconds,
            "return_code": result.returncode,
        },
    )


def main():
    config = load_config()

    pipeline_name = config["pipeline"]["name"]

    logger.info(f"Starting pipeline: {pipeline_name}")

    pipeline_start = time.time()

    for step in config["pipeline"]["steps"]:
        if not step.get("enabled", True):
            logger.info(f"Skipping step: {step['name']}")
            continue

        run_step(step)

    total_duration = round(time.time() - pipeline_start, 2)

    logger.info(f"Pipeline completed successfully in {total_duration} seconds")

    write_metric(
        "pipeline_runs",
        {
            "pipeline_name": pipeline_name,
            "status": "success",
            "duration_seconds": total_duration,
        },
    )


if __name__ == "__main__":
    main()