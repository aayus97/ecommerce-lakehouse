import subprocess
import sys


PIPELINE_STEPS = [
    "jobs.08_incremental_orders_bronze_merge",
    "jobs.12_validate_and_quarantine_orders",
    "jobs.03_transform_orders_silver",
    "jobs.04_create_gold_sales_summary",
    "jobs.07_create_gold_revenue_by_category_country",
]


def run_step(script_path):
    print("=" * 80)
    print(f"Running: {script_path}")
    print("=" * 80)

    result = subprocess.run(
        [sys.executable, "-m", script_path],
        text=True
    )

    if result.returncode != 0:
        print(f"Pipeline failed at step: {script_path}")
        sys.exit(result.returncode)

    print(f"Completed: {script_path}")


def main():
    print("Starting e-commerce lakehouse pipeline")

    for step in PIPELINE_STEPS:
        run_step(step)

    print("=" * 80)
    print("Pipeline completed successfully")
    print("=" * 80)


if __name__ == "__main__":
    main()