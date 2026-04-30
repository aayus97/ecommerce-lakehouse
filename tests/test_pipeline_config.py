import yaml

from run_pipeline import validate_config


def load_config():
    with open("configs/pipeline.yaml", "r") as f:
        return yaml.safe_load(f)


def test_pipeline_has_name():
    config = load_config()

    assert "pipeline" in config
    assert "name" in config["pipeline"]
    assert config["pipeline"]["name"]


def test_pipeline_has_steps():
    config = load_config()

    steps = config["pipeline"]["steps"]

    assert isinstance(steps, list)
    assert len(steps) > 0


def test_step_names_are_unique():
    config = load_config()

    steps = config["pipeline"]["steps"]
    step_names = [step["name"] for step in steps]

    assert len(step_names) == len(set(step_names))


def test_each_step_has_required_fields():
    config = load_config()

    for step in config["pipeline"]["steps"]:
        assert "name" in step
        assert "module" in step
        assert "enabled" in step


def test_dependencies_exist():
    config = load_config()

    steps = config["pipeline"]["steps"]
    step_names = {step["name"] for step in steps}

    for step in steps:
        for dependency in step.get("depends_on", []):
            assert dependency in step_names


def test_pipeline_config_passes_validation():
    config = load_config()

    assert validate_config(config) == []


def test_duplicate_step_names_fail_validation():
    config = {
        "pipeline": {
            "name": "test",
            "steps": [
                {
                    "name": "bronze_merge",
                    "module": "jobs.08_incremental_orders_bronze_merge",
                    "enabled": True,
                },
                {
                    "name": "bronze_merge",
                    "module": "jobs.12_validate_and_quarantine_orders",
                    "enabled": True,
                },
            ],
        }
    }

    assert any("Duplicate step name" in error for error in validate_config(config))


def test_invalid_retry_value_fails_validation():
    config = load_config()
    config["pipeline"]["steps"][0]["retries"] = -1

    assert any("retries" in error for error in validate_config(config))


def test_unknown_dependency_fails_validation():
    config = load_config()
    config["pipeline"]["steps"][1]["depends_on"] = ["missing_step"]

    assert any("depends on unknown" in error for error in validate_config(config))


def test_dependency_cycle_fails_validation():
    config = {
        "pipeline": {
            "name": "test",
            "steps": [
                {
                    "name": "step_a",
                    "module": "jobs.08_incremental_orders_bronze_merge",
                    "enabled": True,
                    "depends_on": ["step_b"],
                },
                {
                    "name": "step_b",
                    "module": "jobs.12_validate_and_quarantine_orders",
                    "enabled": True,
                    "depends_on": ["step_a"],
                },
            ],
        }
    }

    assert any("Dependency cycle" in error for error in validate_config(config))
