from dagster import AssetSelection, Definitions, ScheduleDefinition, define_asset_job

from orchestration.dagster_assets import ASSET_GROUP, lakehouse_assets

ecommerce_lakehouse_job = define_asset_job(
    name="ecommerce_lakehouse_job",
    selection=AssetSelection.groups(ASSET_GROUP),
)

daily_ecommerce_lakehouse_schedule = ScheduleDefinition(
    name="daily_ecommerce_lakehouse_schedule",
    job=ecommerce_lakehouse_job,
    cron_schedule="0 6 * * *",
)


defs = Definitions(
    assets=lakehouse_assets,
    jobs=[ecommerce_lakehouse_job],
    schedules=[daily_ecommerce_lakehouse_schedule],
)
