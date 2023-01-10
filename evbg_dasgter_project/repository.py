import os

from dagster_dbt import dbt_cli_resource
from dagster_duckdb_pandas import duckdb_pandas_io_manager
from evbg_dasgter_project import assets
from evbg_dasgter_project.assets import DBT_PROFILES, DBT_PROJECT_PATH

from dagster import load_assets_from_package_module, repository, with_resources

from dagster import (
    load_assets_from_package_module,
    repository,
    define_asset_job,
    ScheduleDefinition,
)

daily_job = define_asset_job(name="daily_refresh", selection="*")
daily_schedule = ScheduleDefinition(
    job=daily_job,
    cron_schedule="@daily",
)


@repository
def evbg_dbt_dagster():
    return with_resources(
        load_assets_from_package_module(assets),
        {
            "dbt": dbt_cli_resource.configured(
                {
                    "project_dir": DBT_PROJECT_PATH,
                    "profiles_dir": DBT_PROFILES,
                },
            ),
            "io_manager": duckdb_pandas_io_manager.configured(
                {"database": os.path.join(DBT_PROJECT_PATH, "tutorial.duckdb")}
            ),
        },
    )
