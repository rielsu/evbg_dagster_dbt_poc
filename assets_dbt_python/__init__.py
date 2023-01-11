import os

from assets_dbt_python.assets import forecasting, raw_data
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project
from dagster_duckdb_pandas import duckdb_pandas_io_manager

import pandas as pd
import plotly.express as px
from dagster_dbt import load_assets_from_dbt_project

from dagster import AssetIn, MetadataValue, asset, file_relative_path
from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    fs_io_manager,
    load_assets_from_package_module,
)
from dagster._utils import file_relative_path

DBT_PROJECT_DIR = file_relative_path(__file__, "../evbg_dbt_project")
DBT_PROFILES_DIR = file_relative_path(__file__, "../evbg_dbt_project/config")

# all assets live in the default dbt_schema
dbt_assets = load_assets_from_dbt_project(
    DBT_PROJECT_DIR,
    DBT_PROFILES_DIR,
    # prefix the output assets based on the database they live in plus the name of the schema
    key_prefix=["snowflake", "dbt_schema"],
    # prefix the source assets based on just the database
    # (dagster populates the source schema information automatically)
    source_key_prefix=["snowflake"],
    use_build_command=False,
)

@asset(
   key_prefix=["snowflake", "dbt_schema", "stg_subscription_threshold_triggers"],
   group_name="staging",
)
def stg_subscription_threshold_triggers_chart(context, stg_subscription_threshold_triggers: pd.DataFrame):
   fig = px.histogram(stg_subscription_threshold_triggers, x="number_of_orders")
   fig.update_layout(bargap=0.2)
   save_chart_path = file_relative_path(__file__, "order_count_chart.html")
   fig.write_html(save_chart_path, auto_open=True)

   context.add_output_metadata({"plot_url": MetadataValue.url("file://" + save_chart_path)})


# define jobs as selections over the larger graph
everything_job = define_asset_job("everything_everywhere_job", selection="*")
# forecast_job = define_asset_job("refresh_forecast_model_job", selection="*order_forecast_model")

resources = {
    # this io_manager allows us to load dbt models as pandas dataframes
    "io_manager": duckdb_pandas_io_manager.configured(
        {"database": os.path.join(DBT_PROJECT_DIR, "example.duckdb")}
    ),
    # this io_manager is responsible for storing/loading our pickled machine learning model
    "model_io_manager": fs_io_manager,
    # this resource is used to execute dbt cli commands
    "dbt": dbt_cli_resource.configured(
        {"project_dir": DBT_PROJECT_DIR, "profiles_dir": DBT_PROFILES_DIR}
    ),
}

defs = Definitions(
    #assets=[*dbt_assets, *raw_data_assets, *forecasting_assets],
    assets=[*dbt_assets,stg_subscription_threshold_triggers_chart],
    resources=resources,
    schedules=[
        ScheduleDefinition(job=everything_job, cron_schedule="*/15 * * * *"),
        #ScheduleDefinition(job=forecast_job, cron_schedule="@daily"),
    ],
)

