from dagster import Definitions

from orchestration.dagster_project.jobs.clickstream_pipeline_job import clickstream_pipeline_job
from orchestration.dagster_project.schedules.every_30_min_schedule import every_30_min_schedule


defs = Definitions(
    jobs=[clickstream_pipeline_job],
    schedules=[every_30_min_schedule],
)
