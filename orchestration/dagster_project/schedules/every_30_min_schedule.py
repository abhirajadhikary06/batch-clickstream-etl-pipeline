from dagster import ScheduleDefinition

from orchestration.dagster_project.jobs.clickstream_pipeline_job import clickstream_pipeline_job


every_30_min_schedule = ScheduleDefinition(
    job=clickstream_pipeline_job,
    cron_schedule="*/30 * * * *",
    execution_timezone="UTC",
)
