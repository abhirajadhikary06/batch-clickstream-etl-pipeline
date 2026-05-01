from datetime import datetime, timezone

from dagster import RunRequest, ScheduleDefinition

from orchestration.dagster_project.jobs.clickstream_pipeline_job import clickstream_pipeline_job
from src.observability.openobserve_logger import log_pipeline_event


def _schedule_tick(context) -> RunRequest:
    scheduled_dt = getattr(context, "scheduled_execution_time", None)
    if scheduled_dt is None:
        scheduled_dt = datetime.now(timezone.utc)

    scheduled_at = scheduled_dt.isoformat()

    # Best-effort observability: schedule execution should never fail due to logging.
    log_pipeline_event(
        stage="orchestration_schedule",
        status="triggered",
        details={
            "schedule_name": "every_30_min_schedule",
            "scheduled_execution_time": scheduled_at,
            "source": "dagster",
        },
    )

    return RunRequest(
        run_key=f"every_30_min_schedule:{scheduled_at}",
        tags={
            "trigger": "schedule",
            "schedule_name": "every_30_min_schedule",
        },
    )


every_30_min_schedule = ScheduleDefinition(
    job=clickstream_pipeline_job,
    cron_schedule="*/30 * * * *",
    execution_timezone="UTC",
    execution_fn=_schedule_tick,
)
