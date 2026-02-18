"""Azure Functions entry point for scheduled QBO ETL pipeline."""

import logging

import azure.functions as func

from config.settings import Settings
from orchestrator.pipeline import run_pipeline_all_clients

app = func.FunctionApp()


@app.timer_trigger(
    schedule="0 0 11 * * *",  # Daily at 11:00 UTC (6:00 AM ET)
    arg_name="timer",
    run_on_startup=False,
)
def daily_qbo_etl(timer: func.TimerRequest) -> None:
    """Daily ETL: pull QBO data for all clients and load into SQL."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger = logging.getLogger(__name__)

    if timer.past_due:
        logger.warning("Timer trigger is past due, running now")

    logger.info("Daily QBO ETL triggered")

    try:
        settings = Settings()
        run_pipeline_all_clients(settings)
        logger.info("Daily QBO ETL completed successfully")
    except Exception as e:
        logger.error(f"Daily QBO ETL failed: {e}", exc_info=True)
        raise  # Re-raise so Azure Functions marks the run as failed
