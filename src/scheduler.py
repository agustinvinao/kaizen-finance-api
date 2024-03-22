import schedule
import logging
import time
from jobs.fetch_ticks import job as job_fetch_ticks


logging.basicConfig()
schedule_logger = logging.getLogger('schedule')
schedule_logger.setLevel(level=logging.DEBUG)

# Create a new scheduler
scheduler = schedule.Scheduler()

# Add jobs to the created scheduler
schedule.every().second.until("22:30").do(job_fetch_ticks, logging=schedule_logger)
# .at(":01")

if __name__ == "__main__":
  schedule.run_all()
  schedule.clear()

  # while True:
  #   # run_pending needs to be called on every scheduler
  #   schedule_logger.info("run_pending")
  #   scheduler.run_pending()
  #   time.sleep(60)