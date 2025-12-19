from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import logging
from datetime import datetime
from typing import Optional
import os
from dotenv import load_dotenv

from .scraper.pipeline import run_full_pipeline

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class SchedulerManager:
    """Manages the cron job scheduler for scraping tasks."""
    
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.job_id = "autoscout_scraping_job"
        
        # Get schedule from environment
        self.schedule_hour = int(os.getenv("SCRAPE_AT_HOUR", 2))
        self.schedule_minute = int(os.getenv("SCRAPE_AT_MINUTE", 0))
        self.interval_hours = int(os.getenv("SCRAPE_INTERVAL_HOURS", 12))
    
    def start_scheduler(self):
        """Start the scheduler and add the scraping job."""
        try:
            if not self.scheduler.running:
                self.scheduler.start()
                logger.info("Scheduler started")
                
                # Add the scraping job
                self._add_scraping_job()
                
        except Exception as e:
            logger.error(f"Error starting scheduler: {e}")
            raise
    
    def _add_scraping_job(self):
        """Add the scraping job to the scheduler."""
        # Remove existing job if it exists
        if self.scheduler.get_job(self.job_id):
            self.scheduler.remove_job(self.job_id)
        
        # Add new job with cron schedule
        self.scheduler.add_job(
            func=self._run_scheduled_scraping,
            trigger=CronTrigger(
                hour=self.schedule_hour,
                minute=self.schedule_minute
            ),
            id=self.job_id,
            name="AutoScout24 Scraping Pipeline",
            replace_existing=True,
            coalesce=True,
            max_instances=1
        )
        
        logger.info(f"Scheduled scraping job for {self.schedule_hour:02d}:{self.schedule_minute:02d} daily")
    
    def _run_scheduled_scraping(self):
        """Wrapper function for scheduled scraping."""
        try:
            logger.info(f"Starting scheduled scraping at {datetime.now()}")
            run_full_pipeline()
            logger.info(f"Completed scheduled scraping at {datetime.now()}")
        except Exception as e:
            logger.error(f"Error in scheduled scraping: {e}")
    
    def update_schedule(self, hour: Optional[int] = None, minute: Optional[int] = None, 
                       interval_hours: Optional[int] = None):
        """Update the scraping schedule."""
        if hour is not None:
            self.schedule_hour = hour
        if minute is not None:
            self.schedule_minute = minute
        if interval_hours is not None:
            self.interval_hours = interval_hours
        
        # Update the job
        self._add_scraping_job()
        
        logger.info(f"Schedule updated to {self.schedule_hour:02d}:{self.schedule_minute:02d}")
    
    def get_scheduled_jobs(self):
        """Get all scheduled jobs."""
        return self.scheduler.get_jobs()
    
    def is_running(self):
        """Check if scheduler is running."""
        return self.scheduler.running
    
    def shutdown_scheduler(self):
        """Shutdown the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Scheduler shutdown")