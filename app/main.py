from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
from datetime import datetime
import os

from .scheduler import SchedulerManager
from .scraper.pipeline import run_full_pipeline, run_link_scraping_only, run_data_scraping_only
from .database.mongo_handler import MongoDBHandler

# Configure logging
# In app/main.py, replace your logging.basicConfig with this:
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        # This prevents the Unicode error for the log file
        logging.FileHandler('scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup and shutdown events."""
    # Startup
    logger.info("Starting AutoScout24 Scraper API")
    scheduler_manager = SchedulerManager()
    scheduler_manager.start_scheduler()
    
    # Store scheduler in app state
    app.state.scheduler = scheduler_manager
    
    yield
    
    # Shutdown
    logger.info("Shutting down AutoScout24 Scraper API")
    scheduler_manager.shutdown_scheduler()

# Create FastAPI app
app = FastAPI(
    title="AutoScout24 Scraper API",
    description="API for scheduling and running AutoScout24 vehicle scraping jobs",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "AutoScout24 Scraper API",
        "version": "1.0.0",
        "endpoints": {
            "trigger_scrape": "/api/scrape/trigger",
            "status": "/api/scrape/status",
            "last_run": "/api/scrape/last-run",
            "stats": "/api/scrape/stats",
            "schedule": "/api/schedule"
        }
    }

@app.post("/api/scrape/trigger")
async def trigger_scraping(background_tasks: BackgroundTasks, full_pipeline: bool = True):
    """
    Trigger the scraping pipeline manually.
    
    Args:
        full_pipeline: If True, run all steps. If False, run only link scraping.
    """
    try:
        if full_pipeline:
            background_tasks.add_task(run_full_pipeline)
            return {
                "status": "success",
                "message": "Full scraping pipeline started in background",
                "timestamp": datetime.now().isoformat()
            }
        else:
            background_tasks.add_task(run_link_scraping_only)
            return {
                "status": "success",
                "message": "Link scraping only started in background",
                "timestamp": datetime.now().isoformat()
            }
    except Exception as e:
        logger.error(f"Error triggering scraping: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/scrape/status")
async def get_scrape_status():
    """Get current status of the scraper."""
    try:
        # Check if scheduler is running
        scheduler = app.state.scheduler
        jobs = scheduler.get_scheduled_jobs()
        
        # Check if any scraping process is currently running
        # This would require tracking running jobs in a more sophisticated way
        # For now, we'll return scheduler status
        
        return {
            "scheduler_running": scheduler.is_running(),
            "scheduled_jobs": len(jobs),
            "next_run": str(jobs[0].next_run_time) if jobs else None,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting scrape status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/scrape/last-run")
async def get_last_run_stats():
    """Get statistics from the last scraping run."""
    try:
        mongo = MongoDBHandler()
        latest_record = mongo.get_latest_scrape_stats()
        
        if latest_record:
            return {
                "status": "success",
                "data": latest_record
            }
        else:
            return {
                "status": "success",
                "message": "No previous runs found",
                "data": None
            }
    except Exception as e:
        logger.error(f"Error getting last run stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/scrape/stats")
async def get_scraping_stats(days: int = 7):
    """Get scraping statistics for the last N days."""
    try:
        mongo = MongoDBHandler()
        stats = mongo.get_scraping_stats(days)
        
        return {
            "status": "success",
            "days": days,
            "data": stats
        }
    except Exception as e:
        logger.error(f"Error getting scraping stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/schedule/update")
async def update_schedule(hour: int = None, minute: int = None, interval_hours: int = None):
    """Update the scraping schedule."""
    try:
        scheduler = app.state.scheduler
        scheduler.update_schedule(hour, minute, interval_hours)
        
        return {
            "status": "success",
            "message": "Schedule updated successfully",
            "new_schedule": {
                "hour": hour,
                "minute": minute,
                "interval_hours": interval_hours
            }
        }
    except Exception as e:
        logger.error(f"Error updating schedule: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "autoscout-scraper"
    }