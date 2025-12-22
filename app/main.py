from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
from datetime import datetime
import os
import json
from pathlib import Path

from .scheduler import SchedulerManager
from .scraper.pipeline import run_full_pipeline, run_link_scraping_only, run_data_scraping_only
from .database.mongo_handler import MongoDBHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Track test runs
class TestTracker:
    def __init__(self):
        self.last_test_run = None
        self.last_scraped_links = []

# Global test tracker
test_tracker = TestTracker()

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

def find_latest_link_file():
    """Find the latest link file created by the scraper."""
    possible_files = [
        "abc3.txt",
        "links.txt",
        "data/links.txt",
        "scraped_links.txt",
        "newest_links.txt"
    ]
    
    for file_path in possible_files:
        if os.path.exists(file_path):
            return file_path
    
    # Also check for any .txt file in current directory
    txt_files = list(Path(".").glob("*.txt"))
    if txt_files:
        # Get the most recently modified
        latest_file = max(txt_files, key=lambda x: x.stat().st_mtime)
        return str(latest_file)
    
    return None

def get_first_link_from_file(file_path):
    """Get the first link from a text file."""
    try:
        if not file_path or not os.path.exists(file_path):
            return None
        
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f.readlines() if line.strip()]
        
        if not lines:
            return None
        
        first_line = lines[0]
        
        # If it's just a UUID, construct full URL
        if len(first_line) <= 50 and '-' in first_line:  # Looks like a UUID
            return f"https://www.autoscout24.be/fr/details/{first_line}"
        
        return first_line  # Already a full URL
    
    except Exception as e:
        logger.error(f"Error reading link file: {e}")
        return None

@app.get("/")
async def root():
    """Root endpoint with API information."""
    endpoints = {
        "trigger_scrape": "/api/scrape/trigger",
        "status": "/api/scrape/status",
        "last_run": "/api/scrape/last-run",
        "stats": "/api/scrape/stats",
        "schedule": "/api/schedule",
        "test_cron": "/api/test/cron-job",
        "test_first_link": "/api/test/first-link",
        "test_check_links": "/api/test/check-links"
    }
    
    # Check if link file exists
    link_file = find_latest_link_file()
    if link_file:
        endpoints["current_link_file"] = f"File: {link_file}"
    
    return {
        "message": "AutoScout24 Scraper API",
        "version": "1.0.0",
        "endpoints": endpoints
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
        
        # Check link file
        link_file = find_latest_link_file()
        first_link = None
        link_count = 0
        
        if link_file:
            try:
                with open(link_file, 'r', encoding='utf-8') as f:
                    links = [line.strip() for line in f.readlines() if line.strip()]
                    link_count = len(links)
                    if links:
                        first_link = links[0]
            except:
                pass
        
        return {
            "scheduler_running": scheduler.is_running(),
            "scheduled_jobs": len(jobs),
            "next_run": str(jobs[0].next_run_time) if jobs else None,
            "link_file": link_file,
            "links_found": link_count,
            "first_link_available": first_link is not None,
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
    # Check if link file exists
    link_file = find_latest_link_file()
    
    health_info = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "autoscout-scraper",
        "api_running": True,
    }
    
    if link_file:
        health_info["link_file_exists"] = True
        health_info["link_file"] = link_file
        try:
            with open(link_file, 'r', encoding='utf-8') as f:
                link_count = len([line.strip() for line in f.readlines() if line.strip()])
                health_info["link_count"] = link_count
        except:
            health_info["link_file_readable"] = False
    else:
        health_info["link_file_exists"] = False
    
    return health_info

# ==================== NEW TEST ENDPOINTS ====================

@app.post("/api/test/cron-job")
async def test_cron_job(background_tasks: BackgroundTasks):
    """
    Test endpoint to manually trigger a cron job.
    This simulates what the actual cron job would do.
    """
    try:
        logger.info("Testing cron job - starting link scraping")
        
        # Track test run
        test_tracker.last_test_run = {
            "start_time": datetime.now(),
            "status": "running",
            "type": "link_scraping_only"
        }
        
        # Run link scraping only (first step)
        background_tasks.add_task(run_full_pipeline)
        
        return {
            "status": "success",
            "message": "Test cron job started (link scraping only)",
            "start_time": datetime.now().isoformat(),
            "monitor_endpoint": "/api/test/first-link",
            "check_links_endpoint": "/api/test/check-links",
            "note": "Check the endpoints above for results in 30-60 seconds"
        }
    except Exception as e:
        logger.error(f"Error testing cron job: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/test/first-link")
async def get_first_link():
    """
    Get the FIRST link scraped from the latest test run.
    This reads from your text file (abc3.txt or similar).
    """
    try:
        # Find the link file
        link_file = find_latest_link_file()
        
        if not link_file:
            return {
                "status": "not_found",
                "message": "No link file found. Run the scraper first.",
                "searched_files": ["abc3.txt", "links.txt", "*.txt"],
                "suggestion": "Run: POST /api/test/cron-job"
            }
        
        # Get the first link
        first_link = get_first_link_from_file(link_file)
        
        if not first_link:
            # Check if file is empty
            with open(link_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()
            
            if not content:
                return {
                    "status": "empty",
                    "message": f"Link file '{link_file}' exists but is empty",
                    "file": link_file,
                    "suggestion": "The scraper may still be running or failed"
                }
            
            # File has content but we couldn't parse first link
            return {
                "status": "error",
                "message": f"Could not extract first link from '{link_file}'",
                "file": link_file,
                "file_content_preview": content[:100] + "..." if len(content) > 100 else content
            }
        
        # Count total links
        with open(link_file, 'r', encoding='utf-8') as f:
            all_links = [line.strip() for line in f.readlines() if line.strip()]
        
        return {
            "status": "success",
            "message": "First scraped link found",
            "test_status": test_tracker.last_test_run,
            "link_file": link_file,
            "total_links": len(all_links),
            "first_link": first_link,
            "first_link_uuid": first_link.split('/')[-1] if '/' in first_link else first_link,
            "other_links_count": len(all_links) - 1,
            "sample_other_links": all_links[1:4] if len(all_links) > 1 else [],
            "timestamp": datetime.now().isoformat(),
            "verification": "✅ This is the first link that would be processed in the full pipeline"
        }
        
    except Exception as e:
        logger.error(f"Error getting first link: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/test/check-links")
async def check_links_file():
    """
    Check the current state of the links file.
    """
    try:
        link_file = find_latest_link_file()
        
        if not link_file:
            return {
                "status": "not_found",
                "message": "No link file found",
                "scraper_status": "Not run yet",
                "action_required": "Run the scraper first: POST /api/test/cron-job"
            }
        
        # Get file info
        file_stats = os.stat(link_file)
        file_size = file_stats.st_size
        last_modified = datetime.fromtimestamp(file_stats.st_mtime)
        
        # Read file content
        with open(link_file, 'r', encoding='utf-8') as f:
            all_lines = f.readlines()
        
        # Process links
        links = [line.strip() for line in all_lines if line.strip()]
        empty_lines = len(all_lines) - len(links)
        
        # Get first few links
        sample_links = links[:5] if len(links) > 5 else links
        
        # Check if links are UUIDs or full URLs
        link_type = "unknown"
        if links:
            first_link = links[0]
            if first_link.startswith('http'):
                link_type = "full_url"
            elif '-' in first_link and len(first_link) <= 50:
                link_type = "uuid"
        
        return {
            "status": "success",
            "file_info": {
                "path": link_file,
                "size_bytes": file_size,
                "size_kb": round(file_size / 1024, 2),
                "last_modified": last_modified.isoformat(),
                "age_seconds": (datetime.now() - last_modified).total_seconds()
            },
            "content_info": {
                "total_lines": len(all_lines),
                "valid_links": len(links),
                "empty_lines": empty_lines,
                "link_type": link_type,
                "first_link": links[0] if links else None,
                "first_link_full": f"https://www.autoscout24.be/fr/details/{links[0]}" if links and link_type == "uuid" else None,
                "last_link": links[-1] if links else None,
                "sample_links": sample_links
            },
            "scraper_ready": len(links) > 0,
            "next_step": "Run data scraping to get vehicle details" if len(links) > 0 else "Run link scraping first",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error checking links file: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/test/quick-verify")
async def quick_verify():
    """
    Quick verification endpoint to check if cron job would work.
    """
    # Check all components
    checks = []
    
    # 1. Check if scheduler is running
    try:
        scheduler = app.state.scheduler
        checks.append({
            "component": "scheduler",
            "status": "running" if scheduler.is_running() else "stopped",
            "ok": scheduler.is_running()
        })
    except:
        checks.append({
            "component": "scheduler",
            "status": "error",
            "ok": False
        })
    
    # 2. Check if link file exists
    link_file = find_latest_link_file()
    checks.append({
        "component": "link_file",
        "status": f"found: {link_file}" if link_file else "not_found",
        "ok": link_file is not None,
        "path": link_file
    })
    
    # 3. Check if file has content
    if link_file:
        try:
            with open(link_file, 'r', encoding='utf-8') as f:
                links = [line.strip() for line in f.readlines() if line.strip()]
            checks.append({
                "component": "link_content",
                "status": f"{len(links)} links found",
                "ok": len(links) > 0,
                "count": len(links)
            })
        except:
            checks.append({
                "component": "link_content",
                "status": "unreadable",
                "ok": False
            })
    
    # Calculate overall status
    all_ok = all(check.get("ok", False) for check in checks)
    
    return {
        "status": "ready" if all_ok else "issues",
        "checks": checks,
        "all_checks_passed": all_ok,
        "test_cron_job_endpoint": "/api/test/cron-job (POST)",
        "check_first_link": "/api/test/first-link (GET)",
        "timestamp": datetime.now().isoformat()
    }

# ==================== BACKGROUND TASK ====================

async def track_test_scraping():
    """Track a test scraping run."""
    try:
        logger.info("Starting test scraping tracking...")
        
        # Update test tracker
        test_tracker.last_test_run["status"] = "completed"
        test_tracker.last_test_run["completion_time"] = datetime.now()
        
        # Find and store links
        link_file = find_latest_link_file()
        if link_file:
            try:
                with open(link_file, 'r', encoding='utf-8') as f:
                    test_tracker.last_scraped_links = [line.strip() for line in f.readlines() if line.strip()]
                test_tracker.last_test_run["links_found"] = len(test_tracker.last_scraped_links)
            except Exception as e:
                logger.error(f"Error reading links after test: {e}")
        
        logger.info(f"Test scraping completed. Links found: {len(test_tracker.last_scraped_links)}")
        
    except Exception as e:
        logger.error(f"Error in test tracking: {e}")
        if test_tracker.last_test_run:
            test_tracker.last_test_run["status"] = "failed"
            test_tracker.last_test_run["error"] = str(e)