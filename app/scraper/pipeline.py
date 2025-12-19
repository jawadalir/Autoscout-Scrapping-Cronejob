import logging
from datetime import datetime
import os
from typing import Dict, Any

from .link_scraper import LinkScraper
from .data_scraper import DataScraper
from .data_cleaner import DataCleaner
from ..database.mongo_handler import MongoDBHandler

logger = logging.getLogger(__name__)

class ScrapingPipeline:
    """Orchestrates the complete scraping pipeline."""
    
    def __init__(self):
        self.link_scraper = LinkScraper()
        self.data_scraper = DataScraper()
        self.data_cleaner = DataCleaner()
        self.mongo_handler = MongoDBHandler()
        
        # File paths
        self.main_links_file = os.getenv("MAIN_LINKS_FILE", "abc3.txt")
        self.new_links_file = os.getenv("NEW_LINKS_FILE", "new_links.txt")
        self.latest_links_file = os.getenv("LATEST_LINKS_FILE", "latest_links.txt")
    
    def run_full_pipeline(self) -> Dict[str, Any]:
        """Run the complete scraping pipeline."""
        start_time = datetime.now()
        stats = {
            "start_time": start_time,
            "status": "running",
            "steps": {}
        }
        
        try:
            # Step 1: Scrape links
            logger.info("Step 1: Starting link scraping")
            link_stats = self.link_scraper.scrape_links()
            stats["steps"]["link_scraping"] = link_stats
            
            # Step 2: Scrape vehicle data
            logger.info("Step 2: Starting data scraping")
            data_stats = self.data_scraper.scrape_vehicles(self.new_links_file)
            stats["steps"]["data_scraping"] = data_stats
            
            # Step 3: Clean data
            logger.info("Step 3: Starting data cleaning")
            if data_stats.get("output_file"):
                clean_stats = self.data_cleaner.clean_data(data_stats["output_file"])
                stats["steps"]["data_cleaning"] = clean_stats
                
                # Step 4: Update MongoDB
                logger.info("Step 4: Updating MongoDB")
                if clean_stats.get("output_file"):
                    mongo_stats = self.mongo_handler.update_database(
                        clean_stats["output_file"],
                        self.main_links_file
                    )
                    stats["steps"]["mongodb_update"] = mongo_stats
            
            # Update final stats
            stats["status"] = "completed"
            stats["end_time"] = datetime.now()
            stats["duration"] = (stats["end_time"] - start_time).total_seconds()
            
            # Save stats to MongoDB
            self.mongo_handler.save_scraping_stats(stats)
            
            logger.info(f"Pipeline completed in {stats['duration']:.2f} seconds")
            return stats
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            stats["status"] = "failed"
            stats["error"] = str(e)
            stats["end_time"] = datetime.now()
            self.mongo_handler.save_scraping_stats(stats)
            raise
    
    def run_link_scraping_only(self) -> Dict[str, Any]:
        """Run only the link scraping step."""
        start_time = datetime.now()
        
        try:
            logger.info("Running link scraping only")
            stats = self.link_scraper.scrape_links()
            stats["duration"] = (datetime.now() - start_time).total_seconds()
            return stats
            
        except Exception as e:
            logger.error(f"Link scraping failed: {e}")
            raise
    
    def run_data_scraping_only(self) -> Dict[str, Any]:
        """Run only the data scraping step."""
        start_time = datetime.now()
        
        try:
            logger.info("Running data scraping only")
            stats = self.data_scraper.scrape_vehicles(self.new_links_file)
            stats["duration"] = (datetime.now() - start_time).total_seconds()
            return stats
            
        except Exception as e:
            logger.error(f"Data scraping failed: {e}")
            raise

# Create pipeline instance
pipeline = ScrapingPipeline()

# Convenience functions
def run_full_pipeline():
    """Run the complete scraping pipeline."""
    return pipeline.run_full_pipeline()

def run_link_scraping_only():
    """Run only link scraping."""
    return pipeline.run_link_scraping_only()

def run_data_scraping_only():
    """Run only data scraping."""
    return pipeline.run_data_scraping_only()