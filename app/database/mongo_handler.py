from pymongo import MongoClient
from urllib.parse import quote_plus
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import logging
import certifi

logger = logging.getLogger(__name__)
load_dotenv()

class MongoDBHandler:
    """Handles MongoDB operations for the scraper with improved connection handling."""
    
    def __init__(self):
        self.user = os.getenv("MONGODB_USER")
        self.password = quote_plus(os.getenv("MONGODB_PASSWORD", ""))
        self.cluster = os.getenv("MONGODB_CLUSTER", "cluster0.ksd8tim.mongodb.net")
        self.db_name = os.getenv("MONGODB_DB_NAME", "vehicle_db")
        self.collection_name = os.getenv("MONGODB_COLLECTION", "cars")
        self.stats_collection = "scraping_stats"
        
        # File paths from environment
        self.latest_links_file = os.getenv("LATEST_LINKS_FILE", "latest_links.txt")
        self.main_links_file = os.getenv("MAIN_LINKS_FILE", "abc3.txt")
        self.new_links_file = os.getenv("NEW_LINKS_FILE", "new_links.txt")
        
        # Build MongoDB URI
        self.uri = f"mongodb+srv://{self.user}:{self.password}@{self.cluster}/{self.db_name}?retryWrites=true&w=majority&appName=AutoScout"
        
        self.client = None
        self.db = None
        self.collection = None
        self.stats_coll = None
        
        self._connect()
    
    def _connect(self):
        """Establish secure connection to MongoDB with TLS/SSL."""
        try:
            self.client = MongoClient(
                self.uri,
                tls=True,
                tlsCAFile=certifi.where(),  # Use certifi for SSL certificates
                serverSelectionTimeoutMS=10000,
                connectTimeoutMS=10000,
                socketTimeoutMS=10000,
            )
            
            # Force real connection test
            self.client.admin.command("ping")
            
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            self.stats_coll = self.db[self.stats_collection]
            
            logger.info("MongoDB TLS handshake successful")
            logger.info(f"Connected to database: {self.db_name}")
            logger.info(f"Collection: {self.collection_name}")
            
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            logger.info("Troubleshooting steps:")
            logger.info("1. Install certifi: pip install certifi")
            logger.info("2. Check if IP is whitelisted in MongoDB Atlas")
            logger.info("3. Try different connection string with ssl=false")
            logger.info("4. Update OpenSSL: pip install --upgrade cryptography")
            
            # Try alternative connection without SSL
            self._try_fallback_connection()
    
    def _try_fallback_connection(self):
        """Try alternative connection method without SSL."""
        try:
            logger.info("Trying fallback connection without SSL...")
            
            fallback_uri = f"mongodb+srv://{self.user}:{self.password}@{self.cluster}/{self.db_name}?retryWrites=true&w=majority&ssl=false"
            
            self.client = MongoClient(
                fallback_uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
            )
            
            self.client.admin.command("ping")
            
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            self.stats_coll = self.db[self.stats_collection]
            
            logger.info("Connected using fallback (no SSL)")
            logger.info(f"Database: {self.db_name}")
            
        except Exception as e:
            logger.error(f"Fallback connection also failed: {e}")
            self.client = None
            self.db = None
            self.collection = None
            self.stats_coll = None
    
    def is_connected(self):
        """Check if MongoDB is connected."""
        return self.client is not None
    
    def cleanup_files(self, cleaned_csv_file):
        """Clean up temporary files after MongoDB update."""
        try:
            # 1. Empty latest_links.txt
            if os.path.exists(self.latest_links_file):
                with open(self.latest_links_file, 'w', encoding='utf-8') as f:
                    f.write('')  # Empty the file
                logger.info(f"Emptied {self.latest_links_file}")
            
            # 2. Delete the cleaned CSV file
            if os.path.exists(cleaned_csv_file):
                os.remove(cleaned_csv_file)
                logger.info(f"Deleted temporary CSV: {cleaned_csv_file}")
            
            # 3. Delete any other temporary CSV files
            self._delete_temp_csv_files()
            
            # 4. Also empty new_links.txt for next run
            if os.path.exists(self.new_links_file):
                with open(self.new_links_file, 'w', encoding='utf-8') as f:
                    f.write('')
                logger.info(f"Emptied {self.new_links_file} for next run")
                
            return True
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            return False
    
    def _delete_temp_csv_files(self):
        """Delete temporary CSV files from previous runs."""
        import glob
        
        # Delete complete_vehicles_*.csv files
        complete_files = glob.glob("complete_vehicles_*.csv")
        for file in complete_files:
            try:
                os.remove(file)
                logger.info(f"Deleted temporary file: {file}")
            except:
                pass
        
        # Delete cleaned_vehicles_*.csv files
        cleaned_files = glob.glob("cleaned_vehicles_*.csv")
        for file in cleaned_files:
            try:
                os.remove(file)
                logger.info(f"Deleted temporary file: {file}")
            except:
                pass
        
        # Delete temp_results_*.csv files
        temp_files = glob.glob("temp_results_*.csv")
        for file in temp_files:
            try:
                os.remove(file)
                logger.info(f"Deleted temporary file: {file}")
            except:
                pass
    
    def update_database(self, cleaned_csv_file, existing_csv_file):
        """
        Update MongoDB with cleaned data.
        1. Load existing data from MongoDB
        2. Load new data from CSV
        3. Append and save to main CSV
        4. Insert only new records to MongoDB
        5. Clean up temporary files
        """
        try:
            # Check connection
            if not self.is_connected():
                logger.warning("MongoDB not connected. Skipping database update.")
                return {
                    "status": "skipped",
                    "message": "MongoDB not connected",
                    "records_inserted": 0,
                    "total_records": 0
                }
            
            # Step 1: Load existing data from MongoDB
            logger.info("Loading existing data from MongoDB...")
            mongo_docs = list(self.collection.find({}, {"_id": 0}))
            df_existing = pd.DataFrame(mongo_docs)
            logger.info(f"Loaded {len(df_existing)} records from MongoDB")
            
            # Step 2: Load new CSV data
            logger.info("Loading new CSV data...")
            df_new = pd.read_csv(cleaned_csv_file)
            logger.info(f"New records in CSV: {len(df_new)}")
            
            # Step 3: Clean NaN values
            df_existing = df_existing.where(pd.notnull(df_existing), None)
            df_new = df_new.where(pd.notnull(df_new), None)
            
            # Step 4: Append data (but don't save to existing_csv_file)
            df_updated = pd.concat([df_existing, df_new], ignore_index=True)
            
            # Step 5: Insert ONLY new records to MongoDB
            inserted_count = 0
            if not df_new.empty:
                # Convert to dictionary format
                new_records = df_new.to_dict("records")
                
                # Insert into MongoDB
                result = self.collection.insert_many(new_records)
                inserted_count = len(result.inserted_ids)
                
                logger.info(f"Inserted {inserted_count} new records into MongoDB")
            
            # Step 6: Clean up files
            cleanup_success = self.cleanup_files(cleaned_csv_file)
            
            # Return results
            if not df_new.empty:
                return {
                    "status": "success",
                    "records_inserted": inserted_count,
                    "total_records": len(df_updated),
                    "mongo_records": len(mongo_docs) + inserted_count,
                    "cleanup_success": cleanup_success,
                    "message": f"Inserted {inserted_count} records and cleaned up files"
                }
            else:
                logger.info("No new records to insert into MongoDB")
                return {
                    "status": "success",
                    "records_inserted": 0,
                    "total_records": len(df_updated),
                    "mongo_records": len(mongo_docs),
                    "cleanup_success": cleanup_success,
                    "message": "No new records found"
                }
                
        except FileNotFoundError as e:
            logger.error(f"CSV file not found: {e}")
            return {
                "status": "error",
                "message": f"File not found: {e}",
                "records_inserted": 0,
                "total_records": 0
            }
        except Exception as e:
            logger.error(f"Error updating database: {e}")
            return {
                "status": "error",
                "message": str(e),
                "records_inserted": 0,
                "total_records": 0
            }
    
    def save_scraping_stats(self, stats):
        """Save scraping statistics to MongoDB."""
        try:
            if not self.is_connected():
                logger.warning("Cannot save stats: MongoDB not connected")
                return False
            
            stats["_id"] = stats["start_time"]
            self.stats_coll.insert_one(stats)
            logger.info("Saved scraping statistics to MongoDB")
            return True
            
        except Exception as e:
            logger.error(f"Error saving stats: {e}")
            return False
    
    def get_latest_scrape_stats(self):
        """Get the latest scraping statistics."""
        try:
            if not self.is_connected():
                logger.warning("Cannot get stats: MongoDB not connected")
                return None
            
            latest = self.stats_coll.find_one(
                sort=[("start_time", -1)]
            )
            
            if latest and "_id" in latest:
                del latest["_id"]  # Remove MongoDB ID
            
            return latest
        except Exception as e:
            logger.error(f"Error getting latest stats: {e}")
            return None
    
    def get_scraping_stats(self, days=7):
        """Get scraping statistics for the last N days."""
        try:
            if not self.is_connected():
                logger.warning("Cannot get stats: MongoDB not connected")
                return []
            
            cutoff_date = datetime.now() - timedelta(days=days)
            
            pipeline = [
                {
                    "$match": {
                        "start_time": {"$gte": cutoff_date}
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$start_time"
                            }
                        },
                        "total_runs": {"$sum": 1},
                        "successful_runs": {
                            "$sum": {
                                "$cond": [
                                    {"$eq": ["$status", "completed"]},
                                    1,
                                    0
                                ]
                            }
                        },
                        "failed_runs": {
                            "$sum": {
                                "$cond": [
                                    {"$eq": ["$status", "failed"]},
                                    1,
                                    0
                                ]
                            }
                        },
                        "avg_duration": {"$avg": "$duration"},
                        "latest_run": {"$max": "$start_time"}
                    }
                },
                {
                    "$sort": {"_id": -1}
                }
            ]
            
            results = list(self.stats_coll.aggregate(pipeline))
            
            # Format results
            formatted_results = []
            for result in results:
                formatted_results.append({
                    "date": result["_id"],
                    "total_runs": result["total_runs"],
                    "successful_runs": result["successful_runs"],
                    "failed_runs": result["failed_runs"],
                    "success_rate": (
                        (result["successful_runs"] / result["total_runs"] * 100)
                        if result["total_runs"] > 0 else 0
                    ),
                    "avg_duration_seconds": result.get("avg_duration", 0),
                    "latest_run": result.get("latest_run")
                })
            
            return formatted_results
            
        except Exception as e:
            logger.error(f"Error getting scraping stats: {e}")
            return []
    
    def get_collection_stats(self):
        """Get statistics about the MongoDB collection."""
        try:
            if not self.is_connected():
                return {
                    "connected": False,
                    "message": "MongoDB not connected"
                }
            
            total_documents = self.collection.count_documents({})
            
            # Get sample of recent documents
            recent_docs = list(self.collection.find(
                {}, 
                {"_id": 0}
            ).sort("_id", -1).limit(5))
            
            # Get unique brands
            unique_brands = self.collection.distinct("brand")
            
            return {
                "connected": True,
                "total_documents": total_documents,
                "unique_brands": len(unique_brands),
                "brands": unique_brands[:10],  # Show first 10
                "recent_documents": recent_docs
            }
            
        except Exception as e:
            logger.error(f"Error getting collection stats: {e}")
            return {
                "connected": False,
                "error": str(e)
            }
    
    def close(self):
        """Close the MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")