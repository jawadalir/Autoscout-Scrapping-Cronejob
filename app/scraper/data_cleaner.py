import pandas as pd
import numpy as np
import re
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DataCleaner:
    """Cleans and processes scraped vehicle data."""
    
    def __init__(self):
        self.allowed_emission_classes = {
            "euro 6", "euro 5", "euro 6d", "euro 6d-temp",
            "euro 6c", "euro 6e", "euro 6b"
        }
    
    def clean_data(self, input_file):
        """
        Clean the scraped vehicle data.
        
        Args:
            input_file: Path to the raw CSV file
        
        Returns:
            dict: Statistics about the cleaning process
        """
        logger.info(f"🔧 Starting data cleaning for {input_file}")
        
        try:
            # ------------------ LOAD DATA ------------------
            df = pd.read_csv(input_file)
            original_shape = df.shape
            
            logger.info(f"📊 Original data shape: {original_shape}")
            
            # ------------------ HANDLE DUPLICATE COLUMNS EARLY ------------------
            if df.columns.duplicated().any():
                dup_cols = df.columns[df.columns.duplicated()].unique().tolist()
                logger.warning(f"Duplicate columns found. Dropping duplicates: {dup_cols}")
                df = df.loc[:, ~df.columns.duplicated()].copy()
            
            # ------------------ RESET INDEX (avoid non-unique index problems) ------------------
            df = df.reset_index(drop=True)
            
            # ------------------ DROP & RENAME ------------------
            df = df.drop(columns=[
                "subtitle",
                "price_raw",
                "energy_consumption__emission_class",
                "vehicle_history__mileage",
                "date",
                "general_information__subtitle"
            ], errors="ignore")
            
            df = df.rename(columns={
                "price_eur": "price",
                "energy_consumption__co2_emissions": "co2",
                "general_information__BRAND": "brand",
                "title": "brand_model",
                "general_information__model": "model",
                "general_information__warranty": "warranty",
                "vehicle_history__year": "year"
            })
            
            # ------------------ PRICE ------------------
            logger.info("💰 Cleaning price data...")
            df["price"] = (
                df.get("price", pd.Series([], dtype="object"))
                .astype(str)
                .str.replace(r"[^\d]", "", regex=True)
            )
            
            df["price"] = pd.to_numeric(df["price"], errors="coerce")
            
            def clean_price(x):
                if pd.isna(x):
                    return x
                x = str(int(x))
                if x.endswith("15"):
                    x = x[:-2]
                elif x.endswith("5"):
                    x = x[:-1]
                return float(x) if x else pd.NA
            
            df["price"] = df["price"].apply(clean_price)
            
            # ------------------ PRICE RANGE ------------------
            logger.info("📈 Applying price range filter...")
            original_count = len(df)
            df = df.loc[df["price"].ge(5000) & df["price"].le(150000)].copy()
            price_filtered = original_count - len(df)
            logger.info(f"  Removed {price_filtered} vehicles outside price range")
            
            # ------------------ TRANSMISSION ------------------
            if "transmission" in df.columns:
                logger.info("⚙️ Cleaning transmission data...")
                df["transmission"] = df["transmission"].replace({
                    "Automatic transmission": "automatic",
                    "Manual transmission": "manual"
                })
                original_count = len(df)
                df = df.loc[df["transmission"].isin(["automatic", "manual"])].copy()
                transmission_filtered = original_count - len(df)
                logger.info(f"  Removed {transmission_filtered} vehicles with invalid transmission")
            else:
                logger.warning("⚠️ 'transmission' column not found")
            
            # ------------------ FUEL ------------------
            if "fuel" in df.columns:
                logger.info("⛽ Cleaning fuel type data...")
                df["fuel"] = df["fuel"].replace({
                    "Essence": "petrol",
                    "Diesel": "diesel"
                })
            else:
                logger.warning("⚠️ 'fuel' column not found")
            
            # ------------------ MILEAGE ------------------
            if "mileage" in df.columns:
                logger.info("📏 Cleaning mileage data...")
                df["mileage"] = df["mileage"].astype(str).str.replace(r"[^\d]", "", regex=True)
                df["mileage"] = pd.to_numeric(df["mileage"], errors="coerce")
                original_count = len(df)
                df = df.loc[df["mileage"].lt(200000)].copy()
                mileage_filtered = original_count - len(df)
                logger.info(f"  Removed {mileage_filtered} vehicles with high mileage")
            else:
                logger.warning("⚠️ 'mileage' column not found")
            
            # ------------------ BRAND ------------------
            if "brand" in df.columns:
                logger.info("🏷️ Cleaning brand data...")
                df["brand"] = (
                    df["brand"]
                    .astype(str)
                    .str.replace("Mercedes-Benz", "mercedes", regex=True)
                    .str.lower()
                )
            else:
                logger.warning("⚠️ 'brand' column not found")
            
            # ------------------ YEAR ------------------
            logger.info("📅 Cleaning year data...")
            df = df.loc[:, ~df.columns.duplicated()]
            
            df["year"] = pd.to_datetime(
                df["year"].astype(str),
                errors="coerce",
                dayfirst=True
            ).dt.year
            
            # Remove cars older than 2010 (keep 2010 and newer), avoid NaNs
            year_mask = df["year"].notna() & (df["year"] >= 2010)
            original_count = len(df)
            df = df.loc[year_mask, :].copy()
            year_filtered = original_count - len(df)
            logger.info(f"  Removed {year_filtered} vehicles older than 2010")
            
            # ------------------ CO2 ------------------
            if "co2" in df.columns:
                logger.info("🌿 Cleaning CO2 emissions data...")
                df["co2"] = pd.to_numeric(
                    df["co2"].astype(str).str.extract(r"(\d+)")[0],
                    errors="coerce"
                )
                df["co2"] = df["co2"].fillna(df["co2"].median())
                original_count = len(df)
                df = df.loc[df["co2"] <= 300].copy()
                co2_filtered = original_count - len(df)
                logger.info(f"  Removed {co2_filtered} vehicles with high CO2 emissions")
            else:
                logger.warning("⚠️ 'co2' column not found")
            
            # ------------------ EMISSION CLASS ------------------
            if "emission_class" in df.columns:
                logger.info("♻️ Cleaning emission class data...")
                df["emission_class"] = df["emission_class"].astype(str).str.lower().str.strip()
                original_count = len(df)
                df = df.loc[df["emission_class"].isin(self.allowed_emission_classes)].copy()
                emission_filtered = original_count - len(df)
                logger.info(f"  Removed {emission_filtered} vehicles with invalid emission class")
            else:
                logger.warning("⚠️ 'emission_class' column not found")
            
            # ------------------ WARRANTY ------------------
            if "warranty" in df.columns:
                logger.info("🛡️ Cleaning warranty data...")
                df["warranty"] = df["warranty"].astype(str).str.extract(r"(\d+)", expand=False)
                counts = df["warranty"].value_counts()
                rare_values = counts[counts < 2].index
                df["warranty"] = df["warranty"].replace(rare_values, 12)
                df["warranty"] = df["warranty"].fillna(12)
                df["warranty"] = pd.to_numeric(df["warranty"], errors="coerce")
            else:
                logger.warning("⚠️ 'warranty' column not found")
            
            # ------------------ BRAND MODEL ------------------
            if "brand_model" in df.columns:
                logger.info("📋 Cleaning brand model data...")
                df["brand_model"] = (
                    df["brand_model"]
                    .astype(str)
                    .str.lower()
                    .str.replace("mercedes-benz", "mercedes", regex=False)
                    .str.strip()
                )
            else:
                logger.warning("⚠️ 'brand_model' column not found")
            
            # ------------------ SAVE CLEANED DATA ------------------
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"cleaned_vehicles_{timestamp}.csv"
            df.to_csv(output_file, index=False)
            
            # ------------------ FINAL STATISTICS ------------------
            final_shape = df.shape
            records_removed = original_shape[0] - final_shape[0]
            
            stats = {
                "status": "success",
                "original_records": original_shape[0],
                "final_records": final_shape[0],
                "records_removed": records_removed,
                "original_columns": original_shape[1],
                "final_columns": final_shape[1],
                "output_file": output_file,
                "filters_applied": {
                    "price_filtered": price_filtered,
                    "transmission_filtered": transmission_filtered,
                    "mileage_filtered": mileage_filtered,
                    "year_filtered": year_filtered,
                    "co2_filtered": co2_filtered,
                    "emission_filtered": emission_filtered
                }
            }
            
            logger.info("✅ Data cleaning completed!")
            logger.info(f"📊 Final dataset shape: {final_shape}")
            logger.info(f"📈 Records removed: {records_removed}")
            logger.info(f"💾 Cleaned data saved to: {output_file}")
            
            # Show sample of cleaned data
            logger.info("\n📋 Sample of cleaned data (first 5 rows):")
            logger.info(df.head().to_string())
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Error cleaning data: {e}")
            import traceback
            traceback.print_exc()
            return {
                "status": "error",
                "message": str(e)
            }