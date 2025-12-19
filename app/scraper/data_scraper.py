import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import time
import random
import threading
from urllib.parse import urlparse
from datetime import datetime
from googletrans import Translator
import logging

logger = logging.getLogger(__name__)

class DataScraper:
    """Scrapes vehicle data from AutoScout24 links."""
    
    def __init__(self):
        # Initialize translator
        self.translator = Translator()
        
        # Thread-local storage for session
        self.thread_local = threading.local()
        
        # Allowed brands
        self.allowed_brands = [
            'mercedes-benz', 'mercedes', 'mercedesbenz',
            'volkswagen', 'vw', 'volks',
            'bmw', 'audi', 'peugeot', 'ford', 'volvo', 'kia'
        ]
        
        self.brand_patterns = {
            'mercedes-benz': ['mercedes-benz', 'mercedes', 'mercedesbenz'],
            'volkswagen': ['volkswagen', 'vw', 'volks'],
            'bmw': ['bmw'],
            'audi': ['audi'],
            'peugeot': ['peugeot'],
            'ford': ['ford'],
            'volvo': ['volvo'],
            'kia': ['kia']
        }
    
    def get_session(self):
        """Get or create a session for each thread."""
        if not hasattr(self.thread_local, "session"):
            self.thread_local.session = requests.Session()
            self.thread_local.session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Cache-Control': 'max-age=0',
            })
        return self.thread_local.session
    
    def translate_dutch_to_english(self, text):
        """Translate Dutch text to English."""
        if text == 'nd' or not text or text.strip() == '':
            return text
        try:
            # First try to detect language and translate if Dutch
            detection = self.translator.detect(text)
            if detection.lang == 'nl':
                translation = self.translator.translate(text, src='nl', dest='en')
                return translation.text
            # Also translate if it's French (since URL is /fr/ offers)
            elif detection.lang == 'fr':
                translation = self.translator.translate(text, src='fr', dest='en')
                return translation.text
            return text
        except:
            return text
    
    def clean_numeric_text(self, text):
        """Clean numeric text."""
        if text == 'nd' or not text:
            return text
        text = text.replace('â€¯', '').replace(' ', '').replace('\xa0', '').replace(' ', '')
        numbers = re.findall(r'\d+', text)
        return ''.join(numbers) if numbers else text
    
    def extract_specific_element(self, soup, parent_class, target_class, index=0, parent='div', child='div'):
        """Find parent container first, then get specific child."""
        try:
            # Find the parent container
            parent_elem = soup.find(parent, class_=parent_class)
            if parent_elem:
                # Find all elements with target class within parent
                elements = parent_elem.find_all(child, class_=target_class)
                if elements and len(elements) > index:
                    return elements[index].get_text(strip=True)
            return 'nd'
        except:
            return 'nd'
    
    def extract_by_class(self, soup, tag, class_name):
        """Extract text using exact CSS classes from Excel."""
        try:
            if class_name == 'nd':
                return 'nd'
            
            # Split multiple classes
            classes = class_name.split()
            class_dict = {'class': classes}
            
            element = soup.find(tag, class_dict)
            return element.get_text(strip=True) if element else 'nd'
        except:
            return 'nd'
    
    def extract_brand_from_url(self, url):
        """Extract brand from URL before making request."""
        # Extract the path from URL
        parsed = urlparse(url)
        path = parsed.path.lower()
        
        # Look for brand in the URL path
        for brand, patterns in self.brand_patterns.items():
            for pattern in patterns:
                # Check if pattern appears as a separate word in the path
                if f'/{pattern}-' in path:
                    return brand
        
        return None
    
    def extract_emission_class(self, soup):
        """Extract emission class with more specific targeting."""
        try:
            # Method 1: Look for specific label pattern
            emission_labels = soup.find_all(['dt', 'span', 'div'], 
                                        string=re.compile(r'.*(emission|émission|uitstoot|norm).*', re.IGNORECASE))
            
            for label in emission_labels:
                # Get the parent container
                parent_container = label.find_parent('dl')
                if parent_container:
                    # Find the corresponding value
                    value_elem = label.find_next_sibling('dd')
                    if value_elem:
                        return value_elem.get_text(strip=True)
            
            # Method 2: Try specific data grid structure
            data_grids = soup.find_all('dl', class_=re.compile(r'.*DataGrid.*'))
            for grid in data_grids:
                labels = grid.find_all('dt')
                for i, label in enumerate(labels):
                    label_text = label.get_text(strip=True).lower()
                    if any(keyword in label_text for keyword in ['emission', 'émission', 'uitstoot', 'norm', 'euro']):
                        values = grid.find_all('dd')
                        if i < len(values):
                            return values[i].get_text(strip=True)
            
            # Method 3: Fallback to original method
            emission_class = self.extract_specific_element(
                soup, 
                'DataGrid_defaultDlStyle__xlLi_ DataGrid_asColumnUntilLg__HEguB DataGrid_hideLastBorder__F6GqU', 
                "DataGrid_defaultDdStyle__3IYpG DataGrid_fontBold__RqU01", 
                0, "dl", "dd"
            )
            
            return emission_class if emission_class != 'nd' else 'nd'
            
        except Exception as e:
            logger.error(f"Error extracting emission class: {e}")
            return 'nd'
    
    def extract_co2_emissions(self, soup):
        """Extract CO2 emissions information with label-based targeting."""
        try:
            # Method 1: Look for specific label pattern
            co2_labels = soup.find_all(['dt', 'span', 'div'], 
                                     string=re.compile(r'.*(co2|co₂|carbon|koolstof|émissions|emissions|uitstoot).*', re.IGNORECASE))
            
            for label in co2_labels:
                # Get the parent container
                parent_container = label.find_parent('dl')
                if parent_container:
                    # Find the corresponding value
                    value_elem = label.find_next_sibling('dd')
                    if value_elem:
                        return value_elem.get_text(strip=True)
            
            # Method 2: Try specific data grid structure
            data_grids = soup.find_all('dl', class_=re.compile(r'.*DataGrid.*'))
            for grid in data_grids:
                labels = grid.find_all('dt')
                for i, label in enumerate(labels):
                    label_text = label.get_text(strip=True).lower()
                    if any(keyword in label_text for keyword in ['co2', 'co₂', 'carbon', 'koolstof', 'émissions', 'emissions', 'uitstoot']):
                        values = grid.find_all('dd')
                        if i < len(values):
                            return values[i].get_text(strip=True)
            
            # Method 3: Fallback to original method
            co2 = self.extract_specific_element(
                soup, 
                'DataGrid_defaultDlStyle__xlLi_ DataGrid_hideLastBorder__F6GqU', 
                "DataGrid_defaultDdStyle__3IYpG DataGrid_fontBold__RqU01 DataGrid_lastItem__ObUNO", 
                2, "dl", "dd"
            )
            
            return co2 if co2 != 'nd' else 'nd'
            
        except Exception as e:
            logger.error(f"Error extracting CO2 emissions: {e}")
            return 'nd'
    
    def scrape_vehicle_data(self, url, attempt=1, max_attempts=3):
        """Scrape vehicle data from a single URL with retry logic."""
        # Check brand from URL first (quick filter)
        brand_from_url = self.extract_brand_from_url(url)
        if not brand_from_url:
            return None
        
        try:
            session = self.get_session()
            
            # Add random delay to avoid rate limiting
            time.sleep(random.uniform(1.0, 3.0))
            
            response = session.get(url, timeout=15)
            
            # Check for rate limiting or blocking
            if response.status_code == 429:  # Too Many Requests
                if attempt < max_attempts:
                    wait_time = 10 * attempt  # Exponential backoff
                    logger.info(f"⚠️  Rate limited, waiting {wait_time}s before retry {attempt+1}...")
                    time.sleep(wait_time)
                    return self.scrape_vehicle_data(url, attempt + 1, max_attempts)
                else:
                    logger.error(f"❌ Max retries reached for rate limiting: {url[:60]}...")
                    return None
            
            if response.status_code == 403:  # Forbidden (blocked)
                logger.error(f"❌ Blocked by server (403): {url[:60]}...")
                return None
            
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract data - ALL ATTRIBUTES INCLUDING SUBTITLE
            results = {}
            
            # 1. TITLE
            title = self.extract_by_class(soup, 'span', 'StageTitle_boldClassifiedInfo__sQb0l StageTitle_textOverflow__KN9BA')
            results['title'] = self.translate_dutch_to_english(title)
            
            # 2. SUBTITLE (ADDED)
            subtitle = self.extract_by_class(soup, 'div', 'StageTitle_modelVersion__Yof2Z')
            results['subtitle'] = self.translate_dutch_to_english(subtitle)
            
            # 3. PRICE_RAW
            price_raw = self.extract_by_class(soup, 'span', 'SuperDeal_highlightContainer__R8edU SuperDeal_superDeal__P3xYV PriceInfo_price__XU0aF')
            results['price_raw'] = price_raw
            
            # 4. PRICE_EUR
            results['price_eur'] = f"€{self.clean_numeric_text(price_raw)}" if price_raw != 'nd' else 'nd'
            
            # 5. MILEAGE
            mileage = self.extract_by_class(soup, 'div', 'VehicleOverview_itemText__AI4dA')
            results['mileage'] = f"{self.clean_numeric_text(mileage)} km" if mileage != 'nd' else 'nd'
            
            # 6. TRANSMISSION
            transmission = self.extract_specific_element(soup, 'VehicleOverview_containerMoreThanFourItems__691k2', 'VehicleOverview_itemText__AI4dA', 1)
            results['transmission'] = self.translate_dutch_to_english(transmission)
            
            # 7. EMISSION CLASS
            emission_class = self.extract_emission_class(soup)
            results['emission_class'] = self.translate_dutch_to_english(emission_class)
            
            # 8. FUEL TYPE
            fuel = self.extract_specific_element(soup, 'VehicleOverview_containerMoreThanFourItems__691k2', 'VehicleOverview_itemText__AI4dA', 3)
            results['fuel'] = self.translate_dutch_to_english(fuel)
            
            # 9. YEAR
            year = self.extract_specific_element(soup, 'VehicleOverview_containerMoreThanFourItems__691k2', 'VehicleOverview_itemText__AI4dA', 2)
            results['year'] = self.translate_dutch_to_english(year)
            
            # 10. BRAND (extract from title)
            brand = self.extract_by_class(soup, 'span', 'StageTitle_boldClassifiedInfo__sQb0l StageTitle_textOverflow__KN9BA')
            
            if brand:
                brand_lower = brand.strip().lower()
                
                # Find the matched brand name
                matched_brand = next((allowed_brand for allowed_brand in self.allowed_brands 
                                    if allowed_brand in brand_lower or brand_lower in allowed_brand), None)
                
                if matched_brand:
                    results['general_information__BRAND'] = matched_brand.title()
                else:
                    results['general_information__BRAND'] = 'Unknown'
            else:
                results['general_information__BRAND'] = 'Unknown'
            
            # 11. MODEL (extract from title)
            raw_brand = self.extract_by_class(soup, 'span', 'StageTitle_boldClassifiedInfo__sQb0l StageTitle_textOverflow__KN9BA').strip().lower()
            model = raw_brand
            for b in self.allowed_brands:
                if raw_brand.startswith(b):
                    model = raw_brand[len(b):].strip()
                    break
            results['general_information__model'] = self.translate_dutch_to_english(model)
            
            # 12. EMISSION CLASS (for energy consumption)
            emission = self.extract_emission_class(soup)
            results['energy_consumption__emission_class'] = self.translate_dutch_to_english(emission)
            
            # 13. CO2 EMISSIONS
            co2 = self.extract_co2_emissions(soup)
            results['energy_consumption__co2_emissions'] = self.translate_dutch_to_english(co2)
            
            # 14. WARRANTY
            warranty = self.extract_specific_element(soup, 'DataGrid_defaultDlStyle__xlLi_ DataGrid_hideLastBorder__F6GqU', 
                                                  "DataGrid_defaultDdStyle__3IYpG DataGrid_fontBold__RqU01 DataGrid_lastItem__ObUNO", 
                                                  0, "dl", "dd")
            results['general_information__warranty'] = self.translate_dutch_to_english(warranty or "nd")
            
            # 15. VEHICLE_HISTORY__MILEAGE
            results['vehicle_history__mileage'] = results['mileage']

            # 16. VEHICLE_HISTORY__YEAR
            results['vehicle_history__year'] = results['year']

            # 17. URL and DATE
            results['link'] = url
            results['date'] = datetime.now().strftime("%Y-%m-%d")
            
            # 18. SUBTITLE (also add as separate field if needed)
            results['general_information__subtitle'] = results['subtitle']
            
            return results
            
        except requests.exceptions.Timeout:
            logger.warning(f"⏱️  Timeout (attempt {attempt}): {url[:60]}...")
            if attempt < max_attempts:
                time.sleep(3 * attempt)
                return self.scrape_vehicle_data(url, attempt + 1, max_attempts)
            return None
        except requests.exceptions.RequestException as e:
            if attempt < max_attempts and hasattr(e, 'response') and e.response.status_code == 429:
                wait_time = 15 * attempt
                logger.info(f"⚠️  Rate limited ({e.response.status_code}), waiting {wait_time}s...")
                time.sleep(wait_time)
                return self.scrape_vehicle_data(url, attempt + 1, max_attempts)
            logger.error(f"❌ Request error (attempt {attempt}): {url[:60]}...")
            return None
        except Exception as e:
            logger.error(f"❌ Error (attempt {attempt}): {url[:60]}... - {str(e)[:50]}")
            return None
    
    def process_urls_conservatively(self, urls):
        """Process URLs with conservative rate limiting."""
        logger.info(f"🚗 Starting scraping of {len(urls)} URLs...")
        
        # Pre-filter URLs by brand
        logger.info("\n📊 Pre-filtering URLs by brand...")
        filtered_urls = []
        skipped_urls = []
        
        for url in urls:
            brand = self.extract_brand_from_url(url)
            if brand:
                filtered_urls.append(url)
            else:
                skipped_urls.append(url)
        
        logger.info(f"📈 After pre-filtering: {len(filtered_urls)} to process, {len(skipped_urls)} skipped")
        
        if not filtered_urls:
            logger.error("❌ No URLs match the allowed brands!")
            return pd.DataFrame()
        
        # Process URLs
        all_results = []
        successful = 0
        failed = 0
        
        logger.info(f"\n🔍 Processing {len(filtered_urls)} filtered URLs...")
        
        for i, url in enumerate(filtered_urls, 1):
            # Conservative delay: 3-6 seconds between requests
            if i > 1:
                delay = random.uniform(3, 6)
                time.sleep(delay)
            
            logger.info(f"🔍 {i}/{len(filtered_urls)}: {url[:70]}...")
            
            result = self.scrape_vehicle_data(url)
            
            if result:
                all_results.append(result)
                successful += 1
                logger.info(f"✅ {result['general_information__BRAND']} - {result.get('subtitle', 'No subtitle')[:30]}")
            else:
                failed += 1
                logger.info(f"❌ Failed to scrape")
            
            # Progress update
            if i % 5 == 0:
                logger.info(f"📊 Progress: {i}/{len(filtered_urls)} ({successful} successful, {failed} failed)")
            
            # Save intermediate results every 20 vehicles
            if i % 20 == 0 and all_results:
                temp_df = pd.DataFrame(all_results)
                temp_file = f"temp_results_{i}.csv"
                temp_df.to_csv(temp_file, index=False, encoding='utf-8')
                logger.info(f"💾 Saved intermediate results to {temp_file}")
                
                # Extra pause every 20 requests
                logger.info("⏸️  Taking a 10-second break...")
                time.sleep(10)
        
        return pd.DataFrame(all_results) if all_results else pd.DataFrame()
    
    def read_urls_from_file(self, filename):
        """Read URLs from text file."""
        try:
            with open(filename, 'r', encoding='utf-8') as file:
                urls = [line.strip() for line in file if line.strip() and line.startswith('http')]
            return urls
        except FileNotFoundError:
            logger.error(f"❌ File '{filename}' not found!")
            return []
    
    def scrape_vehicles(self, links_file):
        """
        Main function to scrape vehicle data from links file.
        
        Args:
            links_file: Path to file containing vehicle URLs
        
        Returns:
            dict: Statistics about the scraping process
        """
        logger.info("🚗 AutoScout24 Vehicle Scraper (Complete with Subtitle)")
        logger.info("=" * 70)
        logger.info("📖 Reading URLs from file...")
        
        urls = self.read_urls_from_file(links_file)
        
        if not urls:
            logger.error("❌ No URLs found in the file!")
            return {"status": "error", "message": "No URLs found"}
        
        logger.info(f"🔗 Found {len(urls)} total URLs")
        
        # Show brand distribution
        logger.info("\n📊 Analyzing brand distribution in URLs...")
        brand_counts = {}
        for url in urls:
            brand = self.extract_brand_from_url(url)
            if brand:
                brand_counts[brand] = brand_counts.get(brand, 0) + 1
        
        if brand_counts:
            logger.info("Brand distribution:")
            total_filtered = sum(brand_counts.values())
            for brand, count in sorted(brand_counts.items(), key=lambda x: x[1], reverse=True):
                percentage = (count / total_filtered) * 100
                logger.info(f"  {brand}: {count} URLs ({percentage:.1f}%)")
            logger.info(f"\nTotal to scrape: {total_filtered} URLs")
        else:
            logger.error("No brands detected in URLs!")
            return {"status": "error", "message": "No allowed brands detected"}
        
        # Calculate estimated time and auto-start scraping
        estimated_time = (total_filtered * 4.5) / 60  # Average 4.5 seconds per URL
        logger.info(f"\n⏱️  Estimated scraping time: {estimated_time:.1f} minutes")
        logger.info("▶️  Starting automatic scraping...\n")
        
        # Start timer
        start_time = time.time()
        
        # Process URLs conservatively
        results_df = self.process_urls_conservatively(urls)
        
        # Calculate time
        elapsed_time = time.time() - start_time
        
        stats = {
            "total_urls_processed": len(urls),
            "filtered_urls": total_filtered,
            "vehicles_scraped": 0,
            "success_rate": 0,
            "elapsed_time_minutes": elapsed_time / 60,
            "time_per_vehicle": 0,
            "output_file": None
        }
        
        if not results_df.empty:
            # Save results
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"complete_vehicles_with_subtitle_{timestamp}.csv"
            results_df.to_csv(output_file, index=False, encoding='utf-8')
            
            logger.info(f"\n{'='*70}")
            logger.info(f"✅ Scraping completed in {elapsed_time/60:.1f} minutes!")
            logger.info(f"💾 Results saved to: {output_file}")
            
            # Update stats
            stats.update({
                "vehicles_scraped": len(results_df),
                "success_rate": len(results_df)/total_filtered*100 if total_filtered > 0 else 0,
                "time_per_vehicle": elapsed_time/len(results_df) if len(results_df) > 0 else 0,
                "output_file": output_file,
                "status": "success"
            })
            
            # Show statistics
            logger.info(f"\n📊 Summary:")
            logger.info(f"   Total URLs processed: {len(urls)}")
            logger.info(f"   Vehicles scraped: {len(results_df)}")
            logger.info(f"   Success rate: {len(results_df)/total_filtered*100:.1f}%")
            logger.info(f"   Time per vehicle: {elapsed_time/len(results_df):.2f} seconds")
            
            # Show brand distribution
            logger.info(f"\n📈 Brand distribution in results:")
            if 'general_information__BRAND' in results_df.columns:
                brand_dist = results_df['general_information__BRAND'].value_counts()
                for brand, count in brand_dist.items():
                    percentage = count / len(results_df) * 100
                    logger.info(f"   {brand}: {count} vehicles ({percentage:.1f}%)")
            
            # Show data quality statistics
            logger.info(f"\n📈 Data Quality Statistics (key attributes):")
            logger.info("-" * 60)
            key_attributes = ['title', 'subtitle', 'price_eur', 'year', 'mileage', 
                            'emission_class', 'energy_consumption__co2_emissions',
                            'fuel', 'transmission', 'general_information__warranty']
            
            for col in key_attributes:
                if col in results_df.columns:
                    filled = results_df[col].apply(lambda x: x != 'nd' and pd.notna(x) and str(x).strip() != '').sum()
                    percentage = (filled / len(results_df)) * 100
                    logger.info(f"{col}: {filled}/{len(results_df)} ({percentage:.1f}%)")
            
        else:
            logger.error("❌ No data collected! Check if URLs contain allowed brands.")
            stats["status"] = "error"
            stats["message"] = "No data collected"
        
        return stats