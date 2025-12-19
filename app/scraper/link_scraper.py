import os
import re
import time
from urllib.parse import urlparse, parse_qs, urlencode
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager
import logging

logger = logging.getLogger(__name__)

class LinkScraper:
    """Scrapes vehicle links from AutoScout24."""
    
    def __init__(self):
        self.MAIN_URL = "https://www.autoscout24.be/fr/lst?atype=C&cy=B&damaged_listing=exclude&desc=1&explim=true&ocs_listing=include&page=1&powertype=kw&search_id=iqwpwzlmde&sort=age&source=listpage_pagination&standardSortStrategy=mia_ltr&tier_rotation=true&ustate=N%2CU"
        
        # File paths - configurable
        self.main_links_file = "abc3.txt"
        self.new_links_file = "new_links.txt"
        self.latest_links_file = "latest_links.txt"
        
    def setup_driver(self):
        """Setup Chrome driver with options."""
        try:
            options = webdriver.ChromeOptions()
            
            # Optional: Uncomment for production
            # options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            
            # Better user agent and window size
            options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            options.add_argument('--window-size=1920,1080')
            
            # Additional arguments to avoid detection
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-webgl')
            options.add_argument('--disable-software-rasterizer')
            options.add_argument('--disable-features=VizDisplayCompositor')
            
            # Enable JavaScript
            options.add_argument('--enable-javascript')
            
            # FIX: Clear any existing webdriver_manager cache issues
            try:
                # Force webdriver_manager to install fresh driver
                driver_path = ChromeDriverManager().install()
                logger.info(f"ChromeDriver installed at: {driver_path}")
                
                # Ensure we have the correct .exe file
                if not driver_path.endswith('.exe'):
                    # Look for chromedriver.exe in the directory
                    import glob
                    parent_dir = os.path.dirname(driver_path)
                    exe_files = glob.glob(os.path.join(parent_dir, "chromedriver*.exe"))
                    if exe_files:
                        driver_path = exe_files[0]
                        logger.info(f"Found .exe file: {driver_path}")
                    else:
                        # If still no .exe, try to find any .exe in subdirectories
                        exe_files = glob.glob(os.path.join(parent_dir, "**", "*.exe"), recursive=True)
                        for file in exe_files:
                            if "chromedriver" in file.lower():
                                driver_path = file
                                logger.info(f"Found chromedriver .exe: {driver_path}")
                                break
                
                # Create service with the driver path
                service = Service(driver_path)
                
            except Exception as e:
                logger.error(f"Error with webdriver_manager: {e}")
                logger.info("Falling back to system PATH for ChromeDriver...")
                # Fallback to system PATH
                service = Service()
            
            # Create driver
            driver = webdriver.Chrome(service=service, options=options)
            
            # Execute CDP commands to avoid detection
            driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [1, 2, 3, 4, 5]
                    });
                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['en-US', 'en']
                    });
                '''
            })
            
            logger.info("✅ ChromeDriver setup successful")
            return driver
            
        except Exception as e:
            logger.error(f"Error setting up driver: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def accept_cookies(self, driver):
        """Try to accept cookies with multiple selectors."""
        cookie_selectors = [
            "//button[contains(text(), 'Accept')]",
            "//button[contains(text(), 'Accepter')]",
            "//button[contains(text(), 'OK')]",
            "//button[contains(@id, 'accept')]",
            "//button[contains(@class, 'accept')]",
            "//div[contains(@class, 'cookie')]//button",
            "//button[@data-testid='uc-accept-all-button']",
            "//button[@id='uc-btn-accept-banner']"
        ]
        
        for selector in cookie_selectors:
            try:
                cookie_btn = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, selector))
                )
                cookie_btn.click()
                logger.info("✅ Cookies accepted")
                time.sleep(1)
                return True
            except:
                continue
        
        return False
    
    def load_existing_links(self, file_path):
        """Load existing links from file."""
        existing_links = set()
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        link = line.strip()
                        if link and link.startswith('http'):
                            # Normalize URL - remove tracking parameters
                            clean_link = re.sub(r'\?.*$', '', link)
                            existing_links.add(clean_link)
                logger.info(f"📚 Loaded {len(existing_links)} existing links from {file_path}")
            except Exception as e:
                logger.error(f"⚠️ Error loading existing links: {e}")
        return existing_links
    
    def save_links_to_file(self, file_path, links):
        """Save links to file."""
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                for link in links:
                    f.write(link + '\n')
            logger.info(f"💾 Saved {len(links)} links to {file_path}")
        except Exception as e:
            logger.error(f"❌ Error saving links: {e}")
    
    def extract_vehicle_id(self, url):
        """Extract vehicle ID from URL."""
        try:
            # Look for UUID pattern in URL
            uuid_pattern = r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'
            match = re.search(uuid_pattern, url)
            if match:
                return match.group(0)
            
            # If no UUID, get the last part of the URL
            parts = url.split('/')
            for part in parts:
                if '-' in part and len(part) > 20:
                    return part
            return url.split('/')[-1][:30] + "..."
        except:
            return "Unknown ID"
    
    def wait_for_listings_to_load(self, driver):
        """Wait for vehicle listings to load on the page."""
        logger.info("  ⏳ Waiting for listings to load...")
        
        # Wait for the page to be interactive
        time.sleep(2)
        
        # Wait for any listing-related element to appear
        listing_selectors = [
            (By.CSS_SELECTOR, "article"),
            (By.CSS_SELECTOR, "[data-cy='listing-item']"),
            (By.CSS_SELECTOR, "[data-item-name='listing-item']"),
            (By.CSS_SELECTOR, ".cldt-summary-full-item"),
            (By.CSS_SELECTOR, "[class*='list-item']"),
            (By.CSS_SELECTOR, "[class*='vehicle-item']"),
            (By.CSS_SELECTOR, "h2"),  # Often contains vehicle titles
        ]
        
        for by, selector in listing_selectors:
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((by, selector))
                )
                logger.info(f"  ✅ Found listings using selector: {selector}")
                return True
            except:
                continue
        
        # If no specific selectors work, wait for body to have content
        try:
            WebDriverWait(driver, 10).until(
                lambda d: d.find_element(By.TAG_NAME, "body").text.strip() != ""
            )
            logger.info("  ✅ Page body has content")
            return True
        except:
            logger.warning("  ⚠️ Could not detect listings, continuing anyway")
            return False
    
    def find_vehicle_links_on_page(self, driver, page_num):
        """Find all vehicle links on the current page."""
        logger.info(f"  🔍 Searching for vehicle links on page {page_num}...")
        
        # First, wait for listings to load
        if not self.wait_for_listings_to_load(driver):
            logger.error("  ❌ Listings did not load properly")
            return []
        
        # Take a screenshot for debugging
        # try:
        #     driver.save_screenshot(f'page_{page_num}_debug.png')
        #     logger.info(f"  📸 Screenshot saved: page_{page_num}_debug.png")
        # except:
        #     pass
        
        vehicle_links = []
        
        # Strategy 1: Find links by href pattern
        logger.info("  Trying Strategy 1: Direct href pattern search...")
        try:
            all_links = driver.find_elements(By.TAG_NAME, 'a')
            logger.info(f"    Found {len(all_links)} total <a> tags")
            
            for i, link in enumerate(all_links):
                try:
                    href = link.get_attribute('href')
                    if href and '/offres/' in href and 'autoscout24.be' in href:
                        # Clean the URL (remove query parameters for comparison)
                        clean_href = re.sub(r'\?.*$', '', href)
                        if clean_href not in vehicle_links:
                            vehicle_links.append(clean_href)
                except StaleElementReferenceException:
                    continue
                except Exception as e:
                    continue
            
            logger.info(f"    Found {len(vehicle_links)} vehicle links with Strategy 1")
        except Exception as e:
            logger.error(f"    Strategy 1 error: {e}")
        
        # If first strategy found few links, try more specific selectors
        if len(vehicle_links) < 10:
            logger.info("  Trying Strategy 2: Specific listing selectors...")
            
            # Multiple selectors to try
            selectors_to_try = [
                "article a[href*='/offres/']",
                "a[href*='/offres/']",
                "[data-cy='listing-item'] a",
                "[data-item-name='listing-item'] a",
                ".cldt-summary-full-item a",
                ".cldt-summary-titles a",
                "[class*='list-item'] a[href*='/offres/']",
                "[class*='vehicle-card'] a",
                "h2 a",
                ".title a",
                "a.cldt-summary-full-item-link",
                "a[data-testid='details-page-link']",
                "a[data-item-name='details-page-link']"
            ]
            
            for selector in selectors_to_try:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        logger.info(f"    Found {len(elements)} elements with selector: {selector[:50]}...")
                        for element in elements:
                            try:
                                href = element.get_attribute('href')
                                if href and '/offres/' in href and 'autoscout24.be' in href:
                                    clean_href = re.sub(r'\?.*$', '', href)
                                    if clean_href not in vehicle_links:
                                        vehicle_links.append(clean_href)
                            except:
                                continue
                except Exception as e:
                    logger.error(f"    Error with selector {selector}: {e}")
                    continue
        
        # Remove duplicates while preserving order
        unique_links = []
        seen = set()
        for link in vehicle_links:
            if link not in seen:
                seen.add(link)
                unique_links.append(link)
        
        logger.info(f"  ✅ Found {len(unique_links)} unique vehicle links")
        
        # Debug: Print the HTML source if no links found
        if len(unique_links) == 0:
            logger.warning("  ⚠️ No vehicle links found! Debugging...")
            try:
                # Get a snippet of page source
                page_source = driver.page_source[:3000]
                with open(f'page_{page_num}_source.html', 'w', encoding='utf-8') as f:
                    f.write(page_source)
                logger.info(f"  💾 Page source snippet saved: page_{page_num}_source.html")
                
                # Check if there's any text on the page
                body_text = driver.find_element(By.TAG_NAME, 'body').text[:500]
                logger.info(f"  📝 Body text snippet: {body_text}")
            except Exception as e:
                logger.error(f"  ❌ Debug error: {e}")
        
        return unique_links
    
    def get_next_page_url(self, current_url, next_page_number):
        """Construct the next page URL by updating the page parameter."""
        try:
            # Parse the current URL
            parsed_url = urlparse(current_url)
            query_params = parse_qs(parsed_url.query)
            
            # Update the page number
            query_params['page'] = [str(next_page_number)]
            
            # Rebuild the query string
            new_query = urlencode(query_params, doseq=True)
            
            # Reconstruct the URL
            new_url = parsed_url._replace(query=new_query).geturl()
            
            return new_url
        except Exception as e:
            logger.error(f"    Error constructing next page URL: {e}")
            # Fallback: manually construct URL
            if 'page=' in current_url:
                return re.sub(r'page=\d+', f'page={next_page_number}', current_url)
            else:
                if '?' in current_url:
                    return f"{current_url}&page={next_page_number}"
                else:
                    return f"{current_url}?page={next_page_number}"
    
    def get_latest_vehicle_links(self, existing_links, target_matches=1):
        """
        Get latest vehicle links from AutoScout24
        Stop when we find at least target_matches (100) links that already exist in our file
        """
        driver = self.setup_driver()
        if not driver:
            logger.error("❌ Failed to setup WebDriver")
            return [], []
        
        all_new_links = []  # Links not in existing file
        matched_links = []  # Links that already exist in file
        current_page = 1
        max_pages = 200  # Safety limit
        
        try:
            logger.info(f"🚗 Starting to scrape latest vehicles from AutoScout24...")
            logger.info(f"🎯 Target: Continue until {target_matches} existing links are found")
            logger.info(f"📄 Main URL: {self.MAIN_URL}")
            
            # Set longer timeout for page loads
            driver.set_page_load_timeout(30)
            
            while len(matched_links) < target_matches and current_page <= max_pages:
                logger.info(f"\n{'='*60}")
                logger.info(f"📄 PROCESSING PAGE {current_page}")
                logger.info(f"  Matched so far: {len(matched_links)}/{target_matches}")
                logger.info(f"  New links found: {len(all_new_links)}")
                
                # Construct URL for this page
                if current_page == 1:
                    current_url = self.MAIN_URL
                else:
                    current_url = self.get_next_page_url(self.MAIN_URL, current_page)
                
                logger.info(f"🌐 Navigating to: {current_url}")
                
                try:
                    driver.get(current_url)
                except TimeoutException:
                    logger.warning("⚠️ Page load timeout, but continuing...")
                
                # Wait for page to start loading
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                    logger.info("✅ Page body loaded")
                except TimeoutException:
                    logger.warning("⚠️ Timeout waiting for body, but continuing...")
                
                # Accept cookies on first page
                if current_page == 1:
                    self.accept_cookies(driver)
                
                # Scroll to trigger lazy loading
                logger.info("  🔄 Scrolling to trigger content loading...")
                try:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 3);")
                    time.sleep(1)
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
                    time.sleep(1)
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
                except:
                    pass
                
                # Find links on current page
                page_links = self.find_vehicle_links_on_page(driver, current_page)
                if not page_links:
                    logger.error("❌ No vehicle links found on this page!")
                    
                    # Try one more scroll and wait
                    logger.info("   Trying one more scroll and wait...")
                    driver.execute_script("window.scrollTo(0, 0);")
                    time.sleep(2)
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(3)
                    
                    # Try finding links again
                    page_links = self.find_vehicle_links_on_page(driver, current_page)
                    
                    if not page_links:
                        logger.error("❌ Still no links after retry. Stopping.")
                        break
                
                logger.info(f"  Processing {len(page_links)} links...")
                
                # Process each link
                new_matches_on_page = 0
                new_links_on_page = 0
                
                for i, link in enumerate(page_links, 1):
                    if i <= 3:
                        continue
                    if link in existing_links:
                        if link not in matched_links:
                            matched_links.append(link)
                            new_matches_on_page += 1
                            all_new_links.append(link)
                            break
                    elif link not in all_new_links:
                        all_new_links.append(link)
                        new_links_on_page += 1
                    
                    # Show progress every 10 links
                    if i % 10 == 0:
                        logger.info(f"    Processed {i}/{len(page_links)} links...")
                
                logger.info(f"  Page {current_page} results:")
                logger.info(f"    Total links on page: {len(page_links)}")
                logger.info(f"    New matches found: {new_matches_on_page}")
                logger.info(f"    New links found: {new_links_on_page}")
                logger.info(f"    Total matches: {len(matched_links)}/{target_matches}")
                
                # Show some sample links from this page
                if page_links:
                    logger.info(f"    Sample links from page {current_page} (first 3):")
                    for i, link in enumerate(page_links[:3], 1):
                        vehicle_id = self.extract_vehicle_id(link)
                        logger.info(f"      {i}. {vehicle_id}")
                
                # Check if we reached our target
                if len(matched_links) >= target_matches:
                    logger.info(f"\n🎯 SUCCESS: Found {len(matched_links)} existing links (target: {target_matches})!")
                    break
                
                # Move to next page
                current_page += 1
                
                # Add delay between pages to be nice to the server
                delay_time = 3
                logger.info(f"  ⏱️ Waiting {delay_time} seconds before next page...")
                time.sleep(delay_time)
            
            logger.info(f"\n{'='*60}")
            logger.info(f"📊 FINAL RESULTS:")
            logger.info(f"   Pages processed: {current_page - 1}")
            logger.info(f"   Existing links matched: {len(matched_links)} (target: {target_matches})")
            logger.info(f"   New unique links found: {len(all_new_links)}")
            
            # Show statistics
            if matched_links:
                logger.info(f"\n📋 Matched links breakdown:")
                logger.info(f"    First matched link: {self.extract_vehicle_id(matched_links[0])}")
                logger.info(f"    Last matched link: {self.extract_vehicle_id(matched_links[-1])}")
            
            if all_new_links:
                logger.info(f"\n📋 New links breakdown:")
                logger.info(f"    First new link: {self.extract_vehicle_id(all_new_links[0])}")
                logger.info(f"    Last new link: {self.extract_vehicle_id(all_new_links[-1])}")
            
            return all_new_links, matched_links
            
        except Exception as e:
            logger.error(f"❌ Error getting vehicle links: {e}")
            import traceback
            traceback.print_exc()
            return all_new_links, matched_links
        finally:
            try:
                driver.quit()
                logger.info("🔚 Driver closed")
            except:
                pass
    
    def scrape_links(self):
        """
        Main function to scrape latest vehicles and update files.
        
        Returns:
            dict: Statistics about the scraping process
        """
        logger.info("=" * 80)
        logger.info("🚗 AUTO SCOUT 24 LATEST VEHICLE SCRAPER - IMPROVED VERSION")
        logger.info("=" * 80)
        logger.info("📋 Strategy: Scrape until 100 vehicle links already exist in the file")
        logger.info("⭐ Output: Top 3 newest links saved separately for cron job tracking")
        logger.info("=" * 80)
        
        # Step 1: Load existing links
        logger.info("\n📂 Loading existing data...")
        existing_links = self.load_existing_links(self.main_links_file)
        logger.info(f"   Found {len(existing_links)} existing links in {self.main_links_file}")
        
        # If we don't have enough links in the file yet
        if len(existing_links) < 100:
            logger.info(f"⚠️ Only {len(existing_links)} links in file. We'll scrape until we find 100 matches.")
            logger.info(f"   Note: This may take longer since we need to find {100 - len(existing_links)} more existing links.")
        
        # Step 2: Scrape latest vehicles
        logger.info("\n🔄 Scraping latest vehicles...")
        new_links, matched_links = self.get_latest_vehicle_links(
            existing_links=existing_links,
            target_matches=1
        )
        
        logger.info(f"\n{'='*60}")
        logger.info("📦 PROCESSING RESULTS...")
        
        # Step 3: Save NEW links to separate file (not appending to main file)
        if new_links:
            logger.info(f"\n💾 Saving new links to separate file...")
            self.save_links_to_file(self.new_links_file, new_links)
            logger.info(f"   Saved {len(new_links)} new links to {self.new_links_file}")
        else:
            logger.info(f"\nℹ️ No new links found")
        
        # Step 4: Update MAIN_LINKS_FILE with top 3 values of new_links (replace, not append)
        logger.info(f"\n⭐ Updating {self.main_links_file} with top 3 latest links...")
        
        # Get the first 3 links from new_links (these should be the newest)
        top_links = new_links[:3] if new_links else []
        
        if top_links:
            # Replace the entire MAIN_LINKS_FILE with top 3 links
            self.save_links_to_file(self.main_links_file, top_links)
            logger.info(f"  Replaced {self.main_links_file} with {len(top_links)} top links")
            logger.info("📋 Top 3 latest links in main file:")
            for i, link in enumerate(top_links, 1):
                vehicle_id = self.extract_vehicle_id(link)
                logger.info(f"  {i}. {vehicle_id}")
        else:
            logger.info(f"ℹ️ No new links to update in main file")
        
        # Step 5: Save top 3 latest links to LATEST_LINKS_FILE (for cron job tracking)
        logger.info(f"\n⭐ Saving top 3 latest links to {self.latest_links_file}...")
        
        if top_links:
            self.save_links_to_file(self.latest_links_file, top_links)
            logger.info(f"  Saved {len(top_links)} links to {self.latest_links_file}")
        else:
            logger.info(f"ℹ️ No links to save as latest")
        
        # Final summary
        logger.info(f"\n{'='*80}")
        logger.info("🎉 SCRAPING COMPLETE!")
        
        # Return statistics
        stats = {
            "total_pages_scraped": "variable",  # Would need to track this
            "matched_existing_links": len(matched_links),
            "new_links_found": len(new_links),
            "top_links_saved": len(top_links),
            "previous_total_links": len(existing_links),
            "new_total_links": len(top_links)
        }
        
        logger.info(f"📊 Final Statistics: {stats}")
        
        return stats