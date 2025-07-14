#!/usr/bin/env python3
"""
Web Scraping Script for CAO PDF Downloads
Downloads PDFs from uitvoeringarbeidsvoorwaardenwetgeving.nl for specific CAO numbers
"""

# =========================
# Imports and Configuration
# =========================
import os
import time
import requests
import pandas as pd
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import urllib.parse
import re
import random
from tracker import update_progress

# =========================
# Global Configuration
# =========================
WEBSITE_URL = "https://www.uitvoeringarbeidsvoorwaardenwetgeving.nl/mozard/!suite16.scherm1168?mGmr=66"
INPUT_EXCEL_PATH = "inputExcel/CAO_Frequencies_2014.xlsx"
OUTPUT_FOLDER = "input_pdfs"
DOWNLOAD_DELAY = 2  # Delay between downloads
MAX_RETRIES = 3
MAX_PDFS_PER_CAO = 1  # Set your desired limit here

# Initialize DataFrame to store extracted information
extracted_data = []


def random_delay(min_seconds=0.5, max_seconds=1.2):
    """
    Sleep for a random duration between min_seconds and max_seconds.
    Used to mimic human-like interaction and avoid bot detection.
    """
    time.sleep(random.uniform(min_seconds, max_seconds))


def setup_chrome_driver():
    """
    Set up and return a Selenium Chrome WebDriver with options for headless operation,
    anti-fingerprinting, and custom download preferences.
    Returns:
        driver (webdriver.Chrome): Configured Chrome WebDriver instance.
    """
    chrome_options = Options()
    
    # Set download preferences
    prefs = {
        "download.default_directory": os.path.abspath(OUTPUT_FOLDER),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,  # Download PDFs instead of opening in browser
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    # Headless mode and privacy settings
    chrome_options.add_argument("--headless")  # Run in background
    chrome_options.add_argument("--incognito")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument("--allow-running-insecure-content")
    chrome_options.add_argument("--disable-features=VizDisplayCompositor")
    chrome_options.add_argument("--disable-ipc-flooding-protection")
    
    # Anti-fingerprinting measures
    chrome_options.add_argument("--disable-canvas-aa")  # Disable canvas anti-aliasing
    chrome_options.add_argument("--disable-2d-canvas-clip-aa")  # Disable 2D canvas clip anti-aliasing
    chrome_options.add_argument("--disable-gl-drawing-for-tests")  # Disable GL drawing
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-plugins")
    chrome_options.add_argument("--disable-images")  # Faster loading
    chrome_options.add_argument("--disable-background-timer-throttling")
    chrome_options.add_argument("--disable-backgrounding-occluded-windows")
    chrome_options.add_argument("--disable-renderer-backgrounding")
    chrome_options.add_argument("--disable-features=TranslateUI")
    chrome_options.add_argument("--disable-ipc-flooding-protection")
    
    # Fixed window size for headless mode
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Random user agent
    user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    ]
    chrome_options.add_argument(f"--user-agent={random.choice(user_agents)}")
    
    # Remove automation indicators
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # Install and setup ChromeDriver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # Execute scripts to remove webdriver properties and fingerprinting
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
    driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']})")
    driver.execute_script("Object.defineProperty(navigator, 'platform', {get: () => 'MacIntel'})")
    
    return driver


def download_pdf(pdf_url, filename, output_folder):
    """
    Download a PDF file from the given URL and save it in the specified output folder.
    Args:
        pdf_url (str): URL of the PDF to download.
        filename (str): Fallback filename if the original cannot be determined.
        output_folder (str): Directory to save the PDF.
    Returns:
        str or None: The filename used if successful, None otherwise.
    """
    try:
        os.makedirs(output_folder, exist_ok=True)
        parsed_url = urllib.parse.urlparse(pdf_url)
        original_filename = os.path.basename(parsed_url.path)
        if not original_filename or original_filename == '':
            original_filename = f"{filename}.pdf"
        file_path = os.path.join(output_folder, original_filename)
        response = requests.get(pdf_url, stream=True, timeout=30)
        if response.status_code == 200:
            with open(file_path, "wb") as pdf_file:
                for chunk in response.iter_content(chunk_size=8192):
                    pdf_file.write(chunk)
            return original_filename  # Return the actual filename used
        else:
            return None
    except Exception as e:
        return None


def search_cao_number(driver, cao_number):
    """
    Search for a specific CAO number on the website using the provided Selenium driver.
    Args:
        driver (webdriver.Chrome): Selenium WebDriver instance.
        cao_number (int or str): The CAO number to search for.
    Returns:
        bool: True if search and navigation succeeded, False otherwise.
    """
    try:
        # Wait longer for the search box to be available
        search_box = WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="mZoekGmr"]'))
        )
        
        # More human-like interaction
        driver.execute_script("arguments[0].scrollIntoView(true);", search_box)
        random_delay(0.3, 0.8)
        search_box.click()
        random_delay(0.3, 0.8)
        search_box.clear()
        random_delay(0.3, 0.8)
        
        # Type the CAO number character by character (more human-like)
        cao_str = str(cao_number)
        for char in cao_str:
            search_box.send_keys(char)
            random_delay(0.08, 0.15)
        
        random_delay(0.5, 1.2)
        
        # Click the search button
        submit_button = driver.find_element(By.XPATH, '//*[@id="mZoekGmr_btn"]')
        driver.execute_script("arguments[0].scrollIntoView(true);", submit_button)
        random_delay(0.5, 1.2)
        submit_button.click()
        
        # Wait longer for results to load
        time.sleep(2)
        
        # Click on "Geselecteerd" (Selected) with longer wait
        geselecteerd = WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable((By.XPATH, "//span[text()='Geselecteerd']"))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", geselecteerd)
        random_delay(0.5, 1.2)
        geselecteerd.click()
        random_delay(0.5, 1.2)
        
        # Set the date to 01-01-2006 to filter PDFs from 2006 onwards
        date_field = WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable((By.CLASS_NAME, "datumveld"))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", date_field)
        random_delay(0.5, 1.2)
        date_field.click()
        random_delay(0.5, 1.2)
        date_field.clear()
        random_delay(0.5, 1.2)
        
        # Type the date character by character
        date_str = "01-01-2006"
        for char in date_str:
            date_field.send_keys(char)
            random_delay(0.08, 0.15)
        
        random_delay(0.5, 1.2)
        
        # Click the search button in the modal
        search_button = driver.find_element(By.XPATH, '//*[@id="moz_item_edit_modal_slaop"]')
        driver.execute_script("arguments[0].scrollIntoView(true);", search_button)
        random_delay(0.5, 1.2)
        search_button.click()
        
        # Wait longer for the results page to load
        time.sleep(2)
        
        return True
        
    except (TimeoutException, NoSuchElementException) as e:
        print(f"✗ Error searching for CAO {cao_number}: {e}")
        return False
    except WebDriverException as e:
        if "no such window" in str(e).lower() or "window already closed" in str(e).lower():
            print(f"✗ Browser window closed during search for CAO {cao_number}")
            return False
        else:
            print(f"✗ WebDriver error searching for CAO {cao_number}: {e}")
            return False
    except Exception as e:
        print(f"✗ Unexpected error searching for CAO {cao_number}: {e}")
        return False


def extract_page_info(driver, cao_number, position):
    """
    Extract metadata and PDF filename from the current page.
    Args:
        driver (webdriver.Chrome): Selenium WebDriver instance.
        cao_number (int or str): The CAO number being processed.
        position (int): The position/index of the PDF for this CAO.
    Returns:
        dict: Dictionary with extracted metadata and PDF filename.
    """
    info = {
        'cao_number': cao_number,
        'id': f"{cao_number}{position:03d}",  # e.g., 633001, 533004
        'ingangsdatum': '',
        'expiratiedatum': '',
        'datum_kennisgeving': '',
        'pdf_name': '',
        'page_name': ''
    }
    
    try:
        # Extract page name (the main title)
        try:
            page_name_element = driver.find_element(By.CSS_SELECTOR, "div.aandachttekst__tekst > span")
            info['page_name'] = page_name_element.text.strip()
        except NoSuchElementException:
            pass
        
        # Get the visible text content (not page source) for better date extraction
        try:
            page_text = driver.find_element(By.TAG_NAME, "body").text
        except:
            page_text = driver.page_source
        
        # More flexible regex patterns for date extraction
        # Look for various date formats and spacing patterns
        
        # Extract Ingangsdatum - multiple patterns
        ingangs_patterns = [
            r'Ingangsdatum\s*:?\s*(\d{1,2}-\d{1,2}-\d{4})',
            r'Ingangsdatum\s*:?\s*(\d{1,2}/\d{1,2}/\d{4})',
            r'Ingangsdatum\s*:?\s*(\d{1,2}\.\d{1,2}\.\d{4})',
            r'Ingangsdatum\s*:?\s*(\d{4}-\d{1,2}-\d{1,2})',
            r'Ingangsdatum\s*:?\s*(\d{1,2}-\d{1,2}-\d{2})',
        ]
        
        for pattern in ingangs_patterns:
            ingangs_match = re.search(pattern, page_text, re.IGNORECASE)
            if ingangs_match:
                info['ingangsdatum'] = ingangs_match.group(1)
                break
        
        # Extract Expiratiedatum - multiple patterns
        expiratie_patterns = [
            r'Expiratiedatum\s*:?\s*(\d{1,2}-\d{1,2}-\d{4})',
            r'Expiratiedatum\s*:?\s*(\d{1,2}/\d{1,2}/\d{4})',
            r'Expiratiedatum\s*:?\s*(\d{1,2}\.\d{1,2}\.\d{4})',
            r'Expiratiedatum\s*:?\s*(\d{4}-\d{1,2}-\d{1,2})',
            r'Expiratiedatum\s*:?\s*(\d{1,2}-\d{1,2}-\d{2})',
        ]
        
        for pattern in expiratie_patterns:
            expiratie_match = re.search(pattern, page_text, re.IGNORECASE)
            if expiratie_match:
                info['expiratiedatum'] = expiratie_match.group(1)
                break
        
        # Extract Datum formele Kennisgeving van Ontvangst - multiple patterns
        kennisgeving_patterns = [
            r'Datum formele Kennisgeving van Ontvangst\s*:?\s*(\d{1,2}-\d{1,2}-\d{4})',
            r'Datum formele Kennisgeving van Ontvangst\s*:?\s*(\d{1,2}/\d{1,2}/\d{4})',
            r'Datum formele Kennisgeving van Ontvangst\s*:?\s*(\d{1,2}\.\d{1,2}\.\d{4})',
            r'Datum formele Kennisgeving van Ontvangst\s*:?\s*(\d{4}-\d{1,2}-\d{1,2})',
            r'Datum formele Kennisgeving van Ontvangst\s*:?\s*(\d{1,2}-\d{1,2}-\d{2})',
            r'kvo datum\s*:?\s*(\d{1,2}-\d{1,2}-\d{4})',  # Alternative format
            r'kvo datum\s*:?\s*(\d{1,2}/\d{1,2}/\d{4})',  # Alternative format
        ]
        
        for pattern in kennisgeving_patterns:
            kennisgeving_match = re.search(pattern, page_text, re.IGNORECASE)
            if kennisgeving_match:
                info['datum_kennisgeving'] = kennisgeving_match.group(1)
                break
        
        # Extract PDF name from the link
        pdf_links = driver.find_elements(By.CSS_SELECTOR, "a.link--nochevron")
        for link in pdf_links:
            href = link.get_attribute("href")
            if href and href.endswith(".pdf"):
                parsed_url = urllib.parse.urlparse(href)
                original_filename = os.path.basename(parsed_url.path)
                # URL decode the filename
                original_filename = urllib.parse.unquote(original_filename)
                info['pdf_name'] = original_filename
                break
        
                
    except Exception as e:
        print(f"    Error extracting page info: {e}")
    
    return info


def extract_pdf_links(driver, cao_number):
    """
    Extract PDF links and associated metadata from the current CAO page.
    Args:
        driver (webdriver.Chrome): Selenium WebDriver instance.
        cao_number (int or str): The CAO number being processed.
    Returns:
        list: List of dictionaries with PDF link info and metadata.
    """
    pdf_links = []
    position = 1
    
    # Try up to 2 times to extract PDFs
    for extraction_attempt in range(2):
        try:
            # Find all main links (zaakregel__verwijzing)
            main_links = driver.find_elements(By.CSS_SELECTOR, "a.zaakregel__verwijzing")
            
            if not main_links and extraction_attempt == 0:
                # If no links found on first attempt, wait a bit and try again
                time.sleep(3)
                main_links = driver.find_elements(By.CSS_SELECTOR, "a.zaakregel__verwijzing")
            
            for i, main_link in enumerate(main_links):
                # Stop if we've found enough PDFs (only if MAX_PDFS_PER_CAO is not None)
                if MAX_PDFS_PER_CAO is not None and len(pdf_links) >= MAX_PDFS_PER_CAO:
                    break
                    
                try:
                    # Scroll to the link and click it
                    driver.execute_script("arguments[0].scrollIntoView(true);", main_link)
                    main_link.click()
                    
                    # Wait for the new page to load
                    time.sleep(0.5)
                    
                    # Extract information for filename and metadata
                    extracted_info = "default_filename"
                    try:
                        attention_text_div = driver.find_element(By.CSS_SELECTOR, "div.aandachttekst__tekst > span")
                        extracted_info = attention_text_div.text
                    except NoSuchElementException:
                        pass
                    
                    # Extract page information
                    page_info = extract_page_info(driver, cao_number, position)
                    
                    # Find PDF links on this page
                    nested_links = driver.find_elements(By.CSS_SELECTOR, "a.link--nochevron")
                    for nested_link in nested_links:
                        # Stop if we've found enough PDFs (only if MAX_PDFS_PER_CAO is not None)
                        if MAX_PDFS_PER_CAO is not None and len(pdf_links) >= MAX_PDFS_PER_CAO:
                            break
                            
                        try:
                            href = nested_link.get_attribute("href")
                            if href and href.endswith(".pdf"):
                                parsed_url = urllib.parse.urlparse(href)
                                original_filename = os.path.basename(parsed_url.path)
                                original_filename = urllib.parse.unquote(original_filename)
                                page_info['pdf_name'] = original_filename
                                pdf_links.append({
                                    'url': href,
                                    'description': extracted_info,
                                    'page_info': page_info
                                })
                                # Add to extracted data
                                extracted_data.append(page_info)
                                position += 1
                        except Exception as e:
                            pass
                    
                    # Go back to the main page
                    driver.back()
                    time.sleep(0.5)
                    
                except StaleElementReferenceException:
                    continue
                except Exception as e:
                    driver.back()
                    time.sleep(3)
                    continue
            
            # If we found PDFs, break out of retry loop
            if pdf_links:
                break
                
        except Exception as e:
            if extraction_attempt == 0:
                print(f"    First extraction attempt failed for CAO {cao_number}, retrying...")
                time.sleep(2)
            else:
                print(f"    Both extraction attempts failed for CAO {cao_number}")
    
    return pdf_links


def save_extracted_data():
    """
    Save the extracted metadata for all processed PDFs to a CSV file in the output folder.
    Returns:
        pd.DataFrame or None: DataFrame of extracted data if any, else None.
    """
    if extracted_data:
        df = pd.DataFrame(extracted_data)
        csv_path = os.path.join(OUTPUT_FOLDER, "extracted_cao_info.csv")
        df.to_csv(csv_path, index=False, encoding='utf-8', sep=';')
        print(f"📄 Extracted information saved to: {csv_path}")
        return df
    return None


def process_cao_number(driver, cao_number):
    """
    Orchestrate the process for a single CAO number: search, extract links, and download PDFs.
    Args:
        driver (webdriver.Chrome): Selenium WebDriver instance.
        cao_number (int or str): The CAO number to process.
    Returns:
        int: Number of PDFs successfully downloaded for this CAO.
    """
    attempts_needed = 0
    for attempt in range(MAX_RETRIES):
        attempts_needed = attempt + 1
        try:
            # Navigate to the website
            driver.get(WEBSITE_URL)
            time.sleep(2)  # Page load wait
            
            # Search for the CAO number
            if search_cao_number(driver, cao_number):
                break
            else:
                print(f"  Attempt {attempt + 1}/{MAX_RETRIES} failed for CAO {cao_number}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2)  # Retry delay
        except WebDriverException as e:
            if "no such window" in str(e).lower() or "window already closed" in str(e).lower():
                print(f"  Attempt {attempt + 1}/{MAX_RETRIES} failed for CAO {cao_number}: Browser window closed")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2)  # Window issue delay
            else:
                print(f"  Attempt {attempt + 1}/{MAX_RETRIES} failed for CAO {cao_number}: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2)  # Retry delay
        except Exception as e:
            print(f"  Attempt {attempt + 1}/{MAX_RETRIES} failed for CAO {cao_number}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)  # Retry delay
    else:
        print(f"✗ Failed to search for CAO {cao_number} after {MAX_RETRIES} attempts")
        return 0
    
    # Show retry information if more than 1 attempt was needed
    if attempts_needed > 1:
        print(f"  ✓ CAO {cao_number} succeeded after {attempts_needed} attempts")
    
    # Extract PDF links
    pdf_links = extract_pdf_links(driver, cao_number)
    
    if not pdf_links:
        print(f"  No PDFs found for CAO {cao_number}")
        return 0
    
    # Create a subfolder for this CAO number
    cao_folder = os.path.join(OUTPUT_FOLDER, str(cao_number))
    os.makedirs(cao_folder, exist_ok=True)
    
    # Get list of already downloaded PDF filenames in the folder
    existing_pdfs = set(f for f in os.listdir(cao_folder) if f.lower().endswith('.pdf'))
    # Track which PDFs are skipped or downloaded
    skipped = 0
    # Download PDFs (all if MAX_PDFS_PER_CAO is None, otherwise up to the limit)
    downloaded_count = 0
    for link_info in pdf_links:
        if MAX_PDFS_PER_CAO is not None and downloaded_count >= MAX_PDFS_PER_CAO:
            break
        pdf_name = link_info['page_info'].get('pdf_name')
        if pdf_name in existing_pdfs:
            print(f"    ⏩ Skipping already downloaded PDF: {pdf_name}")
            skipped += 1
            continue
        success = download_pdf(
            link_info['url'], 
            link_info['description'], 
            cao_folder
        )
        if success:
            print(f"    ⬇️ Downloaded PDF: {pdf_name}")
            downloaded_count += 1
            existing_pdfs.add(pdf_name)
        else:
            print(f"    ✗ Failed to download PDF: {pdf_name}")
        time.sleep(DOWNLOAD_DELAY)
    print(f"  Downloaded {downloaded_count} new PDFs for CAO {cao_number} (skipped {skipped})")
    # Update tracker
    update_progress(cao_number, "pdfs_found", successful=downloaded_count)
    return downloaded_count


def main():
    """
    Main function to orchestrate the web scraping process for all CAO numbers listed in the Excel file.
    Reads CAO numbers, processes each, and saves extracted metadata.
    """
    
    # Ensure output folder exists
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    # Read CAO numbers from Excel file
    try:
        df = pd.read_excel(INPUT_EXCEL_PATH)
        cao_series = pd.Series(df[df['Needed?'] == 'Yes']['CAO'])
        cao_numbers = cao_series.dropna().astype(int).tolist()
        print(f"📋 Found {len(cao_numbers)} CAO numbers to process: {cao_numbers}")
    except Exception as e:
        print(f"✗ Error reading Excel file: {e}")
        return
    
    if not cao_numbers:
        print("✗ No CAO numbers found with 'Yes' in Needed? column")
        return
    
    total_downloaded = 0
    
    for i, cao_number in enumerate(cao_numbers, 1):
        print(f"\n📄 Processing {i}/{len(cao_numbers)}: CAO {cao_number}")
        
        # Create a new driver for each CAO to avoid session issues
        driver = None
        try:
            driver = setup_chrome_driver()
            
            downloaded = process_cao_number(driver, cao_number)
            total_downloaded += downloaded
            
            # Add longer delay between CAO numbers to avoid rate limiting
            if i < len(cao_numbers):
                time.sleep(DOWNLOAD_DELAY)
                
        except Exception as e:
            print(f"✗ Error processing CAO {cao_number}: {e}")
        finally:
            # Always close the driver
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    
    # Save extracted data
    df_extracted = save_extracted_data()
    
    print(f"\n✅ Download process completed!")
    print(f"📊 Total PDFs downloaded: {total_downloaded}")
    print(f"📁 Files saved in: {os.path.abspath(OUTPUT_FOLDER)}")
    
    if df_extracted is not None:
        print(f"📋 Extracted information for {len(df_extracted)} PDFs")

if __name__ == "__main__":
    main()
