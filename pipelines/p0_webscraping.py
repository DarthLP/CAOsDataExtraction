"""
Web Scraping Script for CAO PDF Downloads
Downloads PDFs from uitvoeringarbeidsvoorwaardenwetgeving.nl for specific CAO numbers
"""
import os
import sys
import time
import requests
import pandas as pd
from pathlib import Path

# Add the parent directory to Python path so we can import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
from utils.OUTPUT_tracker import update_progress
import traceback
import yaml
with open('conf/config.yaml', 'r') as f:
    config = yaml.safe_load(f)
WEBSITE_URL = (
    'https://www.uitvoeringarbeidsvoorwaardenwetgeving.nl/mozard/!suite16.scherm1168?mGmr=66'
    )
INPUT_EXCEL_PATH = (
    f"{config['paths']['inputs_excel']}/CAO_Frequencies_2014.xlsx")
OUTPUT_FOLDER = config['paths']['inputs_pdfs']
DOWNLOAD_DELAY = 2
MAX_RETRIES = 3
MAX_PDFS_PER_CAO = 10000

# Date filter configuration for CAO document search
MIN_INGANGSDATUM = '01-01-1900'  # Minimum start date (earliest documents to include)
MAX_INGANGSDATUM = '01-01-2006'  # Maximum start date (latest documents to include - gets pre-2006 docs)
extracted_data = []
all_main_link_logs = []
existing_info_df = None
existing_log_df = None
existing_pdf_names_by_cao = {}
existing_urls_by_cao = {}
existing_ids_by_cao = {}
if os.path.exists(os.path.join(OUTPUT_FOLDER, 'extracted_cao_info.csv')):
    existing_info_df = pd.read_csv(os.path.join(OUTPUT_FOLDER,
        'extracted_cao_info.csv'), sep=';')
    for _, row in existing_info_df.iterrows():
        cao = str(row['cao_number'])
        pdf_name = str(row['pdf_name'])
        id_val = str(row['id'])
        main_link_url = str(row.get('main_link_url', ''))
        existing_pdf_names_by_cao.setdefault(cao, set()).add(pdf_name)
        existing_ids_by_cao.setdefault(cao, set()).add(id_val)
        if main_link_url:
            existing_urls_by_cao.setdefault(cao, set()).add(main_link_url)
if os.path.exists(os.path.join(OUTPUT_FOLDER, 'main_links_log.csv')):
    existing_log_df = pd.read_csv(os.path.join(OUTPUT_FOLDER,
        'main_links_log.csv'), sep=';')
    # Also load URLs from main_links_log.csv for duplicate detection
    for _, row in existing_log_df.iterrows():
        cao = str(row['cao_number'])
        main_link_url = str(row.get('main_link_url', ''))
        if main_link_url:
            existing_urls_by_cao.setdefault(cao, set()).add(main_link_url)


def random_delay(min_seconds=0.5, max_seconds=1.2):
    """
    Sleep for a random duration between min_seconds and max_seconds.
    Used to mimic human-like interaction and avoid bot detection.
    """
    time.sleep(random.uniform(min_seconds, max_seconds))


def close_overlays(driver):
    """Try to close common overlays/popups if present."""
    try:
        close_selectors = ['button.close', '.modal-close', '.overlay-close',
            'button[aria-label="Close"]', '.ui-dialog-titlebar-close']
        for sel in close_selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, sel)
            for el in elements:
                try:
                    if el.is_displayed() and el.is_enabled():
                        el.click()
                        random_delay(0.2, 0.5)
                except Exception:
                    pass
    except Exception:
        pass


def setup_chrome_driver():
    """
    Set up and return a Selenium Chrome WebDriver with options for headless operation,
    anti-fingerprinting, and custom download preferences.
    Returns:
        driver (webdriver.Chrome): Configured Chrome WebDriver instance.
    """
    chrome_options = Options()
    prefs = {'download.default_directory': os.path.abspath(OUTPUT_FOLDER),
        'download.prompt_for_download': False, 'download.directory_upgrade':
        True, 'plugins.always_open_pdf_externally': True,
        'safebrowsing.enabled': True}
    chrome_options.add_experimental_option('prefs', prefs)
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--incognito')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled'
        )
    chrome_options.add_argument('--disable-web-security')
    chrome_options.add_argument('--allow-running-insecure-content')
    chrome_options.add_argument('--disable-features=VizDisplayCompositor')
    chrome_options.add_argument('--disable-ipc-flooding-protection')
    chrome_options.add_argument('--disable-canvas-aa')
    chrome_options.add_argument('--disable-2d-canvas-clip-aa')
    chrome_options.add_argument('--disable-gl-drawing-for-tests')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-plugins')
    chrome_options.add_argument('--disable-images')
    chrome_options.add_argument('--disable-background-timer-throttling')
    chrome_options.add_argument('--disable-backgrounding-occluded-windows')
    chrome_options.add_argument('--disable-renderer-backgrounding')
    chrome_options.add_argument('--disable-features=TranslateUI')
    chrome_options.add_argument('--disable-ipc-flooding-protection')
    chrome_options.add_argument('--window-size=1920,1080')
    user_agents = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ,
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
        ,
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'
        ,
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ,
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
        ]
    chrome_options.add_argument(f'--user-agent={random.choice(user_agents)}')
    chrome_options.add_experimental_option('excludeSwitches', [
        'enable-automation'])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
    driver.execute_script(
        "Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})"
        )
    driver.execute_script(
        "Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']})"
        )
    driver.execute_script(
        "Object.defineProperty(navigator, 'platform', {get: () => 'MacIntel'})"
        )
    return driver


def sanitize_filename(filename):
    filename = urllib.parse.unquote(filename)
    filename = re.sub('[<>:"/\\\\|?*]', '', filename)
    filename = re.sub('\\s+', ' ', filename).strip()
    return filename


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
        original_filename = sanitize_filename(original_filename)
        if not original_filename or original_filename == '':
            original_filename = f'{sanitize_filename(filename)}.pdf'
        base_name, ext = os.path.splitext(original_filename)
        counter = 1
        final_filename = original_filename
        while os.path.exists(os.path.join(output_folder, final_filename)):
            final_filename = f'{base_name}_{counter}{ext}'
            counter += 1
        file_path = os.path.join(output_folder, final_filename)
        response = requests.get(pdf_url, stream=True, timeout=30)
        if response.status_code == 200:
            with open(file_path, 'wb') as pdf_file:
                for chunk in response.iter_content(chunk_size=8192):
                    pdf_file.write(chunk)
            return final_filename
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
        for _ in range(2):
            try:
                search_box = WebDriverWait(driver, 12).until(EC.
                    element_to_be_clickable((By.XPATH, '//*[@id="mZoekGmr"]')))
                break
            except Exception:
                close_overlays(driver)
                random_delay(0.5, 1.0)
        else:
            return False
        driver.execute_script('arguments[0].scrollIntoView(true);', search_box)
        random_delay(0.3, 0.8)
        search_box.click()
        random_delay(0.3, 0.8)
        search_box.clear()
        random_delay(0.3, 0.8)
        cao_str = str(cao_number)
        for char in cao_str:
            search_box.send_keys(char)
            random_delay(0.08, 0.15)
        random_delay(0.5, 1.2)
        for _ in range(2):
            try:
                submit_button = WebDriverWait(driver, 8).until(EC.
                    element_to_be_clickable((By.XPATH,
                    '//*[@id="mZoekGmr_btn"]')))
                break
            except Exception:
                close_overlays(driver)
                random_delay(0.5, 1.0)
        else:
            return False
        driver.execute_script('arguments[0].scrollIntoView(true);',
            submit_button)
        random_delay(0.5, 1.2)
        submit_button.click()
        random_delay(0.5, 1.2)
        for _ in range(2):
            try:
                geselecteerd = WebDriverWait(driver, 12).until(EC.
                    element_to_be_clickable((By.XPATH,
                    "//span[text()='Geselecteerd']")))
                break
            except Exception:
                close_overlays(driver)
                random_delay(0.5, 1.0)
        else:
            return False
        driver.execute_script('arguments[0].scrollIntoView(true);',
            geselecteerd)
        random_delay(0.5, 1.2)
        geselecteerd.click()
        random_delay(0.5, 1.2)
        for _ in range(2):
            try:
                # Use name attribute to target the specific MIN ingangsdatum field
                # This is more reliable than class name when multiple elements have the same class
                # Note: Field names change dynamically, so we need to find them at runtime
                all_date_fields = driver.find_elements(By.CLASS_NAME, 'datumveld')
                min_field_name = None
                for field in all_date_fields:
                    field_name = field.get_attribute('name')
                    if field_name and '_dva' in field_name:
                        min_field_name = field_name
                        break
                
                if not min_field_name:
                    return False
                    
                date_field = WebDriverWait(driver, 8).until(EC.
                    element_to_be_clickable((By.NAME, min_field_name)))
                break
            except Exception:
                close_overlays(driver)
                random_delay(0.5, 1.0)
        else:
            return False
        driver.execute_script('arguments[0].scrollIntoView(true);', date_field)
        random_delay(0.5, 1.2)
        date_field.click()
        random_delay(0.5, 1.2)
        date_field.clear()
        random_delay(0.5, 1.2)
        # Set MIN ingangsdatum to include all available documents
        for char in MIN_INGANGSDATUM:
            date_field.send_keys(char)
            random_delay(0.08, 0.15)
        random_delay(0.5, 1.2)
        
        # Now set MAX ingangsdatum to 2006 to get only pre-2006 documents
        for _ in range(2):
            try:
                # Use name attribute to target the specific MAX ingangsdatum field
                # Note: Field names change dynamically, so we need to find them at runtime
                max_field_name = None
                for field in all_date_fields:
                    field_name = field.get_attribute('name')
                    if field_name and '_dtm' in field_name:
                        max_field_name = field_name
                        break
                
                if not max_field_name:
                    return False
                    
                max_date_field = WebDriverWait(driver, 8).until(EC.
                    element_to_be_clickable((By.NAME, max_field_name)))
                break
            except Exception:
                close_overlays(driver)
                random_delay(0.5, 1.0)
        else:
            return False
        driver.execute_script('arguments[0].scrollIntoView(true);', max_date_field)
        random_delay(0.5, 1.2)
        max_date_field.click()
        random_delay(0.5, 1.2)
        max_date_field.clear()
        random_delay(0.5, 1.2)
        # Set MAX ingangsdatum to get only pre-2006 CAO documents
        for char in MAX_INGANGSDATUM:
            max_date_field.send_keys(char)
            random_delay(0.08, 0.15)
        random_delay(0.5, 1.2)
        for _ in range(2):
            try:
                search_button = WebDriverWait(driver, 8).until(EC.
                    element_to_be_clickable((By.XPATH,
                    '//*[@id="moz_item_edit_modal_slaop"]')))
                break
            except Exception:
                close_overlays(driver)
                random_delay(0.5, 1.0)
        else:
            return False
        driver.execute_script('arguments[0].scrollIntoView(true);',
            search_button)
        random_delay(0.5, 1.2)
        search_button.click()
        random_delay(2.0, 2.5)
        return True
    except (TimeoutException, NoSuchElementException) as e:
        close_overlays(driver)
        return False
    except WebDriverException as e:
        close_overlays(driver)
        if 'no such window' in str(e).lower(
            ) or 'window already closed' in str(e).lower():
            return False
        else:
            return False
    except Exception as e:
        close_overlays(driver)
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
    info = {'cao_number': cao_number, 'id': f'{cao_number}{position:03d}',
        'ingangsdatum': '', 'expiratiedatum': '', 'datum_kennisgeving': '',
        'pdf_name': '', 'page_name': ''}
    try:
        try:
            page_name_element = driver.find_element(By.CSS_SELECTOR,
                'div.aandachttekst__tekst > span')
            info['page_name'] = page_name_element.text.strip()
        except NoSuchElementException:
            pass
        try:
            page_text = driver.find_element(By.TAG_NAME, 'body').text
        except:
            page_text = driver.page_source
        ingangs_patterns = ['Ingangsdatum\\s*:?\\s*(\\d{1,2}-\\d{1,2}-\\d{4})',
            'Ingangsdatum\\s*:?\\s*(\\d{1,2}/\\d{1,2}/\\d{4})',
            'Ingangsdatum\\s*:?\\s*(\\d{1,2}\\.\\d{1,2}\\.\\d{4})',
            'Ingangsdatum\\s*:?\\s*(\\d{4}-\\d{1,2}-\\d{1,2})',
            'Ingangsdatum\\s*:?\\s*(\\d{1,2}-\\d{1,2}-\\d{2})']
        for pattern in ingangs_patterns:
            ingangs_match = re.search(pattern, page_text, re.IGNORECASE)
            if ingangs_match:
                info['ingangsdatum'] = ingangs_match.group(1)
                break
        expiratie_patterns = [
            'Expiratiedatum\\s*:?\\s*(\\d{1,2}-\\d{1,2}-\\d{4})',
            'Expiratiedatum\\s*:?\\s*(\\d{1,2}/\\d{1,2}/\\d{4})',
            'Expiratiedatum\\s*:?\\s*(\\d{1,2}\\.\\d{1,2}\\.\\d{4})',
            'Expiratiedatum\\s*:?\\s*(\\d{4}-\\d{1,2}-\\d{1,2})',
            'Expiratiedatum\\s*:?\\s*(\\d{1,2}-\\d{1,2}-\\d{2})']
        for pattern in expiratie_patterns:
            expiratie_match = re.search(pattern, page_text, re.IGNORECASE)
            if expiratie_match:
                info['expiratiedatum'] = expiratie_match.group(1)
                break
        kennisgeving_patterns = [
            'Datum formele Kennisgeving van Ontvangst\\s*:?\\s*(\\d{1,2}-\\d{1,2}-\\d{4})'
            ,
            'Datum formele Kennisgeving van Ontvangst\\s*:?\\s*(\\d{1,2}/\\d{1,2}/\\d{4})'
            ,
            'Datum formele Kennisgeving van Ontvangst\\s*:?\\s*(\\d{1,2}\\.\\d{1,2}\\.\\d{4})'
            ,
            'Datum formele Kennisgeving van Ontvangst\\s*:?\\s*(\\d{4}-\\d{1,2}-\\d{1,2})'
            ,
            'Datum formele Kennisgeving van Ontvangst\\s*:?\\s*(\\d{1,2}-\\d{1,2}-\\d{2})'
            , 'kvo datum\\s*:?\\s*(\\d{1,2}-\\d{1,2}-\\d{4})',
            'kvo datum\\s*:?\\s*(\\d{1,2}/\\d{1,2}/\\d{4})']
        for pattern in kennisgeving_patterns:
            kennisgeving_match = re.search(pattern, page_text, re.IGNORECASE)
            if kennisgeving_match:
                info['datum_kennisgeving'] = kennisgeving_match.group(1)
                break
        pdf_links = driver.find_elements(By.CSS_SELECTOR, 'a.link--nochevron')
        for link in pdf_links:
            href = link.get_attribute('href')
            if href and href.endswith('.pdf'):
                parsed_url = urllib.parse.urlparse(href)
                original_filename = os.path.basename(parsed_url.path)
                original_filename = sanitize_filename(original_filename)
                info['pdf_name'] = original_filename
                break
    except Exception as e:
        print(f'    Error extracting page info: {e}')
    return info


def extract_pdf_links(driver, cao_number):
    """
    Extract PDF links and associated metadata from the current CAO page.
    Args:
        driver (webdriver.Chrome): Selenium WebDriver instance.
        cao_number (int or str): The CAO number being processed.
    Returns:
        list: List of dictionaries with PDF link info and metadata.
        list: List of main link logs (dicts with 'cao_number', 'main_link_url', 'pdf_found')
    """
    pdf_links = []
    main_link_logs = []
    position = 1
    for extraction_attempt in range(2):
        try:
            last_height = driver.execute_script(
                'return document.body.scrollHeight')
            while True:
                driver.execute_script(
                    'window.scrollTo(0, document.body.scrollHeight);')
                time.sleep(2)
                new_height = driver.execute_script(
                    'return document.body.scrollHeight')
                if new_height == last_height:
                    break
                last_height = new_height
            driver.execute_script('window.scrollTo(0, 0);')
            time.sleep(1)
            main_links = driver.find_elements(By.CSS_SELECTOR,
                'a.zaakregel__verwijzing')
            cao_number_str = str(cao_number)
            filtered_main_links = []
            for link in main_links:
                text = link.text.strip()
                if re.match(f'^{re.escape(cao_number_str)}[\\s\\-]', text):
                    filtered_main_links.append(link)
            main_links = filtered_main_links
            main_link_urls = []
            for link in main_links:
                url = link.get_attribute('href')
                if url:
                    main_link_urls.append(url)
            if not main_link_urls and extraction_attempt == 0:
                time.sleep(3)
                main_links = driver.find_elements(By.CSS_SELECTOR,
                    'a.zaakregel__verwijzing')
                cao_number_str = str(cao_number)
                filtered_main_links = []
                for link in main_links:
                    text = link.text.strip()
                    if re.match(f'^{re.escape(cao_number_str)}[\\s\\-]', text):
                        filtered_main_links.append(link)
                main_links = filtered_main_links
                main_link_urls = [link.get_attribute('href') for link in
                    main_links if link.get_attribute('href')]
            for main_link_url in main_link_urls:
                pdf_found = False
                found_pdf_name = ''
                pdfs_found_count = 0
                main_links = driver.find_elements(By.CSS_SELECTOR,
                    'a.zaakregel__verwijzing')
                link_to_click = None
                for link in main_links:
                    if link.get_attribute('href') == main_link_url:
                        link_to_click = link
                        break
                if not link_to_click:
                    main_link_logs.append({'cao_number': cao_number,
                        'main_link_url': main_link_url, 'pdf_found': False,
                        'pdf_name': '', 'pdfs_found_count': 0, 'id': ''})
                    continue
                try:
                    driver.execute_script('arguments[0].scrollIntoView(true);',
                        link_to_click)
                    link_to_click.click()
                    try:
                        WebDriverWait(driver, 5).until(EC.
                            presence_of_element_located((By.CSS_SELECTOR,
                            'a.link--nochevron')))
                    except TimeoutException:
                        pass
                    time.sleep(0.5)
                    extracted_info = 'default_filename'
                    try:
                        attention_text_div = driver.find_element(By.
                            CSS_SELECTOR, 'div.aandachttekst__tekst > span')
                        extracted_info = attention_text_div.text
                    except NoSuchElementException:
                        pass
                    nested_links = driver.find_elements(By.CSS_SELECTOR,
                        'a.link--nochevron')
                    pdf_urls = set()
                    for nl in nested_links:
                        href = nl.get_attribute('href')
                        if href and href.endswith('.pdf'):
                            pdf_urls.add(href)
                    pdfs_found_count = len(pdf_urls)
                    for nested_link in nested_links:
                        if MAX_PDFS_PER_CAO is not None and len(pdf_links
                            ) >= MAX_PDFS_PER_CAO:
                            break
                        try:
                            href = nested_link.get_attribute('href')
                            if href and href.endswith('.pdf'):
                                parsed_url = urllib.parse.urlparse(href)
                                original_filename = os.path.basename(parsed_url
                                    .path)
                                original_filename = sanitize_filename(
                                    original_filename)
                                page_info = extract_page_info(driver,
                                    cao_number, position)
                                page_info['pdf_name'] = original_filename
                                page_info['main_link_url'] = main_link_url
                                pdf_links.append({'url': href,
                                    'description': extracted_info,
                                    'page_info': page_info})
                                found_pdf_name = original_filename
                                pdf_found = True
                                id_value = f'{cao_number}{position:03d}'
                                position += 1
                                break
                        except Exception as e:
                            pass
                    main_link_logs.append({'cao_number': cao_number,
                        'main_link_url': main_link_url, 'pdf_found':
                        pdf_found, 'pdf_name': found_pdf_name,
                        'pdfs_found_count': pdfs_found_count, 'id': id_value})
                    driver.back()
                    try:
                        WebDriverWait(driver, 5).until(EC.
                            presence_of_element_located((By.CSS_SELECTOR,
                            'a.zaakregel__verwijzing')))
                    except TimeoutException:
                        pass
                    time.sleep(0.5)
                except StaleElementReferenceException:
                    continue
                except Exception as e:
                    driver.back()
                    time.sleep(3)
                    continue
            if pdf_links:
                break
        except Exception as e:
            if extraction_attempt == 0:
                print(
                    f'    First extraction attempt failed for CAO {cao_number}, retrying...'
                    )
                time.sleep(2)
            else:
                print(
                    f'    Both extraction attempts failed for CAO {cao_number}'
                    )
    return pdf_links, main_link_logs


def save_extracted_data():
    """
    Save the extracted metadata for all processed PDFs to a CSV file in the output folder.
    Returns:
        pd.DataFrame or None: DataFrame of extracted data if any, else None.
    """
    if extracted_data:
        df = pd.DataFrame(extracted_data)
        csv_path = os.path.join(OUTPUT_FOLDER, 'extracted_cao_info.csv')
        df.to_csv(csv_path, index=False, encoding='utf-8', sep=';')
        print(f'📄 Extracted information saved to: {csv_path}')
        return df
    return None


def process_cao_number(driver, cao_number):
    attempts_needed = 0
    for attempt in range(MAX_RETRIES):
        attempts_needed = attempt + 1
        try:
            driver.get(WEBSITE_URL)
            time.sleep(2)
            random_delay(0.5, 1.0)
            if search_cao_number(driver, cao_number):
                break
            else:
                print(
                    f'  Attempt {attempt + 1}/{MAX_RETRIES} failed for CAO {cao_number}'
                    )
                # Debug screenshot removed to avoid cluttering the directory
                pass
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2)
        except WebDriverException as e:
            print(
                f'  Attempt {attempt + 1}/{MAX_RETRIES} failed for CAO {cao_number}: {e}'
                )
            # Debug screenshot removed to avoid cluttering the directory
            pass
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)
        except Exception as e:
            print(
                f'  Attempt {attempt + 1}/{MAX_RETRIES} failed for CAO {cao_number}: {e}'
                )
            # Debug screenshot removed to avoid cluttering the directory
            pass
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)
    else:
        print(
            f'✗ Failed to search for CAO {cao_number} after {MAX_RETRIES} attempts'
            )
        return 0, [], []
    if attempts_needed > 1:
        print(
            f'  ✓ CAO {cao_number} succeeded after {attempts_needed} attempts')
    pdf_links, main_link_logs = extract_pdf_links(driver, cao_number)
    if not pdf_links:
        print(f'  No PDFs found for CAO {cao_number}')
        return 0, [], []
    cao_folder = os.path.join(OUTPUT_FOLDER, str(cao_number))
    os.makedirs(cao_folder, exist_ok=True)
    existing_pdfs = set(f for f in os.listdir(cao_folder) if f.lower().
        endswith('.pdf'))
    seen_urls = set()
    unique_pdf_links = []
    for link_info in pdf_links:
        url = link_info['url']
        if url not in seen_urls:
            seen_urls.add(url)
            unique_pdf_links.append(link_info)
    print(
        f'    Found {len(pdf_links)} PDF links, {len(unique_pdf_links)} unique URLs'
        )
    skipped = 0
    downloaded_count = 0
    downloaded_position = 1
    downloaded_data = []
    pdf_name_counts = {}
    cao_str = str(cao_number)
    existing_ids = existing_ids_by_cao.get(cao_str, set())
    max_id_num = 0
    for eid in existing_ids:
        if eid.startswith(cao_str):
            try:
                num = int(eid[len(cao_str):])
                if num > max_id_num:
                    max_id_num = num
            except:
                pass
    position = max_id_num + 1
    for link_info in unique_pdf_links:
        if (MAX_PDFS_PER_CAO is not None and downloaded_count >=
            MAX_PDFS_PER_CAO):
            break
        pdf_name = link_info['page_info'].get('pdf_name')
        if pdf_name is None:
            print(
                f"[FATAL] pdf_name is None for CAO {cao_number}, main_link_url: {link_info['page_info'].get('main_link_url', 'N/A')}"
                )
            print(f'         link_info: {link_info}')
            continue
        base_name, ext = os.path.splitext(pdf_name)
        count = pdf_name_counts.get(pdf_name, 0)
        if not isinstance(count, int) or count is None:
            count = 0
        pdf_name_counts[pdf_name] = count + 1
        if count > 0:
            new_pdf_name = f'{base_name}_{count}{ext}'
        else:
            new_pdf_name = pdf_name
        link_info['page_info']['pdf_name'] = new_pdf_name
        for log in main_link_logs:
            if log.get('main_link_url') == link_info['page_info'].get(
                'main_link_url'):
                log['pdf_name'] = new_pdf_name
        # Check if URL was already downloaded (more reliable than PDF name)
        main_link_url = link_info['page_info'].get('main_link_url', '')
        if main_link_url in existing_urls_by_cao.get(cao_str, set()):
            skipped += 1
            continue
        success = download_pdf(link_info['url'], link_info['description'],
            cao_folder)
        if success:
            print(f'    ⬇️ Downloaded PDF: {new_pdf_name}')
            downloaded_count += 1
            existing_pdfs.add(new_pdf_name)
            existing_pdf_names_by_cao.setdefault(cao_str, set()).add(
                new_pdf_name)
            # Also track the URL for future duplicate detection
            if main_link_url:
                existing_urls_by_cao.setdefault(cao_str, set()).add(main_link_url)
            new_id = f'{cao_str}{position:03d}'
            while new_id in existing_ids:
                position += 1
                new_id = f'{cao_str}{position:03d}'
            page_info = link_info['page_info']
            page_info['id'] = new_id
            existing_ids.add(new_id)
            position += 1
            downloaded_data.append(page_info)
        else:
            print(f'    ✗ Failed to download PDF: {new_pdf_name}')
        time.sleep(DOWNLOAD_DELAY)
    print(
        f'  Downloaded {downloaded_count} new PDFs for CAO {cao_number} (skipped {skipped})'
        )
    update_progress(cao_number, 'pdfs_found', successful=downloaded_count)
    return downloaded_count, downloaded_data, main_link_logs


def sync_excels_with_pdfs():
    """
    Remove rows from extracted_cao_info.csv and main_links_log.csv if the corresponding PDF file does not exist in the CAO folder.
    Handles case and whitespace differences. Prints missing files and removed rows.
    Only removes from main_links_log if pdf_found is True and the PDF is missing.
    Keeps id as string.
    """
    info_path = os.path.join(OUTPUT_FOLDER, 'extracted_cao_info.csv')
    log_path = os.path.join(OUTPUT_FOLDER, 'main_links_log.csv')
    if not os.path.exists(info_path):
        print('No extracted_cao_info.csv found. Nothing to sync.')
        return
    info_df = pd.read_csv(info_path, sep=';', dtype={'id': str})
    if os.path.exists(log_path):
        log_df = pd.read_csv(log_path, sep=';', dtype={'id': str})
    else:
        log_df = None
    keep_rows = []
    removed_rows = []
    for idx, row in info_df.iterrows():
        cao = str(row['cao_number']).strip()
        pdf_name = str(row['pdf_name']).strip()
        folder = os.path.join(OUTPUT_FOLDER, cao)
        if os.path.exists(folder):
            files = [f for f in os.listdir(folder) if os.path.isfile(os.
                path.join(folder, f))]
            files_norm = {f.lower().strip(): f for f in files}
            pdf_name_norm = pdf_name.lower().strip()
            if pdf_name_norm in files_norm:
                keep_rows.append(idx)
            else:
                removed_rows.append((cao, pdf_name))
                print(
                    f'[SYNC] Missing PDF: {os.path.join(folder, pdf_name)} (removing row)'
                    )
        else:
            removed_rows.append((cao, pdf_name))
            print(
                f'[SYNC] Missing folder: {folder} (removing row for {pdf_name})'
                )
    info_df = info_df.loc[keep_rows].reset_index(drop=True)
    if 'id' in info_df.columns and isinstance(info_df['id'], pd.Series):
        info_df['id'] = info_df['id'].fillna('').astype(str)
    info_df.to_csv(info_path, index=False, encoding='utf-8', sep=';')
    print(f'[SYNC] Updated {info_path}, removed {len(removed_rows)} rows.')
    if (log_df is not None and not log_df.empty and 'pdf_name' in log_df.
        columns and 'cao_number' in log_df.columns):
        valid_pairs = set(zip(info_df['cao_number'].astype(str).str.strip(),
            info_df['pdf_name'].astype(str).str.strip()))

        def keep_log_row(r):
            pair = str(r['cao_number']).strip(), str(r['pdf_name']).strip()
            if 'pdf_found' in r and r['pdf_found']:
                return pair in valid_pairs
            return True
        log_keep = log_df.apply(keep_log_row, axis=1)
        removed_log = log_df[~log_keep]
        log_df = log_df[log_keep].reset_index(drop=True)
        if 'id' in log_df.columns:
            log_df['id'] = pd.Series(log_df['id']).fillna('').astype(str)
        log_df.to_csv(log_path, index=False, encoding='utf-8', sep=';')
        print(f'[SYNC] Updated {log_path}, removed {len(removed_log)} rows.')


if __name__ == '__main__':
    sync_excels_with_pdfs()
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    try:
        df = pd.read_excel(INPUT_EXCEL_PATH)
        cao_series = pd.Series(df[df['Needed?'] == 'Yes']['CAO'])
        cao_numbers = cao_series.dropna().astype(int).tolist()
        print(
            f'📋 Found {len(cao_numbers)} CAO numbers to process: {cao_numbers}'
            )
    except Exception as e:
        print(f'✗ Error reading Excel file: {e}')
        exit(1)
    if not cao_numbers:
        print("✗ No CAO numbers found with 'Yes' in Needed? column")
        exit(1)
    total_downloaded = 0
    for i, cao_number in enumerate(cao_numbers, 1):
        print(f'\n📄 Processing {i}/{len(cao_numbers)}: CAO {cao_number}')
        driver = None
        try:
            driver = setup_chrome_driver()
            downloaded, downloaded_data, main_link_logs = process_cao_number(
                driver, cao_number)
            if downloaded is None:
                downloaded = 0
            total_downloaded += int(downloaded)
            extracted_data.extend(downloaded_data)
            all_main_link_logs.extend(main_link_logs)
            if i < len(cao_numbers):
                time.sleep(DOWNLOAD_DELAY)
        except Exception as e:
            print(f'✗ Error processing CAO {cao_number}: {e}')
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    if extracted_data or existing_info_df is not None:
        if existing_info_df is not None:
            df = pd.concat([existing_info_df, pd.DataFrame(extracted_data)],
                ignore_index=True)
            df = df.drop_duplicates(subset=['cao_number', 'pdf_name', 'id'])
        else:
            df = pd.DataFrame(extracted_data)
        if not df.empty and 'id' in df.columns and isinstance(df['id'], pd.
            Series):
            df['id'] = df['id'].fillna('').astype(str)
        csv_path = os.path.join(OUTPUT_FOLDER, 'extracted_cao_info.csv')
        df.to_csv(csv_path, index=False, encoding='utf-8', sep=';')
        print(f'📄 Extracted information saved to: {csv_path}')
    if all_main_link_logs or existing_log_df is not None:
        if existing_log_df is not None:
            df_log = pd.concat([existing_log_df, pd.DataFrame(
                all_main_link_logs)], ignore_index=True)
            df_log = df_log.drop_duplicates(subset=['cao_number',
                'main_link_url', 'id'])
        else:
            df_log = pd.DataFrame(all_main_link_logs)
        if not df_log.empty and 'id' in df_log.columns and isinstance(df_log
            ['id'], pd.Series):
            df_log['id'] = df_log['id'].fillna('').astype(str)
        log_path = os.path.join(OUTPUT_FOLDER, 'main_links_log.csv')
        df_log.to_csv(log_path, index=False, encoding='utf-8', sep=';')
        print(f'📄 Main link log saved to: {log_path}')
    print(f'\n✅ Download process completed!')
    print(f'📊 Total PDFs downloaded: {total_downloaded}')
    print(f'📁 Files saved in: {os.path.abspath(OUTPUT_FOLDER)}')
    if extracted_data:
        print(f'📋 Extracted information for {len(extracted_data)} PDFs')
