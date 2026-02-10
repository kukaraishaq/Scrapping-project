import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urlparse, urljoin
import concurrent.futures
import time
import random
import json
import re
# Configuration
SITEMAP_URL = "https://www.duramotion.nl/sitemap.xml"
BASE_URL = "https://www.duramotion.nl"
MAX_WORKERS = 5
MAX_URLS = 10000 # Limit as per requirement
OUTPUT_FILE = "Duramotion_Full_Catalog.xlsx"
# Headers for Session
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,nl;q=0.8",
}
# Known non-product paths (blacklist) to skip during harvesting
BLACKLIST = [
    'contact', 'sitemap', 'nieuws', 'reviews', 'cookies', 'assortiment', 
    'categorieen', 'faq', 'vacatures', 'blog', 'team', 'partners', 
    'omvormen', 'kennis-partner', 'technische-support', 'klantportaal', 
    'projectmanagement', 'express-delivery', 'van-der-valk', 'recom', 
    'algemene-voorwaarden', 'privacy', 'enphase', 'his', 'links', 
    'nieuwsbrief-inschrijving', 'huawei', 'eaton', 'cimco', 'omvormers', 
    'elektra', 'kabels', 'klein-materiaal', 'onderconstructie', 
    'calculators', 'chint', 'sas-box', 'cah-caw'
]
def fetch_sitemap_urls(session):
    """Fetches sitemap and returns a list of POTENTIAL product URLs (Dutch)."""
    print(f"Fetching sitemap from {SITEMAP_URL}...")
    try:
        response = session.get(SITEMAP_URL)
        response.raise_for_status()
        # Use html.parser instead of xml to avoid lxml dependency issues
        soup = BeautifulSoup(response.content, 'html.parser')
        locs = soup.find_all('loc')
        print(f"DEBUG: Found {len(locs)} <loc> tags.")
        urls = [loc.text for loc in locs]
        
        # Filter: 
        # 1. Must contain '/nl/' (since sitemap is dutch only)
        # 2. Must NOT be in the blacklist (substring check on path segments)
        # 3. Path depth >= 2 (e.g. /nl/product-name)
        
        valid_urls = []
        for url in urls:
            if '/nl/' in url:
                path = urlparse(url).path.strip('/')
                parts = [p for p in path.split('/') if p] # e.g. ['nl', 'product-name']
                
                # Check depth
                if len(parts) < 2:
                    continue
                
                # Check blacklist
                # If the last segment is in blacklist, skip.
                # If the first segment after 'nl' is in blacklist (e.g. /nl/blog/post), skip.
                slug = parts[-1].lower()
                segment_after_lang = parts[1].lower() if len(parts) > 1 else ""
                if slug in BLACKLIST or segment_after_lang in BLACKLIST:
                    # print(f"Skipping Blacklisted: {slug} / {segment_after_lang}")
                    continue
                
                # Heuristic: Skip if looks like paginated list or special query (though sitemap usually static)
                valid_urls.append(url)
        
        # Deduplicate and Limit
        unique_urls = list(set(valid_urls))
        print(f"Total raw URLs: {len(urls)}")
        print(f"Total valid URLs: {len(unique_urls)}")
        if len(unique_urls) == 0:
            print("DEBUG: Sample URLs from sitemap:")
            for u in urls[:5]:
                print(f" - {u}")
                
        print(f"Found {len(unique_urls)} potential product URLs (Dutch). Processing first {MAX_URLS}...")
        return unique_urls[:MAX_URLS]
    except Exception as e:
        print(f"Error fetching sitemap: {e}")
        return []
def extract_product_data(session, dutch_url):
    """Extracts data: Fetches Dutch page -> Finds English Link -> Scrapes English Page."""
    # time.sleep(random.uniform(1, 2))  # Rate limiting
    try:
        # 1. Fetch Dutch Page
        # print(f"DEBUG: Processing {dutch_url}")
        r_nl = session.get(dutch_url, timeout=10)
        if r_nl.status_code != 200:
            print(f"DEBUG: Failed to fetch Dutch URL {dutch_url} ({r_nl.status_code})")
            return None
        
        soup_nl = BeautifulSoup(r_nl.text, 'html.parser')
        
        # 2. Find English URL
        target_soup = soup_nl
        target_url = dutch_url
        
        en_link_tag = soup_nl.find('link', attrs={'hreflang': 'en'})
        if en_link_tag and en_link_tag.get('href'):
            english_url = en_link_tag.get('href')
            # print(f"DEBUG: Found English URL: {english_url}")
            
            # 3. Fetch English Page
            time.sleep(random.uniform(0.5, 1.5))
            r_en = session.get(english_url, timeout=10)
            if r_en.status_code == 200:
                target_soup = BeautifulSoup(r_en.text, 'html.parser')
                target_url = english_url
            else:
                print(f"DEBUG: Failed to fetch English URL {english_url}, using Dutch")
        else:
            # print(f"DEBUG: No English link found for {dutch_url}, scraping Dutch")
            pass
            
        soup = target_soup
        
        # 4. Extract Data (Standard Logic)
        
        # Title
        title_tag = soup.find('h1')
        title = title_tag.get_text(strip=True) if title_tag else "N/A"
        
        # Product Code
        code = "N/A"
        code_div = soup.find('div', class_='zl_product_list_code')
        if code_div:
             code = code_div.get_text(strip=True)
        if code == "N/A":
            scripts = soup.find_all('script', type='application/ld+json')
            for script in scripts:
                try:
                    data = json.loads(script.string)
                    if isinstance(data, list): data = data[0]
                    if 'mpn' in data:
                        code = data['mpn']
                        break
                    elif 'sku' in data:
                         code = data['sku']
                         break
                except: continue
        
        # Description
        desc_div = soup.find('div', id='omschrijving')
        description = desc_div.get_text(separator='\n', strip=True) if desc_div else "N/A"
        
        # Image Link
        img_tag = soup.find('a', attrs={'rel': 'productImage'})
        image_link = urljoin(BASE_URL, img_tag.get('href')) if img_tag and img_tag.get('href') else "N/A"
            
        # PDF Link
        pdf_link = "N/A"
        pdf_tag = soup.select_one('a.fa-file-pdf')
        if pdf_tag and pdf_tag.get('href'):
             pdf_link = urljoin(BASE_URL, pdf_tag.get('href'))
        else:
            pdf_icon = soup.select_one('.fa-file-pdf')
            if pdf_icon:
                parent_a = pdf_icon.find_parent('a')
                if parent_a and parent_a.get('href'):
                     pdf_link = urljoin(BASE_URL, parent_a.get('href'))
        return {
            "Product Code": code,
            "Title": title,
            "Description": description,
            "Image Link": image_link,
            "PDF Link": pdf_link,
            "Source URL": target_url
        }
    except Exception as e:
        print(f"Error scraping {dutch_url} -> English: {e}")
        return None
def main():
    session = requests.Session()
    session.headers.update(HEADERS)
    
    # Phase 1: Harvest Dutch URLs
    dutch_urls = fetch_sitemap_urls(session)
    if not dutch_urls:
        print("No URLs found. Exiting.")
        return
    print(f"Starting extraction for {len(dutch_urls)} items (Dutch -> English)...")
    results = []
    
    # Phase 2: Extract
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {executor.submit(extract_product_data, session, url): url for url in dutch_urls}
        completed = 0
        for future in concurrent.futures.as_completed(future_to_url):
            data = future.result()
            if data:
                results.append(data)
            completed += 1
            if completed % 10 == 0:
                print(f"Progress: {completed}/{len(dutch_urls)}")
    # Phase 3: Export
    if not results:
        print("No results extracted. Check if English versions exist.")
        return
    print(f"Exporting {len(results)} items to Excel...")
    df = pd.DataFrame(results)
    
    # Create Excel writer
    writer = pd.ExcelWriter(OUTPUT_FILE, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='Catalog')
    
    workbook = writer.book
    worksheet = writer.sheets['Catalog']
    
    # Formats
    link_format = workbook.add_format({'font_color': 'blue', 'underline': 1})
    
    img_col_idx = df.columns.get_loc("Image Link")
    pdf_col_idx = df.columns.get_loc("PDF Link")
    
    for row_num, (img_link, pdf_link) in enumerate(zip(df['Image Link'], df['PDF Link'])):
        excel_row = row_num + 1
        
        if img_link != "N/A":
            worksheet.write_url(excel_row, img_col_idx, img_link, link_format, string=img_link)
        else:
            worksheet.write_string(excel_row, img_col_idx, "N/A")
            
        if pdf_link != "N/A":
            worksheet.write_url(excel_row, pdf_col_idx, pdf_link, link_format, string=pdf_link)
        else:
             worksheet.write_string(excel_row, pdf_col_idx, "N/A")
    writer.close()
    print(f"Done! Saved to {OUTPUT_FILE}")
if __name__ == "__main__":
    main()