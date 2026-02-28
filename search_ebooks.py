import os
import shutil
import requests
from bs4 import BeautifulSoup
from googlesearch import search
from concurrent.futures import ThreadPoolExecutor
from thefuzz import fuzz
import json
import argparse
import re
import logging

# Sites to search
SITES = [
    {"name": "evrit", "domain": "e-vrit.co.il", "id_regex": r"/Product/(\d+)/"},
    {"name": "steimatzky", "domain": "steimatzky.co.il", "id_regex": r"/(\d+)$"},
    {"name": "simania", "domain": "simania.co.il", "id_regex": r"/book/(\d+)"}
]

def clean_filename(filename):
    name = os.path.splitext(filename)[0]
    # Remove common junk but keep Unicode letters
    name = re.sub(r'[\(\)\[\]\._-]', ' ', name)
    return name.strip()

def fuzzy_match(query, result_title, threshold=80):
    score = fuzz.token_set_ratio(query, result_title)
    logging.debug(f"Fuzzy match score: {score} for '{query}' vs '{result_title}'")
    return score >= threshold

def get_book_details(url, site_name):
    try:
        logging.debug(f"Scraping details from: {url}")
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'utf-8' # Ensure UTF-8
        if response.status_code != 200:
            logging.warning(f"Failed to fetch {url}: Status {response.status_code}")
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        title = ""
        author = ""
        
        if site_name == "evrit":
            title_tag = soup.find("h1")
            author_tag = soup.find("a", {"class": "author-link"})
            if title_tag: title = title_tag.text.strip()
            if author_tag: author = author_tag.text.strip()
        elif site_name == "steimatzky":
            title_tag = soup.find("span", {"itemprop": "name"})
            author_tag = soup.find("div", {"class": "product-author"})
            if title_tag: title = title_tag.text.strip()
            if author_tag: author = author_tag.text.strip()
        elif site_name == "simania":
            title_tag = soup.find("h2")
            author_tag = soup.find("h3")
            if title_tag: title = title_tag.text.strip()
            if author_tag: author = author_tag.text.strip()

        return {"title": title, "author": author}
    except Exception as e:
        logging.error(f"Error scraping {url}: {e}")
        return None

def search_book_on_site(query, site):
    full_query = f"site:{site['domain']} {query}"
    logging.debug(f"Searching Google: {full_query}")
    try:
        for url in search(full_query, num_results=3):
            if site['domain'] in url:
                match = re.search(site['id_regex'], url)
                book_id = match.group(1) if match else None
                
                details = get_book_details(url, site['name'])
                if details and fuzzy_match(query, details['title']):
                    logging.info(f"Matched on {site['name']}: {details['title']} (ID: {book_id})")
                    return {
                        "url": url,
                        "id": book_id,
                        "title": details['title'],
                        "author": details['author'],
                        "site": site['name']
                    }
    except Exception as e:
        logging.error(f"Error searching for {query} on {site['name']}: {e}")
    return None

def process_book(file_path):
    filename = os.path.basename(file_path)
    query = clean_filename(filename)
    logging.info(f"Processing: {filename}")
    
    for site in SITES:
        logging.debug(f"Trying site: {site['name']} for '{query}'")
        result = search_book_on_site(query, site)
        if result:
            return {"file": file_path, "result": result}
    
    logging.warning(f"No match found for: {filename}")
    return {"file": file_path, "result": None}

def organize_file(book_data, output_dir):
    file_path = book_data['file']
    result = book_data['result']
    
    if not result:
        return

    site_name = result['site']
    author = result['author'] or "UnknownAuthor"
    title = result['title'] or "UnknownTitle"
    
    # Preserve Unicode letters but remove illegal filesystem chars
    def safe_name(name):
        # Keep any character that is a letter/digit (including Unicode) or space
        # Then replace spaces with underscores and remove illegal chars
        name = re.sub(r'[\/*?:"<>|]', "", name)
        return name.strip().replace(" ", "_")

    safe_author = safe_name(author)
    safe_title = safe_name(title)
    
    ext = os.path.splitext(file_path)[1]
    new_filename = f"{safe_author}_{safe_title}{ext}"
    
    target_dir = os.path.join(output_dir, f"found_on_{site_name}")
    os.makedirs(target_dir, exist_ok=True)
    
    new_path = os.path.join(target_dir, new_filename)
    shutil.move(file_path, new_path)
    logging.info(f"Moved: {new_filename} -> {target_dir}")

def main():
    parser = argparse.ArgumentParser(description="Search for ebook URLs and organize files.")
    parser.add_argument("--input", "-i", required=True, help="Directory containing ebook files")
    parser.add_argument("--threads", "-t", type=int, default=4, help="Number of concurrent threads")
    parser.add_argument("--verbose", "-v", action="count", default=0, help="Increase verbosity (-v, -vv, -vvv)")
    args = parser.parse_args()

    if args.verbose == 0:
        level = logging.WARNING
    elif args.verbose == 1:
        level = logging.INFO
    elif args.verbose == 2:
        level = logging.DEBUG
    else:
        level = logging.DEBUG

    logging.basicConfig(level=level, format='%(levelname)s: %(message)s')

    if not os.path.exists(args.input):
        logging.error(f"Input directory {args.input} does not exist.")
        return

    files = [os.path.join(args.input, f) for f in os.listdir(args.input) 
             if os.path.isfile(os.path.join(args.input, f)) and not f.startswith(".")]

    if not files:
        logging.warning("No files found in input directory.")
        return

    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        results = list(executor.map(process_book, files))

    for book_data in results:
        organize_file(book_data, args.input)

if __name__ == "__main__":
    main()
