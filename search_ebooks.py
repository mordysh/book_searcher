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
from datetime import datetime
import ollama

# Sites to search
SITES = [
    {"name": "evrit", "domain": "e-vrit.co.il", "id_regex": r"/Product/(\d+)/"},
    {"name": "steimatzky", "domain": "steimatzky.co.il", "id_regex": r"/(\d+)$"},
    {"name": "simania", "domain": "simania.co.il", "id_regex": r"/book/(\d+)"}
]

def clean_filename(filename):
    name = os.path.splitext(filename)[0]
    name = re.sub(r'[\(\)\[\]\._-]', ' ', name)
    return name.strip()

def extract_metadata_with_llm(filename, model="llama3"):
    prompt = f"Extract the book title and author name from this filename: '{filename}'. Respond ONLY with a JSON object like: {{'title': '...', 'author': '...'}}"
    try:
        response = ollama.chat(model=model, messages=[{'role': 'user', 'content': prompt}])
        content = response['message']['content']
        match = re.search(r'\{.*\}', content, re.DOTALL)
        if match:
            data = json.loads(match.group(0))
            return data.get('title'), data.get('author')
    except Exception: pass
    return None, None

def fuzzy_match(query, result_title, threshold=80):
    score = fuzz.token_set_ratio(query, result_title)
    logging.debug(f"Fuzzy match score: {score} for '{query}' vs '{result_title}'")
    return score >= threshold

def get_book_details(url, site_name):
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'utf-8'
        if response.status_code != 200: return None
        soup = BeautifulSoup(response.text, 'html.parser')
        title, author = "", ""
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
    except Exception: return None

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
                    return {"url": url, "id": book_id, "title": details['title'], "author": details['author'], "site": site['name']}
    except Exception: pass
    return None

def process_book(file_path, use_llm=False, model="llama3"):
    filename = os.path.basename(file_path)
    llm_title, llm_author = None, None
    if use_llm:
        logging.info(f"LLM parsing: {filename}")
        llm_title, llm_author = extract_metadata_with_llm(filename, model)
    query = llm_title if llm_title else clean_filename(filename)
    logging.info(f"Processing: {filename} (Query: {query})")
    for site in SITES:
        result = search_book_on_site(query, site)
        if result: return {"file": file_path, "original_filename": filename, "result": result}
    return {"file": file_path, "original_filename": filename, "result": None}

def organize_file(book_data, output_dir):
    file_path, result = book_data['file'], book_data['result']
    if not result: return None
    def safe_name(name):
        name = re.sub(r'[\/*?:"<>|]', "", name)
        return name.strip().replace(" ", "_")
    new_filename = f"{safe_name(result['author'] or 'Unknown')}_{safe_name(result['title'] or 'Unknown')}{os.path.splitext(file_path)[1]}"
    target_dir = os.path.join(output_dir, f"found_on_{result['site']}")
    os.makedirs(target_dir, exist_ok=True)
    new_path = os.path.join(target_dir, new_filename)
    try:
        shutil.move(file_path, new_path)
        logging.info(f"Moved: {new_filename}")
        return new_path
    except Exception: return None

def main():
    parser = argparse.ArgumentParser(description="Search for ebook URLs.")
    parser.add_argument("--input", "-i", required=True)
    parser.add_argument("--threads", "-t", type=int, default=4)
    parser.add_argument("--verbose", "-v", action="count", default=0)
    parser.add_argument("--use-llm", action="store_true")
    parser.add_argument("--model", default="llama3")
    args = parser.parse_args()
    level = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}.get(args.verbose, logging.DEBUG)
    logging.basicConfig(level=level, format='%(levelname)s: %(message)s')
    if not os.path.exists(args.input): return
    files = [os.path.join(args.input, f) for f in os.listdir(args.input) if os.path.isfile(os.path.join(args.input, f)) and not f.startswith(".")]
    if not files: return
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        results = list(executor.map(lambda f: process_book(f, args.use_llm, args.model), files))
    final_metadata = []
    for book_data in results:
        new_path = organize_file(book_data, args.input)
        final_metadata.append({"original_filename": book_data['original_filename'], "new_path": new_path, "found": book_data['result'] is not None, "metadata": book_data['result'], "timestamp": datetime.now().isoformat()})
    with open(os.path.join(args.input, "search_results.json"), 'w', encoding='utf-8') as f:
        json.dump(final_metadata, f, ensure_ascii=False, indent=4)
if __name__ == "__main__":
    main()
