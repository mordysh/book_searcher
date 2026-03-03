import os
import shutil
import requests
from bs4 import BeautifulSoup
from ddgs import DDGS
from concurrent.futures import ThreadPoolExecutor, as_completed
from thefuzz import fuzz
import json
import argparse
import re
import logging
from datetime import datetime
import ollama
import sys
import select
import termios
import tty
from colorama import Fore, Style, init
import time

init(autoreset=True)

SITES = [
    {"name": "evrit", "domain": "e-vrit.co.il", "id_regex": r"/Product/(\d+)", "author_regex": r"/Author/(\d+)", "group_regex": r"/Group/(\d+)"},
    {"name": "steimatzky", "domain": "steimatzky.co.il", "id_regex": r"/(\d+)"},
    {"name": "simania", "domain": "simania.co.il", "id_regex": r"/book/(\d+)"}
]

class BookSearcher:
    def __init__(self, args):
        self.args = args
        self.json_path = os.path.join(args.input, "search_results.json")
        self.state = self.load_state()
        self.stop_requested = False

    def load_state(self):
        if os.path.exists(self.json_path):
            try:
                with open(self.json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return {item["original_filename"]: item for item in data}
            except Exception: pass
        return {}

    def save_state(self):
        with open(self.json_path, "w", encoding="utf-8") as f:
            json.dump(list(self.state.values()), f, ensure_ascii=False, indent=4)

    def check_input(self):
        if sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
            char = sys.stdin.read(1).lower()
            if char in ("p", "q"):
                print(f"\n{Fore.YELLOW}Stop requested. Saving...{Style.RESET_ALL}")
                self.stop_requested = True

    def run(self):
        if not os.path.exists(self.args.input):
            print(f"{Fore.RED}Error: Input path '{self.args.input}' does not exist.{Style.RESET_ALL}")
            return
        all_f = [f for f in os.listdir(self.args.input) if os.path.isfile(os.path.join(self.args.input, f)) and not f.startswith(".")]
        files = [os.path.join(self.args.input, f) for f in all_f if f not in self.state and not f.endswith(".json")]
        if not files:
            print(f"{Fore.BLUE}No new files to process.{Style.RESET_ALL}")
            return
        print(f"{Fore.CYAN}Found {len(files)} files. Press p/q to stop.{Style.RESET_ALL}")
        old_settings = termios.tcgetattr(sys.stdin)
        tty.setcbreak(sys.stdin.fileno())
        try:
            with ThreadPoolExecutor(max_workers=self.args.threads) as executor:
                futures = {executor.submit(process_book, f, self.args.use_llm, self.args.model, self.args.verbose): f for f in files}
                for future in as_completed(futures):
                    if self.stop_requested: break
                    self.check_input()
                    book_data = future.result()
                    new_path = organize_file(book_data, self.args.input, self.args.dry_run)
                    
                    res = book_data["result"]
                    # If it's an author page but no book found, we don't mark as found
                    is_found = res is not None and res.get("type") in ["book", "book_from_author_page"]
                    
                    self.state[book_data["original_filename"]] = {
                        "original_filename": book_data["original_filename"],
                        "new_path": new_path,
                        "found": is_found,
                        "id": res.get("id") if is_found else None,
                        "llm_guess": book_data["llm_guess"],
                        "metadata": res,
                        "timestamp": datetime.now().isoformat()
                    }
                    self.save_state()
        finally:
            termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)
            self.save_state()

def clean_filename(f): return os.path.splitext(f)[0].replace("_"," ").replace("."," ").strip()

def normalize_hebrew(text):
    if not text: return ""
    # Replace geresh with apostrophe, and double geresh with quotes
    return text.replace("׳", "'").replace("״", '"').replace("`", "'").strip()

def extract_metadata_with_llm(f, model, v):
    try:
        clean_f = clean_filename(f)
        prompt = "Extract book title and author from Hebrew/English filename: " + clean_f + ". Return ONLY JSON like {\"title\":\"\", \"author\":\"\"}"
        r = ollama.chat(model=model, messages=[{"role":"user", "content":prompt}])
        m = re.search(r"\{.*\}", r["message"]["content"], re.DOTALL)
        if m:
            d = json.loads(m.group(0))
            if v >= 2: print(f"{Fore.BLACK}{Style.BRIGHT}  LLM: {d}{Style.RESET_ALL}")
            return d.get("title"), d.get("author")
    except Exception: pass
    return None, None

def get_book_details(url, site, site_config, target_title=None, v=0):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "he,en-US;q=0.7,en;q=0.3"
        }
        r = requests.get(url, headers=headers, timeout=10)
        r.encoding = "utf-8"
        if r.status_code != 200: return None
        s = BeautifulSoup(r.text, "html.parser")
        t, a, bid = "", "", None
        
        # Determine page type
        page_type = "unknown"
        if "/Product/" in url or (site == "steimatzky" and re.search(r"/\d+", url)):
            page_type = "book"
        elif "/Author/" in url:
            page_type = "author"
        elif "/Group/" in url:
            page_type = "group"

        # Try JSON-LD first
        scripts = s.find_all("script", type="application/ld+json")
        for script in scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, list): data = data[0]
                if data.get("@type") == "Book":
                    t = data.get("name", t)
                    if "author" in data:
                        a_data = data["author"]
                        if isinstance(a_data, list): a = ", ".join([x.get("name", x) if isinstance(x, dict) else x for x in a_data])
                        elif isinstance(a_data, dict): a = a_data.get("name", "")
                        else: a = a_data
                    bid = data.get("sku") or data.get("isbn")
                    break
            except Exception: pass

        if site == "evrit":
            if not t:
                t_tag = s.find("h1")
                if t_tag: t = t_tag.text.strip()
            if not a and page_type == "book":
                a_tag = s.find("a", href=lambda x: x and "/Author/" in x)
                if a_tag: a = a_tag.text.strip()
        elif site == "steimatzky":
            if not t:
                t_tag = s.find("span", {"itemprop": "name"})
                if t_tag: t = t_tag.text.strip()
            if not a:
                a_tag = s.find("div", {"class": "product-author"})
                if a_tag: a = a_tag.text.strip()
        elif site == "simania":
            if not t:
                t_tag = s.find("h2")
                if t_tag: t = t_tag.text.strip()
            if not a:
                a_tag = s.find("h3")
                if a_tag: a = a_tag.text.strip()
        
        if page_type in ["author", "group"] and target_title:
            # Try to find the book on this page
            norm_target = normalize_hebrew(target_title)
            
            # 1. Search in <a> tags
            links = s.find_all("a", href=lambda x: x and "/Product/" in x)
            for link in links:
                link_text = link.text.strip()
                if not link_text:
                    img = link.find("img")
                    if img and img.get("alt"): link_text = img.get("alt").strip()
                
                if link_text and fuzz.token_set_ratio(norm_target, normalize_hebrew(link_text)) >= 85:
                    book_url = link['href']
                    if not book_url.startswith("http"):
                        from urllib.parse import urljoin
                        book_url = urljoin(url, book_url)
                    match = re.search(site_config["id_regex"], book_url)
                    return {
                        "url": book_url,
                        "id": match.group(1) if match else None,
                        "title": link_text,
                        "author": t if page_type == "author" else "",
                        "site": site,
                        "type": "book_from_author_page",
                        "parent_url": url
                    }
            
            # 2. Search in script tags (for React-rendered pages like Evrit)
            if site == "evrit":
                all_scripts = s.find_all("script")
                for script in all_scripts:
                    if script.string and "ProductListItems" in script.string:
                        try:
                            # Extract JSON-like content from script
                            # Look for ProductListItems: [...]
                            m = re.search(r'"ProductListItems":\s*(\[.*?\])', script.string, re.DOTALL)
                            if m:
                                items = json.loads(m.group(1))
                                for item in items:
                                    item_name = item.get("Name", "")
                                    if item_name and fuzz.token_set_ratio(norm_target, normalize_hebrew(item_name)) >= 85:
                                        pid = str(item.get("ProductID"))
                                        # Construct URL: https://www.e-vrit.co.il/Product/ID/Name
                                        safe_name = item_name.replace(" ", "_")
                                        book_url = f"https://www.e-vrit.co.il/Product/{pid}/{safe_name}"
                                        return {
                                            "url": book_url,
                                            "id": pid,
                                            "title": item_name,
                                            "author": t if page_type == "author" else item.get("AuthorName", ""),
                                            "site": site,
                                            "type": "book_from_author_page",
                                            "parent_url": url
                                        }
                        except Exception as e:
                            if v >= 2: print(f"DEBUG: Error parsing script JSON: {e}")
        
        return {"title": t, "author": a, "id": bid, "type": page_type}
    except Exception: return None

def search_book_on_site(q, s, v):
    try:
        norm_q = normalize_hebrew(q)
        
        # Try to extract title if it's in format "Title (Author)" or "Title - Author"
        clean_q = norm_q
        book_title_only = norm_q
        
        if "(" in norm_q:
            m = re.match(r'(.*?)\s*\((.*?)\)', norm_q)
            if m:
                book_title_only = m.group(1).strip()
        elif " - " in norm_q:
            parts = norm_q.split(" - ")
            book_title_only = parts[0].strip() # Assume title is first
        
        queries = [norm_q]
        if book_title_only != norm_q:
            queries.append(book_title_only)
        
        # If query contains parentheses, add a version without them
        if "(" in norm_q:
            no_paren = re.sub(r'\(.*?\)', '', norm_q).strip()
            if no_paren not in queries:
                queries.append(no_paren)
        
        best_author_match = None

        for query in queries:
            strategies = [f"site:{s['domain']} {query}", f"{query} {s['name']}"]
            
            for search_query in strategies:
                if v >= 1: print(f"DEBUG: Querying {search_query}")
                try:
                    results = list(DDGS().text(search_query, region="il-he", max_results=5))
                except Exception as e:
                    if v >= 1: print(f"  DEBUG: DDG Error: {e}")
                    continue

                for r in results:
                    url = r['href']
                    if v >= 1: print(f"  DEBUG: DDG URL: {url}")
                    if s["domain"] in url:
                        d = get_book_details(url, s["name"], s, target_title=query, v=v)
                        if d:
                            if d.get("type") == "book_from_author_page":
                                return d
                            
                            norm_d_title = normalize_hebrew(d["title"])
                            r1 = fuzz.token_set_ratio(normalize_hebrew(query), norm_d_title)
                            r2 = fuzz.token_set_ratio(normalize_hebrew(book_title_only), norm_d_title)
                            
                            if v >= 1: print(f"  DEBUG: Title '{d['title']}' Ratios: {r1}, {r2} (Type: {d.get('type')})")
                            
                            if d.get("type") == "book":
                                if r1 >= 80 or r2 >= 80:
                                    bid = d.get("id")
                                    if not bid:
                                        match = re.search(s["id_regex"], url)
                                        bid = match.group(1) if match else None
                                    return {"url": url, "id": bid, "title": d["title"], "author": d["author"], "site": s["name"], "type": "book"}
                            elif d.get("type") in ["author", "group"]:
                                if r1 >= 80 or r2 >= 80:
                                    if not best_author_match:
                                        best_author_match = {"url": url, "id": None, "title": d["title"], "author": "", "site": s["name"], "type": d.get("type")}
        
        return best_author_match
    except Exception as e:
        if v >= 1: print(f"DEBUG: Error: {e}")
    return None


def process_book(f_path, use_llm, model, v):
    fname = os.path.basename(f_path)
    fname_no_ext = os.path.splitext(fname)[0]
    llm_t, llm_a = (None, None) if not use_llm else extract_metadata_with_llm(fname_no_ext, model, v)
    
    # Primary query logic
    if llm_t and llm_a: q = llm_t + " " + llm_a
    elif llm_t: q = llm_t
    else: q = clean_filename(fname_no_ext)

    print(f"{Fore.LIGHTBLUE_EX}Searching: {Style.DIM}{fname}{Style.RESET_ALL}")
    for site in SITES:
        res = search_book_on_site(q, site, v)
        if res:
            print(f"  {Fore.GREEN}✓ Found on {site["name"]}: {res["title"]} (ID: {res['id']}){Style.RESET_ALL}")
            return {"file": f_path, "original_filename": fname, "llm_guess": {"title": llm_t, "author": llm_a}, "result": res}
    return {"file": f_path, "original_filename": fname, "llm_guess": {"title": llm_t, "author": llm_a}, "result": None}

def organize_file(b_data, out_dir, dry_run):
    f_path, res = b_data["file"], b_data["result"]
    if not res: return None
    def safe(n):
        for char in ["/", "\\", "*", "?", ":", "\"", "<", ">", "|"]: n = n.replace(char, "")
        return n.strip().replace(" ", "_")
    new_f = f"{safe(res["author"] or "Unknown")}_{safe(res["title"] or "Unknown")}{os.path.splitext(f_path)[1]}"
    target = os.path.join(out_dir, f"found_on_{res["site"]}")
    if dry_run: return os.path.join(target, new_f)
    os.makedirs(target, exist_ok=True)
    n_path = os.path.join(target, new_f)
    try:
        shutil.move(f_path, n_path)
        print(f"  {Fore.MAGENTA}→ Moved to {res["site"]}/{new_f}{Style.RESET_ALL}")
        return n_path
    except Exception: return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", "-i", required=True)
    parser.add_argument("--threads", "-t", type=int, default=4)
    parser.add_argument("--verbose", "-v", action="count", default=0)
    parser.add_argument("--use-llm", action="store_true")
    parser.add_argument("--model", default="llama3.1")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    BookSearcher(args).run()
