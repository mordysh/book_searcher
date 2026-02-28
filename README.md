# book_searcher

Ebook URL lookup and organization tool.

## Features
- Search for book IDs and URLs on:
  - e-vrit.co.il
  - steimatzky.co.il
  - simania.co.il
- Concurrent Google search lookup.
- Fuzzy matching to verify book details.
- Automatic file renaming (author_bookname.ext) and organization into subdirectories.

## Installation
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage
```bash
python search_ebooks.py --input <dir_or_file>
```
