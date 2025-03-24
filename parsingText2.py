import os
import re
import logging
import requests
import urllib.request
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor
import fitz
from pdf2image import convert_from_path  
from pytesseract import image_to_string  
from langdetect import detect, LangDetectException
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger("app")
logger.setLevel(logging.INFO)

def extract_tables_from_page(page: fitz.Page) -> List[str]:
    """
    Detects tables on a PyMuPDF page and extracts them as lines of tab-separated cells.
    Returns a list of lines. Each line corresponds to one row in a table.
    """
    results = []
    try:
        tables = page.find_tables()
        for table in tables:
            rows = table.extract()
            for row in rows:
                cells = [(cell if cell else "").strip() for cell in row]
                row_text = "\t".join(cells)
                results.append(row_text)
    except Exception as e:
        logger.error("Error extracting tables from page.", exc_info=True)
    return results

def extract_text_no_tables(page: fitz.Page) -> str:
    """
    Extracts normal (non-table) text from a page using PyMuPDF.
    """
    try:
        return page.get_text("text")
    except Exception as e:
        logger.error("Error extracting text.", exc_info=True)
        return ""

def extract_pdf_with_pymupdf(file_path: str) -> str:
    """
    Extract text & tables from a PDF using PyMuPDF.
    1) For each page, detect tables and extract them.
    2) Also extract the normal text outside tables.
    Returns combined text for the entire PDF.
    """
    if not os.path.isfile(file_path):
        return ""

    try:
        doc = fitz.open(file_path)
    except Exception as e:
        logger.error(f"Error opening PDF '{file_path}' with PyMuPDF.", exc_info=True)
        return ""

    all_text_chunks = []
    for page_index in range(doc.page_count):
        page = doc[page_index]

        table_lines = extract_tables_from_page(page)
        if table_lines:
            all_text_chunks.extend(table_lines)

        normal_text = extract_text_no_tables(page)
        if normal_text.strip():
            all_text_chunks.append(normal_text.strip())

    doc.close()
    return "\n".join(all_text_chunks)

def convert_pdf_to_images(file_path: str, dpi: int = 300,
                         first_page: Optional[int] = None,
                         last_page: Optional[int] = None) -> Optional[List]:
    """
    Converts a PDF to a list of PIL images for OCR.
    """
    try:
        return convert_from_path(file_path, dpi=dpi, thread_count=4,
                               grayscale=True, first_page=first_page,
                               last_page=last_page)
    except Exception as e:
        logger.error("Error converting PDF to images.", exc_info=True)
        return None

def ocr_image(image) -> str:
    """
    Perform OCR on a single PIL image and return extracted text.
    """
    try:
        text = image_to_string(image, lang='eng+hin', config="--psm 1 --oem 1")
        return text if text else ""
    except Exception as e:
        logger.error("Error during OCR on image.", exc_info=True)
        return ""

def extract_pdf_with_ocr(file_path: str, max_pages_per_chunk: int = 50) -> str:
    """
    If PyMuPDF extraction fails or is insufficient, fallback to OCR.
    Processes the PDF in chunks for efficiency.
    """
    if not os.path.isfile(file_path):
        return ""

    try:
        doc = fitz.open(file_path)
        total_pages = doc.page_count
        doc.close()
    except Exception as e:
        logger.error(f"Error opening PDF '{file_path}' for page count.", exc_info=True)
        return ""

    all_text = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        for chunk_start in range(1, total_pages + 1, max_pages_per_chunk):
            chunk_end = min(chunk_start + max_pages_per_chunk - 1, total_pages)
            images = convert_pdf_to_images(file_path, dpi=300,
                                         first_page=chunk_start,
                                         last_page=chunk_end)
            if not images:
                continue
            results = list(executor.map(ocr_image, images))
            all_text.extend(results)

    return "\n".join(all_text)

def remove_unwanted_headers(text: str) -> str:
    """
    Remove lines containing known Gazette headers / disclaimers
    """
    lines = text.split("\n")
    cleaned_lines = []
    for line in lines:
        line_upper = line.upper().strip()

        if "GAZETTE OF INDIA" in line_upper:
            continue
        if "EXTRAORDINARY" in line_upper:
            continue
        if re.match(r'^\s*\d+\]\s*$', line_upper):
            continue

        cleaned_lines.append(line)
    return "\n".join(cleaned_lines)

# def is_english_sentence(sentence: str) -> bool:
#     """
#     Detects if a sentence is English. Returns True if it is.
#     """
#     try:
#         lang = detect(sentence)
#         return (lang == 'en')
#     except LangDetectException:
#         return False

def is_english_or_hindi_sentence(sentence: str) -> bool:
    """
    Detects if a sentence is English or Hindi. Returns True if it is either language.
    """
    try:
        lang = detect(sentence)
        return (lang == 'en' or lang == 'hi')
    except LangDetectException:
        return False

def split_into_sentences(text: str) -> list:
    """
    Simple approach: split on '.' for demonstration.
    """
    return text.split(".")

def clean_and_keep_only_english(text: str, min_length: int = 50) -> str:
    """
    Clean and filter text to keep only English content
    """
    text = remove_unwanted_headers(text)
    sentences = split_into_sentences(text)

    valid_sentences = []
    for s in sentences:
        s = s.strip()
        if not s:
            continue

        if not is_english_or_hindi_sentence(s):
            continue

        s = re.sub(r"[^a-zA-Z0-9\s\.,;:\-\(\)\[\]\'\"\/&\u2022]", "", s)
        s = re.sub(r"\s+", " ", s).strip()

        if len(s) > 0:
            valid_sentences.append(s)

    final_text = ". ".join(valid_sentences).strip()

    if len(final_text) < min_length:
        return ""
    return final_text

def extract_text_from_pdf(file_path: str) -> Optional[str]:
    """
    Extract text from a PDF using PyMuPDF and OCR if needed
    """
    if not os.path.isfile(file_path):
        return None

    native_text = extract_pdf_with_pymupdf(file_path)
    if native_text.strip():
        cleaned = clean_and_keep_only_english(native_text)
        if cleaned:
            return cleaned

    logger.info(f"Falling back to OCR for '{file_path}' ...")
    ocr_text = extract_pdf_with_ocr(file_path)
    if not ocr_text.strip():
        return None

    cleaned_ocr = clean_and_keep_only_english(ocr_text)
    return cleaned_ocr if cleaned_ocr else None

def extract_text_from_pdf_url(url: str) -> Optional[str]:
    """
    Extract text from a PDF URL
    """
    try:
        logger.info(f"Downloading PDF from URL: {url}")
        
        # Browser-like headers
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
        
        # Create a session to handle cookies and redirects
        session = requests.Session()
        
        # First make a HEAD request to check for redirects
        head_response = session.head(url, headers=headers, allow_redirects=True, timeout=30)
        if head_response.status_code == 200:
            final_url = head_response.url
        else:
            final_url = url
            
        logger.info(f"Making GET request to final URL: {final_url}")
        response = session.get(final_url, headers=headers, verify=False, timeout=30)
        response.raise_for_status()
        
        # Check if the response is actually a PDF
        content_type = response.headers.get('Content-Type', '').lower()
        if 'pdf' not in content_type and not final_url.lower().endswith('.pdf'):
            logger.warning(f"Response may not be a PDF. Content-Type: {content_type}")
        
        logger.info("Saving PDF to temporary file")
        with open("temp.pdf", "wb") as f:
            f.write(response.content)
        
        logger.info("Extracting text from downloaded PDF")
        text = extract_text_from_pdf("temp.pdf")
        
        if text is None:
            logger.error("No text could be extracted from the PDF")
        else:
            logger.info(f"Successfully extracted {len(text)} characters of text")
        
        # Clean up temp file
        if os.path.exists("temp.pdf"):
            os.remove("temp.pdf")
            
        return text
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading PDF: {str(e)}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Error processing PDF from URL: {str(e)}", exc_info=True)
        return None

def process_pdf(url_or_path: str) -> Optional[str]:
    """
    Main entry point: Process a PDF file (local or remote) and extract text
    """
    try:
        logger.info(f"Starting to process PDF from: {url_or_path}")
        
        if url_or_path.startswith(('http://', 'https://')):
            logger.info("Detected URL, processing as remote PDF")
            result = extract_text_from_pdf_url(url_or_path)
        else:
            logger.info("Processing as local PDF file")
            result = extract_text_from_pdf(url_or_path)
            
        if result is None:
            logger.error("Failed to extract text from PDF")
        elif not result.strip():
            logger.error("Extracted text is empty")
        else:
            logger.info(f"Successfully extracted {len(result)} characters of text")
            
        return result
    except Exception as e:
        logger.error(f"Error processing PDF from {url_or_path}: {str(e)}", exc_info=True)
        return None

if __name__ == "__main__":
    example_pdf_url = "https://elegalix.allahabadhighcourt.in/elegalix/WebViewAllTranslatedHCJudgment.doWebDownloadOriginalHCJudgmentDocument.do?translatedJudgmentID=17940"
    logging.basicConfig(level=logging.INFO)  # Add this line to see the logs
    
    print("Starting PDF extraction...")
    text = process_pdf(example_pdf_url)
    
    if text:
        print(f"Successfully extracted {len(text)} characters of text")
        with open("output_hin.txt", "w", encoding="utf-8") as f:
            f.write(text)
        print("Extraction complete. See 'output.txt'.")
    else:
        print("No text extracted or file not supported.")