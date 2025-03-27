import os
import logging
import pymongo
from typing import Dict, Optional, Tuple
import requests
import urllib3
from concurrent.futures import ThreadPoolExecutor

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Import our parsing modules
from OCR_parsing_hin import download_pdf, pdf_to_text
from parsingText2 import extract_text_from_pdf, process_pdf

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("extract_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("extract_pipeline")

# MongoDB Configuration
MONGO_URI = "mongodb+srv://doadmin:67K98DEUBAY0T214@lwai-mongo-c557243a.mongo.ondigitalocean.com/stale?authSource=admin&tls=true"
DB_NAME = "judgements"
COLLECTION_NAME = "punjab_haryana_hc"
ENGLISH_URL_FIELD = "Original English Version"
HINDI_URL_FIELD = "Translated Hindi Version"
ENGLISH_CONTENT_FIELD = "Original_Content"
HINDI_CONTENT_FIELD = "Translated_Content"

def download_pdf_temp(url: str) -> str:
    """
    Download PDF from URL to a temporary file
    Returns path to temporary file
    """
    local_path = f"temp_{os.getpid()}.pdf"
    try:
        # Fix malformed URLs if needed
        fixed_url = fix_url(url)
        
        # Use advanced download with headers and session
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/pdf,application/x-pdf,text/html,application/xhtml+xml,*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://elegalix.allahabadhighcourt.in/'
        }
        
        session = requests.Session()
        response = session.get(fixed_url, headers=headers, allow_redirects=True, timeout=30, verify=False)
        response.raise_for_status()
        
        with open(local_path, 'wb') as f:
            f.write(response.content)
            
        # Verify file is not empty
        if os.path.getsize(local_path) < 100:  # If less than 100 bytes, probably not a valid PDF
            logger.warning(f"Downloaded file is suspiciously small ({os.path.getsize(local_path)} bytes)")
            
        return local_path
    except Exception as e:
        logger.error(f"Error downloading PDF from {url}: {str(e)}")
        # Create empty file to prevent future errors
        with open(local_path, 'wb') as f:
            f.write(b'')
        return local_path
        
def fix_url(url: str) -> str:
    """
    Fix common URL formatting issues
    """
    # Fix doubled-up .do segments
    if "doWeb" in url:
        parts = url.split("doWeb")
        if len(parts) > 1:
            url = parts[0] + "do?Web" + parts[1]
    
    # Other fixes as needed
    return url

def extract_english_content(pdf_path: str) -> str:
    """
    Extract English text using PyMuPDF-based parser
    """
    try:
        # Use the text extraction from parsingText2.py
        extracted_text = extract_text_from_pdf(pdf_path)
        return extracted_text or ""
    except Exception as e:
        logger.error(f"Error extracting English text: {str(e)}")
        return ""

def extract_hindi_content(pdf_path: str) -> str:
    """
    Extract Hindi text using OCR with multithreading
    """
    try:
        # Create a temporary output file
        output_path = f"temp_hindi_{os.getpid()}.txt"
        
        # Extract text using OCR with 8 worker threads
        # This directly returns the text content as well as saving it
        text = pdf_to_text(pdf_path, output_path, max_workers=8, lang='hin')
        
        # If empty, try reading from file as backup
        if not text and os.path.exists(output_path):
            with open(output_path, 'r', encoding='utf-8') as f:
                text = f.read()
            
        # Clean up the temporary file
        if os.path.exists(output_path):
            os.remove(output_path)
            
        return text
    except Exception as e:
        logger.error(f"Error extracting Hindi text: {str(e)}")
        return ""

def process_document(doc: Dict) -> Dict:
    """
    Process a single document from MongoDB
    """
    doc_id = doc.get('_id', 'Unknown ID')
    english_url = doc.get(ENGLISH_URL_FIELD)
    hindi_url = doc.get(HINDI_URL_FIELD)
    
    logger.info(f"Processing document {doc_id}")
    
    # Check if content already exists and skip if needed
    english_content = doc.get(ENGLISH_CONTENT_FIELD, "")
    hindi_content = doc.get(HINDI_CONTENT_FIELD, "")
    
    # Process English content if needed
    if not english_content and english_url:
        logger.info(f"Processing English judgment from URL: {english_url}")
        try:
            # Download PDF with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    english_pdf_path = download_pdf_temp(english_url)
                    if os.path.getsize(english_pdf_path) > 1000:  # Reasonable PDF size
                        break
                    logger.warning(f"Attempt {attempt+1}: Downloaded file too small, retrying...")
                    if attempt < max_retries - 1:  # Don't sleep on last attempt
                        import time
                        time.sleep(2)  # Wait between retries
                except Exception as retry_error:
                    logger.warning(f"Attempt {attempt+1} failed: {str(retry_error)}")
                    if attempt < max_retries - 1:
                        import time
                        time.sleep(2)
            
            # Extract text if download succeeded
            if os.path.exists(english_pdf_path) and os.path.getsize(english_pdf_path) > 1000:
                english_content = extract_english_content(english_pdf_path)
                logger.info(f"Extracted {len(english_content)} characters of English text")
            else:
                logger.error("English PDF download failed after retries")
                
            # Clean up
            if os.path.exists(english_pdf_path):
                os.remove(english_pdf_path)
        except Exception as e:
            logger.error(f"Error processing English document: {str(e)}")
    elif english_content:
        logger.info(f"Skipping English extraction - content already exists ({len(english_content)} chars)")
    else:
        logger.warning(f"Document {doc_id} has no English URL field '{ENGLISH_URL_FIELD}'")
    
    # Process Hindi content if needed
    if not hindi_content and hindi_url:
        logger.info(f"Processing Hindi judgment from URL: {hindi_url}")
        try:
            # Download PDF with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    hindi_pdf_path = download_pdf_temp(hindi_url)
                    if os.path.getsize(hindi_pdf_path) > 1000:  # Reasonable PDF size
                        break
                    logger.warning(f"Attempt {attempt+1}: Downloaded file too small, retrying...")
                    if attempt < max_retries - 1:
                        import time
                        time.sleep(2)
                except Exception as retry_error:
                    logger.warning(f"Attempt {attempt+1} failed: {str(retry_error)}")
                    if attempt < max_retries - 1:
                        import time
                        time.sleep(2)
            
            # Extract text if download succeeded
            if os.path.exists(hindi_pdf_path) and os.path.getsize(hindi_pdf_path) > 1000:
                hindi_content = extract_hindi_content(hindi_pdf_path)
                logger.info(f"Extracted {len(hindi_content)} characters of Hindi text")
            else:
                logger.error("Hindi PDF download failed after retries")
                
            # Clean up
            if os.path.exists(hindi_pdf_path):
                os.remove(hindi_pdf_path)
        except Exception as e:
            logger.error(f"Error processing Hindi document: {str(e)}")
    elif hindi_content:
        logger.info(f"Skipping Hindi extraction - content already exists ({len(hindi_content)} chars)")
    else:
        logger.warning(f"Document {doc_id} has no Hindi URL field '{HINDI_URL_FIELD}'")
    
    # Update the document with extracted content
    doc[ENGLISH_CONTENT_FIELD] = english_content
    doc[HINDI_CONTENT_FIELD] = hindi_content
    
    return doc

def connect_to_mongodb():
    """
    Connect to MongoDB and return the collection
    """
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        logger.info(f"Successfully connected to MongoDB database '{DB_NAME}', collection '{COLLECTION_NAME}'")
        return collection
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {str(e)}")
        raise

def main():
    """
    Main function to run the pipeline
    """
    logger.info("Starting judgment text extraction pipeline")
    
    try:
        # Create pdf_images directory for OCR
        os.makedirs('pdf_images', exist_ok=True)
        
        # Connect to MongoDB
        collection = connect_to_mongodb()
        
        # Find documents with URLs but no extracted content
        query = {
            "$and": [
                # Document needs to have at least one URL
                {
                    "$or": [
                        {ENGLISH_URL_FIELD: {"$exists": True, "$ne": ""}},
                        {HINDI_URL_FIELD: {"$exists": True, "$ne": ""}}
                    ]
                },
                # And needs to be missing at least one content field
                {
                    "$or": [
                        # Skip if English content already exists
                        {
                            ENGLISH_URL_FIELD: {"$exists": True, "$ne": ""},
                            "$or": [
                                {ENGLISH_CONTENT_FIELD: {"$exists": False}},
                                {ENGLISH_CONTENT_FIELD: ""}
                            ]
                        },
                        # Skip if Hindi content already exists
                        {
                            HINDI_URL_FIELD: {"$exists": True, "$ne": ""},
                            "$or": [
                                {HINDI_CONTENT_FIELD: {"$exists": False}},
                                {HINDI_CONTENT_FIELD: ""}
                            ]
                        }
                    ]
                }
            ]
        }
        
        # Count documents to process
        total_docs = collection.count_documents(query)
        logger.info(f"Found {total_docs} documents to process")
        
        # Process each document
        for i, doc in enumerate(collection.find(query)):
            logger.info(f"Processing document {i+1}/{total_docs}")
            
            # Skip docs where both contents already exist
            has_eng = doc.get(ENGLISH_CONTENT_FIELD, "") != ""
            has_hin = doc.get(HINDI_CONTENT_FIELD, "") != ""
            
            if has_eng and has_hin:
                logger.info(f"Skipping document {doc['_id']} - both contents already exist")
                continue
                
            # Process the document
            updated_doc = process_document(doc)
            
            # Only update if content was extracted
            eng_content = updated_doc.get(ENGLISH_CONTENT_FIELD, "")
            hin_content = updated_doc.get(HINDI_CONTENT_FIELD, "")
            
            update_fields = {}
            if eng_content:
                update_fields[ENGLISH_CONTENT_FIELD] = eng_content
            if hin_content:
                update_fields[HINDI_CONTENT_FIELD] = hin_content
                
            if update_fields:
                # Update in MongoDB
                collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": update_fields}
                )
                logger.info(f"Updated document {doc['_id']} with {', '.join(update_fields.keys())}")
            else:
                logger.warning(f"No content extracted for document {doc['_id']}")
            
            # Clean up temporary files after each document
            clean_temp_files()
            
        logger.info("Judgment text extraction pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Error in judgment text extraction pipeline: {str(e)}")
    finally:
        # Final cleanup of all temporary files
        clean_temp_files()
        
def clean_temp_files():
    """
    Clean up all temporary files created during processing
    """
    # Clean up PDF and text files
    for filename in os.listdir('.'):
        if filename.startswith('temp_') and (filename.endswith('.pdf') or filename.endswith('.txt')):
            try:
                os.remove(filename)
                logger.debug(f"Cleaned up temporary file: {filename}")
            except Exception as e:
                logger.error(f"Error removing temporary file {filename}: {str(e)}")
                
    # Clean up OCR image directory
    if os.path.exists('pdf_images'):
        for filename in os.listdir('pdf_images'):
            if filename.startswith('page_'):
                try:
                    os.remove(os.path.join('pdf_images', filename))
                    logger.debug(f"Cleaned up temporary image: {filename}")
                except Exception as e:
                    logger.error(f"Error removing temporary image {filename}: {str(e)}")

if __name__ == "__main__":
    main()


