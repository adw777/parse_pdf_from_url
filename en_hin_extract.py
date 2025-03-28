import os
import logging
import pymongo
from typing import Dict, Optional
import requests
import urllib3
import time

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Import OCR modules for English and Hindi
from OCR_parsing_eng import download_pdf, pdf_to_text as pdf_to_text_eng
from OCR_parsing_hin import pdf_to_text as pdf_to_text_hin

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
COLLECTION_NAME = "sclast69"

# Field configuration
ENG_URL_FIELD = "Original English Version"  # Field containing English URLs
HIN_URL_FIELD = "Translated Hindi Version"  # Field containing Hindi URLs
ENG_CONTENT_FIELD = "Original_Content"  # Field to store English content
HIN_CONTENT_FIELD = "Translated_Content"  # Field to store Hindi content

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

def extract_content(url: str, lang: str) -> str:
    """
    Extract content from a URL based on language
    """
    # Create temp files with unique process ID
    pid = os.getpid()
    pdf_path = f"temp_{pid}_{lang}.pdf"
    txt_path = f"temp_{pid}_{lang}.txt"
    content = ""
    
    try:
        # Download PDF with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Use the download function from OCR modules
                download_pdf(fix_url(url), pdf_path)
                
                # Verify file is valid
                if os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 1000:  # Reasonable PDF size
                    break
                logger.warning(f"Attempt {attempt+1}: Downloaded file too small, retrying...")
                if attempt < max_retries - 1:
                    time.sleep(2)  # Wait between retries
            except Exception as retry_error:
                logger.warning(f"Attempt {attempt+1} failed: {str(retry_error)}")
                if attempt < max_retries - 1:
                    time.sleep(2)
        
        # Extract text if download succeeded
        if os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 1000:
            # Use appropriate OCR function based on language
            if lang == 'eng':
                content = pdf_to_text_eng(pdf_path, txt_path, max_workers=8, lang='eng')
            elif lang == 'hin':
                content = pdf_to_text_hin(pdf_path, txt_path, max_workers=8, lang='hin')
            
            logger.info(f"Extracted {len(content)} characters of {lang} text")
        else:
            logger.error(f"PDF download failed after {max_retries} retries")
            
    except Exception as e:
        logger.error(f"Error processing {lang} document: {str(e)}")
        content = ""
    finally:
        # Clean up temp files
        if os.path.exists(pdf_path):
            try:
                os.remove(pdf_path)
            except Exception as e:
                logger.warning(f"Failed to remove temp PDF: {str(e)}")
                
        if os.path.exists(txt_path):
            try:
                os.remove(txt_path)
            except Exception as e:
                logger.warning(f"Failed to remove temp TXT: {str(e)}")
    
    return content

def process_document(doc: Dict) -> Dict:
    """
    Process a single document from MongoDB
    """
    doc_id = doc.get('_id', 'Unknown ID')
    eng_url = doc.get(ENG_URL_FIELD)
    hin_url = doc.get(HIN_URL_FIELD)
    
    logger.info(f"Processing document {doc_id}")
    
    # Check if both URLs exist
    if not eng_url or not hin_url:
        logger.warning(f"Document {doc_id} missing required URL fields. English URL: {bool(eng_url)}, Hindi URL: {bool(hin_url)}")
        return doc
    
    # Check if content already exists in both fields - skip if already processed
    eng_content_exists = ENG_CONTENT_FIELD in doc and doc[ENG_CONTENT_FIELD]
    hin_content_exists = HIN_CONTENT_FIELD in doc and doc[HIN_CONTENT_FIELD]
    
    if eng_content_exists and hin_content_exists:
        logger.info(f"Document {doc_id} already has both content fields - skipping")
        return doc
        
    logger.info(f"Processing judgment with English URL: {eng_url}, Hindi URL: {hin_url}")
    
    # Extract English content if needed
    eng_content = ""
    if not eng_content_exists:
        logger.info(f"Extracting English content for document {doc_id}")
        eng_content = extract_content(eng_url, 'eng')
        if eng_content:
            doc[ENG_CONTENT_FIELD] = eng_content
            logger.info(f"Extracted {len(eng_content)} chars of English content")
        else:
            logger.warning(f"Failed to extract English content for document {doc_id}")
    
    # Extract Hindi content if needed
    hin_content = ""
    if not hin_content_exists:
        logger.info(f"Extracting Hindi content for document {doc_id}")
        hin_content = extract_content(hin_url, 'hin')
        if hin_content:
            doc[HIN_CONTENT_FIELD] = hin_content
            logger.info(f"Extracted {len(hin_content)} chars of Hindi content")
        else:
            logger.warning(f"Failed to extract Hindi content for document {doc_id}")
    
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
        return client, collection
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {str(e)}")
        raise

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

def main():
    """
    Main function to run the pipeline
    """
    logger.info("Starting judgment text extraction pipeline")
    
    try:
        # Create pdf_images directory for OCR
        os.makedirs('pdf_images', exist_ok=True)
        
        # Connect to MongoDB
        client, collection = connect_to_mongodb()
        
        try:
            # Find documents that have both English and Hindi URLs
            query = {
                "$and": [
                    {ENG_URL_FIELD: {"$exists": True, "$ne": ""}},
                    {HIN_URL_FIELD: {"$exists": True, "$ne": ""}}
                ]
            }
            
            # Count documents to process
            total_docs = collection.count_documents(query)
            logger.info(f"Found {total_docs} documents with both English and Hindi URLs")
            
            # Process each document
            processed_count = 0
            skipped_count = 0
            
            for i, doc in enumerate(collection.find(query)):
                logger.info(f"Processing document {i+1}/{total_docs} - ID: {doc.get('_id', 'Unknown')}")
                
                # Check if document already has both content fields
                eng_content_exists = ENG_CONTENT_FIELD in doc and doc[ENG_CONTENT_FIELD]
                hin_content_exists = HIN_CONTENT_FIELD in doc and doc[HIN_CONTENT_FIELD]
                
                if eng_content_exists and hin_content_exists:
                    logger.info(f"Skipping document {doc['_id']} - both content fields already exist")
                    skipped_count += 1
                    continue
                
                # Process the document
                updated_doc = process_document(doc)
                
                # Check if any content was extracted and needs to be updated
                eng_content = updated_doc.get(ENG_CONTENT_FIELD)
                hin_content = updated_doc.get(HIN_CONTENT_FIELD)
                
                update_fields = {}
                if eng_content and not eng_content_exists:
                    update_fields[ENG_CONTENT_FIELD] = eng_content
                
                if hin_content and not hin_content_exists:
                    update_fields[HIN_CONTENT_FIELD] = hin_content
                
                # Update in MongoDB if needed
                if update_fields:
                    collection.update_one(
                        {"_id": doc["_id"]},
                        {"$set": update_fields}
                    )
                    logger.info(f"Updated document {doc['_id']} with new content fields")
                    processed_count += 1
                else:
                    logger.warning(f"No new content extracted for document {doc['_id']}")
                
                # Clean up temporary files after each document
                clean_temp_files()
                
            logger.info(f"Judgment text extraction pipeline completed. Processed: {processed_count}, Skipped: {skipped_count}")
        
        finally:
            # Close MongoDB connection
            client.close()
            logger.info("MongoDB connection closed")
            
    except Exception as e:
        logger.error(f"Error in judgment text extraction pipeline: {str(e)}")
    finally:
        # Final cleanup of all temporary files
        clean_temp_files()

if __name__ == "__main__":
    main()