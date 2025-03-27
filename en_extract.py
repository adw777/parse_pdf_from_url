import os
import logging
import pymongo
from typing import Dict, Optional
import requests
import urllib3
import time

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Import OCR module
from OCR_parsing_eng import download_pdf, pdf_to_text

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
COLLECTION_NAME = "sc2"
URL_FIELD = "Original Judgment"  # Field containing the English judgment URLs
CONTENT_FIELD = "Original_Content"  # Field to update with extracted content

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

def process_document(doc: Dict) -> Dict:
    """
    Process a single document from MongoDB
    """
    doc_id = doc.get('_id', 'Unknown ID')
    url = doc.get(URL_FIELD)
    
    logger.info(f"Processing document {doc_id}")
    
    # We're updating all documents with an "Original Judgement" URL, even if they already have content
    existing_content = doc.get(CONTENT_FIELD, "")
    if existing_content:
        logger.info(f"Document {doc_id} has existing content ({len(existing_content)} chars) - will update anyway")
    
    # Process document if URL exists
    if not url:
        logger.warning(f"Document {doc_id} has no URL field '{URL_FIELD}'")
        return doc
        
    logger.info(f"Processing judgment from URL: {url}")
    
    # Create temp files with unique process ID
    pdf_path = f"temp_{os.getpid()}.pdf"
    txt_path = f"temp_{os.getpid()}.txt"
    
    try:
        # Download PDF with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Use the function from OCR_parsing_eng.py
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
        content = ""
        if os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 1000:
            # Use OCR to extract text (use 'eng' language for English documents)
            content = pdf_to_text(pdf_path, txt_path, max_workers=8, lang='eng')
            logger.info(f"Extracted {len(content)} characters of text")
        else:
            logger.error(f"PDF download failed after {max_retries} retries")
            
    except Exception as e:
        logger.error(f"Error processing document {doc_id}: {str(e)}")
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
    
    # Update the document with extracted content
    if content:
        doc[CONTENT_FIELD] = content
    
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
            # Find all documents with URLs, regardless of whether they have content or not
            query = {
                URL_FIELD: {"$exists": True, "$ne": ""}
            }
            
            # Count documents to process
            total_docs = collection.count_documents(query)
            logger.info(f"Found {total_docs} documents to process")
            
            # Process each document
            for i, doc in enumerate(collection.find(query)):
                logger.info(f"Processing document {i+1}/{total_docs} - ID: {doc.get('_id', 'Unknown')}")
                
                # Process the document
                updated_doc = process_document(doc)
                
                # Only update if content was extracted
                content = updated_doc.get(CONTENT_FIELD, "")
                if content:
                    # Update in MongoDB
                    collection.update_one(
                        {"_id": doc["_id"]},
                        {"$set": {CONTENT_FIELD: content}}
                    )
                    logger.info(f"Updated document {doc['_id']} with {len(content)} chars of content")
                else:
                    logger.warning(f"No content extracted for document {doc['_id']}")
                
                # Clean up temporary files after each document
                clean_temp_files()
                
            logger.info("Judgment text extraction pipeline completed successfully")
        
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