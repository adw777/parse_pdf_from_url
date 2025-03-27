import requests
import pytesseract
from pdf2image import convert_from_path
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

# Set up logging
logger = logging.getLogger("ocr_parsing")

# Step 1: Download the PDF from the URL
def download_pdf(url, output_path):
    # Advanced request with headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/pdf,application/x-pdf,*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Referer': 'https://elegalix.allahabadhighcourt.in/'
    }
    
    # Create session for cookies and redirects
    session = requests.Session()
    response = session.get(url, headers=headers, verify=False, timeout=30)
    response.raise_for_status()  # Ensure the request was successful
    
    with open(output_path, 'wb') as f:
        f.write(response.content)

# Process a single image with OCR
def process_image(args):
    image, lang, index = args
    try:
        # Create a unique temporary image path
        pid = os.getpid()
        image_path = os.path.join('pdf_images', f'page_{pid}_{index}.png')
        
        # Save the image
        image.save(image_path, 'PNG')
        
        # Perform OCR
        text = pytesseract.image_to_string(image_path, lang=lang)
        
        # Clean up temporary file
        try:
            os.remove(image_path)
        except Exception as e:
            logger.warning(f"Failed to remove temporary image {image_path}: {str(e)}")
            
        return text
    except Exception as e:
        logger.error(f"Error processing image {index}: {str(e)}")
        return ""

# Step 2: Convert PDF pages to images and extract text with multithreading
def pdf_to_text(pdf_path, output_txt_path, max_workers=4, lang='hin'):
    """
    Extract text from PDF using OCR with multithreading
    
    Args:
        pdf_path: Path to the PDF file
        output_txt_path: Path to save the extracted text
        max_workers: Number of worker threads for OCR
        lang: Language code for OCR (default: 'hin')
    """
    try:
        # Ensure the Tesseract executable path is correctly set
        # Uncomment this line for Windows
        pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
        
        # Directory to save images
        image_dir = 'pdf_images'
        os.makedirs(image_dir, exist_ok=True)
        
        # Convert PDF to images
        logger.info(f"Converting PDF to images: {pdf_path}")
        pages = convert_from_path(pdf_path, dpi=300)
        logger.info(f"PDF converted to {len(pages)} images")
        
        # Prepare arguments for parallel processing
        process_args = [(page, lang, i) for i, page in enumerate(pages)]
        
        # Extract text from each page in parallel
        extracted_text = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_image, arg) for arg in process_args]
            
            for future in as_completed(futures):
                try:
                    text = future.result()
                    if text and text.strip():
                        extracted_text.append(text)
                except Exception as e:
                    logger.error(f"Error processing page: {str(e)}")
        
        # Save the extracted text to a .txt file
        logger.info(f"Writing extracted text to {output_txt_path}")
        with open(output_txt_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(extracted_text))
            
        logger.info(f"OCR completed: {len(extracted_text)} pages processed")
        
        # Clean up any remaining images in the directory
        for filename in os.listdir(image_dir):
            if filename.startswith('page_'):
                try:
                    os.remove(os.path.join(image_dir, filename))
                except Exception as e:
                    logger.warning(f"Failed to remove temporary image {filename}: {str(e)}")
        
        return '\n'.join(extracted_text)  # Return the extracted text
        
    except Exception as e:
        logger.error(f"Error in pdf_to_text: {str(e)}")
        return ""

# Main function to orchestrate the process
def main(pdf_url):
    pdf_path = 'downloaded_pdf.pdf'
    output_txt_path = 'extracted_text.txt'

    # Download the PDF
    download_pdf(pdf_url, pdf_path)

    # Extract text from the downloaded PDF
    pdf_to_text(pdf_path, output_txt_path)

    print(f'Text extracted and saved to {output_txt_path}')

# Example usage
if __name__ == '__main__':
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    pdf_url = 'https://elegalix.allahabadhighcourt.in/elegalix/WebDownloadTranslatedSCJudgmentDocument.do?SCJudgmentID=356'
    main(pdf_url)