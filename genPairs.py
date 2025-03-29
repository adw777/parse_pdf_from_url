import re
import json
import os
from pymongo import MongoClient

def extract_aligned_pairs(text_content, threshold=0.6):
    """
    Extract English-Hindi aligned pairs with scores above the threshold.
    
    Args:
        text_content (str): Aligned text content
        threshold (float): Minimum alignment score to include a pair
        
    Returns:
        dict: Dictionary with English text as keys and Hindi text as values
    """
    try:
        # Regular expression to extract English, Hindi, and score triplets
        pattern = r'([^\t]+)\t([^\t]+)\t([0-9\.]+)'
        matches = re.findall(pattern, text_content)
        
        # Filter pairs with score > threshold and create dictionary
        pairs = {}
        for eng, hindi, score in matches:
            try:
                score_val = float(score)
                if score_val > threshold:
                    # Clean up the texts (remove extra whitespace)
                    eng_clean = eng.strip()
                    hindi_clean = hindi.strip()
                    if eng_clean and hindi_clean:  # Ensure neither is empty
                        pairs[eng_clean] = hindi_clean
            except ValueError:
                # Skip if score is not a valid float
                continue
        
        return pairs
    
    except Exception as e:
        print(f"Error processing content: {e}")
        return {}

def save_to_json(pairs, file_name, output_dir="PAIRS0.6punjab_harayana_hc463"):
    """
    Save the extracted pairs to a JSON file.
    
    Args:
        pairs (dict): Dictionary of English-Hindi pairs
        file_name (str): Base name for the JSON file
        output_dir (str): Directory to save JSON files
    
    Returns:
        str: Path to the saved file
    """
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Ensure the file has .json extension
        if not file_name.endswith('.json'):
            file_name += '.json'
        
        output_path = os.path.join(output_dir, file_name)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(pairs, f, ensure_ascii=False, indent=2)
        
        print(f"Successfully saved {len(pairs)} pairs to {output_path}")
        return output_path
    
    except Exception as e:
        print(f"Error saving JSON file: {e}")
        return None

def main():
    # MongoDB connection parameters
    mongo_uri = "mongodb+srv://doadmin:67K98DEUBAY0T214@lwai-mongo-c557243a.mongo.ondigitalocean.com/stale?authSource=admin&tls=true"
    database_name = "judgements"  # Update with your database name
    collection_name = "punjab_haryana_hc463"
    
    # Connect to MongoDB
    try:
        client = MongoClient(mongo_uri)
        db = client[database_name]
        collection = db[collection_name]
        
        print(f"Connected to MongoDB collection: {collection_name}")
        
        # Get all documents that have the sent_aligned field
        cursor = collection.find({"sent_aligned": {"$exists": True}})
        total_docs = 0
        processed_docs = 0
        
        for doc in cursor:
            total_docs += 1
            doc_id = doc.get('_id')
            file_name = doc.get('file_name', str(doc_id))
            aligned_text = doc.get('sent_aligned')
            
            if not aligned_text:
                print(f"Document {doc_id} has empty 'sent_aligned' field, skipping...")
                continue
            
            print(f"Processing document: {doc_id} (file: {file_name})")
            
            # Extract English-Hindi pairs
            threshold = 0.6
            pairs = extract_aligned_pairs(aligned_text, threshold)
            
            if not pairs:
                print(f"No valid pairs found in document {doc_id}")
                continue
            
            # Save as JSON file locally
            save_to_json(pairs, file_name)
            
            # Update the document with the extracted pairs
            collection.update_one(
                {"_id": doc_id},
                {"$set": {"ex_pairs": pairs}}
            )
            
            print(f"Updated MongoDB document {doc_id} with {len(pairs)} pairs")
            processed_docs += 1
        
        print(f"Processing complete: {processed_docs} out of {total_docs} documents processed")
        
    except Exception as e:
        print(f"MongoDB operation error: {e}")
    finally:
        if 'client' in locals():
            client.close()
            print("MongoDB connection closed")

if __name__ == "__main__":
    main()