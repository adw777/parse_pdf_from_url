# import os
# import json

# def join_json_files(input_dir="PAIRS", output_json="complete_data.json", output_jsonl="complete_data.jsonl"):
#     """
#     Join all JSON files in the specified directory into a single JSON and JSONL file.
    
#     Args:
#         input_dir (str): Directory containing JSON files
#         output_json (str): Name of the output JSON file
#         output_jsonl (str): Name of the output JSONL file
#     """
#     combined_data = []

#     # Iterate through all files in the input directory
#     for filename in os.listdir(input_dir):
#         if filename.endswith('.json'):
#             file_path = os.path.join(input_dir, filename)
#             with open(file_path, 'r', encoding='utf-8') as f:
#                 try:
#                     data = json.load(f)
#                     combined_data.append(data)
#                 except json.JSONDecodeError:
#                     print(f"Error decoding JSON from file: {file_path}")

#     # Save combined data to a JSON file
#     with open(output_json, 'w', encoding='utf-8') as f:
#         json.dump(combined_data, f, ensure_ascii=False, indent=2)
    
#     print(f"Successfully saved combined data to {output_json}")

#     # Save combined data to a JSONL file
#     with open(output_jsonl, 'w', encoding='utf-8') as f:
#         for entry in combined_data:
#             f.write(json.dumps(entry, ensure_ascii=False) + '\n')
    
#     print(f"Successfully saved combined data to {output_jsonl}")

# if __name__ == "__main__":
#     join_json_files()


import os
import json
import glob

def merge_json_files(input_dir, output_json_path, output_jsonl_path):
    """
    Merge multiple JSON files from a directory into a single unified JSON object
    (combining all key-value pairs) and create a JSONL file from the merged content.
    
    Args:
        input_dir (str): Directory containing JSON files to merge
        output_json_path (str): Path to save the merged JSON file
        output_jsonl_path (str): Path to save the JSONL file
    """
    try:
        # Ensure input directory exists
        if not os.path.isdir(input_dir):
            print(f"Error: Directory '{input_dir}' does not exist")
            return
        
        # Find all JSON files in the input directory
        json_files = glob.glob(os.path.join(input_dir, "*.json"))
        if not json_files:
            print(f"No JSON files found in '{input_dir}'")
            return
        
        print(f"Found {len(json_files)} JSON files in {input_dir}")
        
        # Merged data - a single unified dictionary for all files
        merged_data = {}
        total_pairs = 0
        
        # Process each JSON file and merge all key-value pairs
        for json_path in json_files:
            file_name = os.path.basename(json_path)
            print(f"Processing: {file_name}")
            
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    file_data = json.load(f)
                    
                    # Add all key-value pairs from this file to the merged data
                    file_pairs = 0
                    for key, value in file_data.items():
                        merged_data[key] = value
                        file_pairs += 1
                    
                    total_pairs += file_pairs
                    print(f"  Added {file_pairs} pairs from {file_name}")
                    
            except Exception as e:
                print(f"Error processing {file_name}: {e}")
                continue
        
        # Write the merged data to the output JSON file
        with open(output_json_path, 'w', encoding='utf-8') as f:
            json.dump(merged_data, f, ensure_ascii=False, indent=2)
        
        # Create JSONL file from the merged data (one entry per line)
        with open(output_jsonl_path, 'w', encoding='utf-8') as jsonl_file:
            for key, value in merged_data.items():
                # Each line is a JSON object with "english" and "hindi" keys
                entry = {"english": key, "hindi": value}
                jsonl_file.write(json.dumps(entry, ensure_ascii=False) + '\n')
        
        print(f"Successfully merged {len(json_files)} JSON files with {total_pairs} total pairs:")
        print(f"- Combined JSON saved to: {output_json_path}")
        print(f"- JSONL file saved to: {output_jsonl_path}")
        
    except Exception as e:
        print(f"Error merging JSON files: {e}")

def main():
    # Configuration
    input_directory = "PAIRS0.6sclast69"  # Directory containing JSON files
    output_json = "merged_pairssclast69.json"  # Output merged JSON file
    output_jsonl = "pairssclast69.jsonl"  # Output JSONL file
    
    print(f"Starting to merge JSON files from '{input_directory}'...")
    merge_json_files(input_directory, output_json, output_jsonl)

if __name__ == "__main__":
    main()