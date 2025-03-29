import json
import argparse

def merge_json_files(file1, file2, output_file):
    with open(file1, 'r', encoding='utf-8') as f1, open(file2, 'r', encoding='utf-8') as f2:
        data1 = json.load(f1)
        data2 = json.load(f2)
    
    merged_data = data1 + data2 if isinstance(data1, list) and isinstance(data2, list) else {**data1, **data2}
    
    with open(output_file, 'w', encoding='utf-8') as out:
        json.dump(merged_data, out, indent=4, ensure_ascii=False)
    print(f"Merged JSON saved to {output_file}")

def merge_jsonl_files(file1, file2, output_file):
    with open(output_file, 'w', encoding='utf-8') as out:
        for file in [file1, file2]:
            with open(file, 'r', encoding='utf-8') as f:
                out.writelines(f.readlines())
    print(f"Merged JSONL saved to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge two JSON or JSONL files.")
    parser.add_argument("file1", help="Path to the first file")
    parser.add_argument("file2", help="Path to the second file")
    parser.add_argument("output", help="Path to the output file")
    parser.add_argument("--type", choices=["json", "jsonl"], required=True, help="Type of files to merge")
    
    args = parser.parse_args()
    
    if args.type == "json":
        merge_json_files(args.file1, args.file2, args.output)
    elif args.type == "jsonl":
        merge_jsonl_files(args.file1, args.file2, args.output)


# python .py file1.json file2.json output.json --type json
# python merge_script.py file1.jsonl file2.jsonl output.jsonl --type jsonl