import json

# Input and output file paths
jsonl_file = "merge_sc.jsonl"
eng_file = "train.eng_Latn"
hin_file = "train.hin_Deva"

# Open files for writing
with open(jsonl_file, 'r', encoding='utf-8') as f_in, \
     open(eng_file, 'w', encoding='utf-8') as f_eng, \
     open(hin_file, 'w', encoding='utf-8') as f_hin:
    
    for line in f_in:
        if line.strip():
            # Parse the JSON line
            data = json.loads(line.strip())
            
            # Write to respective files
            f_eng.write(data["english"] + "\n")
            f_hin.write(data["hindi"] + "\n")