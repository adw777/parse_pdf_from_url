import json
import os
import random

# Input file path
jsonl_file = "your_data.jsonl"

# Set the split ratio for dev set (e.g., 0.1 = 10% of data goes to dev)
dev_ratio = 0.1

# Create directory structure
os.makedirs("en-indic-exp/train/eng_Latn-hin_Deva", exist_ok=True)
os.makedirs("en-indic-exp/devtest/all/eng_Latn-hin_Deva", exist_ok=True)

# Output file paths
train_eng = "en-indic-exp/train/eng_Latn-hin_Deva/train.eng_Latn"
train_hin = "en-indic-exp/train/eng_Latn-hin_Deva/train.hin_Deva"
dev_eng = "en-indic-exp/devtest/all/eng_Latn-hin_Deva/dev.eng_Latn"
dev_hin = "en-indic-exp/devtest/all/eng_Latn-hin_Deva/dev.hin_Deva"

# Read all data
data = []
with open(jsonl_file, 'r', encoding='utf-8') as f:
    for line in f:
        if line.strip():
            data.append(json.loads(line.strip()))

# Shuffle data for randomness
random.shuffle(data)

# Split into train and dev
split_idx = int(len(data) * (1 - dev_ratio))
train_data = data[:split_idx]
dev_data = data[split_idx:]

# Write train files
with open(train_eng, 'w', encoding='utf-8') as f_eng, \
     open(train_hin, 'w', encoding='utf-8') as f_hin:
    for item in train_data:
        f_eng.write(item["english"] + "\n")
        f_hin.write(item["hindi"] + "\n")

# Write dev files
with open(dev_eng, 'w', encoding='utf-8') as f_eng, \
     open(dev_hin, 'w', encoding='utf-8') as f_hin:
    for item in dev_data:
        f_eng.write(item["english"] + "\n")
        f_hin.write(item["hindi"] + "\n")

print(f"Created train set with {len(train_data)} examples")
print(f"Created dev set with {len(dev_data)} examples")