from pymongo import MongoClient

def filter_documents():
    client = MongoClient("mongodb+srv://doadmin:67K98DEUBAY0T214@lwai-mongo-c557243a.mongo.ondigitalocean.com/stale?authSource=admin&tls=true")

    db = client["judgements"]  # Replace with your database name
    collection = db["allahabad_hc"]
    
    # Query to find documents where "Translated Judgement" is either missing, empty, or "--"
    filter_query = {
        "$or": [
            {"Translated Judgment": {"$exists": False}},
            {"Translated Judgment": ""},
            {"Translated Judgment": "--"},
        ]
    }
    
    filtered_docs = list(collection.find(filter_query))
    
    print(f"Found {len(filtered_docs)} documents without a valid 'Translated Judgement' URL.")
    
    # for doc in filtered_docs:
    #     print(doc)  # Print or process the documents as needed
    
    # return len(filtered_docs)

    # result = collection.delete_many(filter_query)
    
    # print(f"Deleted {result.deleted_count} documents without a valid 'Translated Judgement' URL.")

if __name__ == "__main__":
    filter_documents()