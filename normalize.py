import pandas as pd
import re
import recordlinkage

df = pd.read_csv("vendors.csv")

def normalize(text):
    if pd.isna(text):
        return ""
    text = text.lower()
    text = re.sub(r'[^a-z0-9 ]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

for col in ["vendor_name", "vendor_address", "city", "state"]:
    df[col + "_norm"] = df[col].apply(normalize)

print(df)


indexer = recordlinkage.Index()
indexer.block(["state_norm", "city_norm"])

candidate_pairs = indexer.index(df)

print(f"Candidate pairs: {len(candidate_pairs)}")


