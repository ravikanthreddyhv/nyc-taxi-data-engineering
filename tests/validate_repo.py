import json
import glob
import sys

def validate_json(path):
    try:
        with open(path) as f:
            json.load(f)
        print(f"OK JSON: {path}")
    except Exception as e:
        print(f"FAIL JSON: {path} -> {e}")
        sys.exit(1)

# Governance configs
for f in glob.glob("governance/*.json"):
    validate_json(f)

# Step Functions
validate_json("step_functions/Tests.json")

print("All JSON files validated")
