import json
from pathlib import Path

import pandas as pd

row = {
    "Name": "",
    "Vorname": "",
    "Strasse": "",
    "PLZ": "",
    "Ort": "",
    "Geburtsdatum": "",
    "Report": json.dumps(
        {}, ensure_ascii=False, indent=4
    ),  # Initialize with an empty JSON object
}

# Load data
path_to_load = Path("./data/MaskingData_de.csv")

data = pd.read_csv(path_to_load)

# Get the maximum and minimum id from the existing data
id_col = "Unnamed: 0"
max_id = data[id_col].max()
min_id = data[id_col].min()

# Create a new row with the next id
row[id_col] = min_id - 1

# Add all the remaining columns with empty values
for col in data.columns:
    if col not in row:
        row[col] = None

# Sort the keys of the row dictionary to match the order of the DataFrame columns
row = {k: row[k] for k in data.columns if k in row}

# Add one row at the front
data = pd.concat([pd.DataFrame([row]), data], ignore_index=True)

print(data)

# Save the modified data to a new CSV file
path_to_save = path_to_load
data.to_csv(path_to_save, index=False)
