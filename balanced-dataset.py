import pandas as pd
import csv

# Load the original dataset
df = pd.read_csv(
    "dataset/training.1600000.processed.noemoticon.csv",
    encoding='latin-1',
    header=None
)

# Assign column names for clarity
df.columns = ['target', 'id', 'date', 'flag', 'user', 'text']

# Randomly sample 100,000 rows
sample_df = df.sample(n=100000, random_state=42)

# Save with original quoting style
sample_df.to_csv(
    "sampled_100k_sentiment140.csv",
    index=False,
    encoding='utf-8',
    quoting=csv.QUOTE_ALL
)
