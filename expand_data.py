import pandas as pd

df = pd.read_csv("clean_data.csv")

# Repeat data 100 times
big_df = pd.concat([df]*100, ignore_index=True)

big_df.to_csv("big_data.csv", index=False)

print("Large dataset created:", len(big_df))