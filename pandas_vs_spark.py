import pandas as pd
import time

# Pandas timing
start = time.time()
df = pd.read_csv("big_data.csv")
result = df.groupby("label").mean()
end = time.time()

print("Pandas Time:", end - start)
print(result)