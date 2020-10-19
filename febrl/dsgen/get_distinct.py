import pandas as pd 


df = pd.read_csv("all_manufacturers.csv", header=0, sep=",")
uv = df.manufacturer.unique()

pd.DataFrame(uv).to_csv("all_manufacturers_distinct.csv")