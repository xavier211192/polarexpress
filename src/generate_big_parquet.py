import polars as pl

# Read the original small Parquet file
df = pl.read_parquet("data/flights.parquet")

# Repeat the data 1000 times
bigger_df = pl.concat([df] * 1000)

# Save to a new Parquet file
bigger_df.write_parquet("data/big_flights.parquet")
