import polars as pl

df_flights = pl.read_parquet("data/flights.parquet")
df_flights = (df_flights.filter(pl.col("dep_delay")>60)
              .select("carrier","flight","dep_delay","origin","dest"))
df_agg = df_flights.group_by("carrier").agg(pl.col("dep_delay").mean())


df_flights = pl.scan_parquet("data/flights.parquet")

df_flights = (df_flights.filter(pl.col("dep_delay")>60)
              .select("carrier","flight","dep_delay","origin","dest"))

df_agg = df_flights.group_by("carrier").agg(pl.col("dep_delay").mean())
print(df_agg.collect())

#query plan
print(df_agg.explain())