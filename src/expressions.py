import polars as pl

df_flights = pl.scan_parquet("data/flights.parquet")
heavy_delay = pl.col("dep_delay") > 60
print(heavy_delay)

df_flights_w_flag = df_flights.select(
    "carrier", "flight", "dep_delay", "origin", "dest"
).with_columns(heavy_delay=heavy_delay)

print(df_flights_w_flag.collect())


##Keeping it DRY

dep_delay = pl.col("dep_delay")

df_summary = df_flights.group_by("carrier").agg(
    [
        dep_delay.mean().alias("avg_dep_delay"),
        dep_delay.max().alias("max_dep_delay"),
        dep_delay.sum().alias("total_delay_minutes"),
    ]
)

print(df_summary.collect())