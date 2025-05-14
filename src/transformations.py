import polars as pl

df_eager = pl.read_parquet("data/flights.parquet")

#pivot
df_pivot = (
    df_eager.group_by(["dest", "month"])
      .agg(pl.col("dep_delay").mean().alias("avg_delay"))
      .pivot(
          values="avg_delay",
          index="dest",
          columns="month"
      )
)
print(df_pivot)

df_lazy = pl.scan_parquet("data/flights.parquet")

#rolling averages
df = df_lazy.sort(["carrier", "day"])  # Ensure proper order

df = df.with_columns([
    pl.col("dep_delay")
      .rolling_mean(window_size=3)
      .over("carrier")
      .alias("rolling_avg_delay")
])

print(df.collect())

#cumulative metrics
df_ct = df_lazy.with_columns([
    pl.col("dep_delay").cum_sum().over("carrier").alias("cumulative_delay")
])

print(df_ct.collect())

#windowing
df_with_rank = df_lazy.with_columns([
    pl.col("dep_delay")
      .rank("dense")
      .over("carrier")
      .alias("delay_rank_within_carrier")
])

print(df_with_rank.collect())


#Lazy pipeline
q = (
    pl.scan_parquet("data/flights.parquet")
    .filter(pl.col("dep_delay").is_not_null())
    .with_columns([
        (pl.col("origin") + "->" + pl.col("dest")).alias("route")
    ])
    .group_by("route")
    .agg([
        pl.col("dep_delay").mean().alias("avg_dep_delay")
    ])
)

print(q.collect())