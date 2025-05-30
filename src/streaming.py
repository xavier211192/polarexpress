import polars as pl

q = (
    pl.scan_parquet("data/big_flights.parquet")
    .filter(pl.col("dep_delay") > 0)
    .group_by("carrier")
    .agg(pl.len().alias("delayed_flights"))
)


result = q.collect(engine="streaming")
#print the number of cpus
# print(pl.threadpool_size())
print(q.explain(engine="streaming"))
print(result)