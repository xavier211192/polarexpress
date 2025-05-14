import polars as pl

df = pl.DataFrame(
    {
        "name":["Adam","Joshua","Moses","Jonah"],
        "age":[55,500,1000,100]
    }
)
print(df)


df_flights = pl.read_parquet("data/flights.parquet")

print(df_flights.limit(10))