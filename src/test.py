import pandas as pd

data = {
    "date": ["2000-01-01", "2000-01-02", "2000-01-03", "2000-01-04"],
    "gdp_growth": [None, 9, None, 7],
}
df = pd.DataFrame(data)
df["date"] = pd.to_datetime(df["date"])

df["gdp_growth"] = df["gdp_growth"].interpolate(method="linear")
print(df)
