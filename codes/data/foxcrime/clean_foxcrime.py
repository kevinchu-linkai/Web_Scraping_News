import pandas as pd

# open urls_uncleaned.csv
df = pd.read_csv('urls_uncleaned.csv', header=None, encoding='utf-8')

# positive filtering
df = df[df[1].str.len() >= 10]
df = df[df[1].str.contains("us")]

# negative filtering
df = df[~df[1].str.contains("/video.")]
df = df[~df[1].str.contains("/latino.")]
df = df[~df[1].str.contains("/sports/")]
df = df[~df[1].str.contains("/weather/")]
df = df[~df[1].str.contains("/lifestyle/")]
df = df[~df[1].str.contains("/travel/")]
df = df[~df[1].str.contains("/health/")]
df = df[~df[1].str.contains("/entertainment/")]

# sorting and duplicate removal
df["length"] = df[1].str.len()
df.sort_values(by="length", ascending=True, inplace=True)
df.drop(columns="length", inplace=True)

# Drop the duplicates
df.drop_duplicates(subset=[1], inplace=True)  # Assuming the second column has index 1

# save the cleaned dataframe to urls_cleaned.csv
df.to_csv('urls_cleaned.csv', header=None, index=False)

