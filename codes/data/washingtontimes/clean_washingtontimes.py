import pandas as pd

# open urls_uncleaned.csv
df = pd.read_csv('urls_uncleaned.csv', header=None, encoding='utf-8')

# positive filtering
df = df[df[1].str.len() >= 50]
# df = df[df[1].str.contains("us")]

# negative filtering
df = df[~df[1].str.contains("/topics/")]
df = df[~df[1].str.contains("/opinion/")]
df = df[~df[1].str.contains("/subscribe/")]
df = df[~df[1].str.contains("/staff/")]
df = df[~df[1].str.contains("/newsletters/")]
df = df[~df[1].str.contains("/sports/")]
df = df[~df[1].str.contains("/directory.washingtontimes")]
df = df[~df[1].str.contains("communities.washingtontimes")]
df = df[~df[1].str.contains("/blog/")]
df = df[~df[1].str.contains("/culture/")]
df = df[~df[1].str.contains("/petitions/")]
df = df[~df[1].str.contains("/communities/")]
df = df[~df[1].str.contains("/specials/")]
df = df[~df[1].str.contains("/higher-ground/")]
df = df[~df[1].str.contains("/foryou/")]
df = df[~df[1].str.contains("/morning-edition/")]
df = df[~df[1].str.contains("/sponsored/")]
df = df[~df[1].str.contains("/special/")]
df = df[~df[1].str.contains("/login/")]




# sorting and duplicate removal
df["length"] = df[1].str.len()
df.sort_values(by="length", ascending=True, inplace=True)
df.drop(columns="length", inplace=True)

# Drop the duplicates
df.drop_duplicates(subset=[1], inplace=True)  # Assuming the second column has index 1

# save the cleaned dataframe to urls_cleaned.csv
df.to_csv('urls_cleaned.csv', header=None, index=False)

