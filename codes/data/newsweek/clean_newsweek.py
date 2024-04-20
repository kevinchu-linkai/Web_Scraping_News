import pandas as pd

# open urls_uncleaned.csv
df = pd.read_csv('urls_uncleaned.csv', header=None, encoding='utf-8')

# positive filtering
df = df[df[1].str.len() >= 40]

# negative filtering
df = df[~df[1].str.contains("/authors/")]
df = df[~df[1].str.contains("/topic/")]
df = df[~df[1].str.contains("web.archive.org")]
df = df[~df[1].str.contains("//subscribe")]
df = df[~df[1].str.contains("about-newsweek")]
df = df[~df[1].str.contains("/issue.html")]
df = df[~df[1].str.contains("//subscription")]
df = df[~df[1].str.contains("/newsletter/")]

# sorting and duplicate removal
df["length"] = df[1].str.len()
df.sort_values(by="length", ascending=True, inplace=True)
df.drop(columns="length", inplace=True)

# Drop the duplicates
df.drop_duplicates(subset=[1], inplace=True)  # Assuming the second column has index 1

# save the cleaned dataframe to urls_cleaned.csv
df.to_csv('urls_cleaned.csv', header=None, index=False)

