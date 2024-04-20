import pandas as pd

# open urls_uncleaned.csv
df = pd.read_csv('urls_uncleaned.csv', header=None, encoding='utf-8')

# positive filtering
df = df[df[1].str.len() >= 10]
# df = df[df[1].str.contains("us")]

# negative filtering
df = df[~df[1].str.contains("/author/")]
df = df[~df[1].str.contains("//twitter")]
df = df[~df[1].str.contains("web.archive.org")]
df = df[~df[1].str.contains("/email-protection")]
df = df[~df[1].str.contains("/galleries/")]
df = df[~df[1].str.contains("/videos/")]
df = df[~df[1].str.contains("/franchise/")]
df = df[~df[1].str.contains("/contributors/")]
df = df[~df[1].str.contains("/category/")]
df = df[~df[1].str.contains("coupons")]
df = df[~df[1].str.contains("subscribe.html")]


# sorting and duplicate removal
df["length"] = df[1].str.len()
df.sort_values(by="length", ascending=True, inplace=True)
df.drop(columns="length", inplace=True)

# Drop the duplicates
df.drop_duplicates(subset=[1], inplace=True)  # Assuming the second column has index 1

# save the cleaned dataframe to urls_cleaned.csv
df.to_csv('urls_cleaned.csv', header=None, index=False)

