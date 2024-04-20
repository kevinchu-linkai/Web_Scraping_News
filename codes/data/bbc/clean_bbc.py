import pandas as pd

# open urls_uncleaned.csv
df = pd.read_csv('urls_uncleaned.csv', header=None, encoding='utf-8')

# drop all the rows that do not contain 'us'
df = df[df[1].str.contains('-us-')]

# drop all the rows that contains /live/
df = df[~df[1].str.contains('/live/')]

# drop the all rows that contains live-report
df = df[~df[1].str.contains('live-report')]

df = df[~df[1].str.contains('/av/')]


# sort the rows based on the value of the first column in ascending order
df.sort_values(by=0, ascending=True, inplace=True)

# save the cleaned dataframe to urls_cleaned.csv
df.to_csv('urls_cleaned.csv', header=None, index=False)

