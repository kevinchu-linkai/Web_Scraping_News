import pandas as pd

# open urls_uncleaned.csv
df = pd.read_csv('urls_uncleaned.csv', header=None, encoding='utf-8')

# drop the all rows that has length less than 40
df = df[df[1].str.len() >= 50]
# drop the all rows that has length less than 40
df = df[df[1].str.contains('/us/')]
# drop the all rows that contains web.archive.org
df = df[~df[1].str.contains('web.archive.org')]
# drop the all rows that contains /interactive/
df = df[~df[1].str.contains('/interactive/')]
# drop the all rows that contains /slideshow/
df = df[~df[1].str.contains('/slideshow/')]
# drop the all rows that contains /aponline/
df = df[~df[1].str.contains('/aponline/')]
# drop the all rows that contains /top/
df = df[~df[1].str.contains('/top/')]


# sort the rows based on the value of the first column in ascending order
df.sort_values(by=0, ascending=True, inplace=True)

# save the cleaned dataframe to urls_cleaned.csv
df.to_csv('urls_cleaned.csv', header=None, index=False)

