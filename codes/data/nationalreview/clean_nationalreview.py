import pandas as pd

# open urls_uncleaned.csv
df = pd.read_csv('urls_uncleaned.csv', header=None, encoding='utf-8')

# drop all the rows that do not contain 'us'
# df = df[df[1].str.contains('-us-')]


#detect which row is the first row that contains /node/
row = df[df[1].str.contains('/node/')].index[0]
# drop all the rows till row the row before the first row that contains /node/
df = df.drop(df.index[0:row])

# drop all the rows that contains /subscribe/
df = df[~df[1].str.contains('/subscribe/')]
# drop all the rows that contains /qa/
df = df[~df[1].str.contains('/qa/')]
# drop all the rows that contains /node/
df = df[~df[1].str.contains('/node/')]
# drop the all rows that contains /video/
df = df[~df[1].str.contains('/video/')]
# drop the all rows that contains /videos/
df = df[~df[1].str.contains('/videos/')]
# drop the all rows that contains /corner/
df = df[~df[1].str.contains('/corner/')]
# drop the all rows that contains /the-feed/
df = df[~df[1].str.contains('/the-feed')]
# drop the all rows that contains /feed
df = df[~df[1].str.contains('/feed')]
# drop the all rows that contains /phi-beta-cons
df = df[~df[1].str.contains('/phi-beta-cons')]
# drop the all rows that contains /agenda/
df = df[~df[1].str.contains('/agenda/')]
# drop the all rows that contains /nrd/
df = df[~df[1].str.contains('/nrd/')]
# drop the all rows that contains /author/
df = df[~df[1].str.contains('/author/')]
# drop the all rows that contains /user/
df = df[~df[1].str.contains('/user/')]
# drop the all rows that contains /podcasts/
df = df[~df[1].str.contains('/podcasts/')]
# drop the all rows that contains /slideshows/
df = df[~df[1].str.contains('/slideshows/')]
# drop the all rows that contains /collections/
df = df[~df[1].str.contains('/collections/')]
# drop the all rows that contains /betweenthecovers/
df = df[~df[1].str.contains('/betweenthecovers/')]
# drop the all rows that contains /nrdsubscribe
df = df[~df[1].str.contains('/nrdsubscribe')]
# drop the all rows that contains web.archive.org
df = df[~df[1].str.contains('web.archive.org')]
# drop the all rows that has length less than 40
df = df[df[1].str.len() >= 50]

# sort the rows based on the value of the first column in ascending order
df.sort_values(by=0, ascending=True, inplace=True)

# save the cleaned dataframe to urls_cleaned.csv
df.to_csv('urls_cleaned.csv', header=None, index=False)

