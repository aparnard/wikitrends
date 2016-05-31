import graphlab as gl

all_wiki_data = gl.SFrame.read_csv('/Users/Akash/Desktop/wikitrends-aws-data/*.csv', header = True)
all_wiki_data.save("../Data/Raw_Wiki_2010_15.csv")

# grouped_data = all_wiki_data.groupby(["Proj","Page","Date"],gl.aggregate.SUM('PageViews'))
# print grouped_data
# grouped_data.save("All_Wiki_2010_15.csv")