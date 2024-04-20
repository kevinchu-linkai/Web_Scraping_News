import sys
import requests
import pandas as pd
import os

from calendar import monthrange
from random import randint

def getArchiveURL(site, start_year, end_year, file_path):

    file = open(file_path, "a")

    for year in range(start_year,end_year+1):
        for month in range(1,13):
            for day in range (1,monthrange(year, month)[1]+1):

                while True:
                    try:
                        request_url = "https://archive.org/wayback/available?url={0}&timestamp={1}".format(site, "{0}{1:02d}{2:02d}{3}{4}{5}".format(year,month,day,randint(0,9),randint(0,9),randint(0,9)))
                        
                        response = eval(requests.get(request_url).content.decode('utf-8').replace("true","True"))

                        print (day, month, response["archived_snapshots"]["closest"]["url"])
                        file.write("{0},{1},{2}\n".format(response["archived_snapshots"]["closest"]["timestamp"], response["archived_snapshots"]["closest"]["url"],"no"))
                        break
                    except Exception as e:
                        print(e)
                        print ('MISSING', day, month, year)
                        break

    file.close()

    # open the csv file from data and remove duplicates
    df = pd.read_csv(file_path, header=None)
    df.drop_duplicates(subset=0, inplace=True)
    df.to_csv(file_path, index=False, header=None)


