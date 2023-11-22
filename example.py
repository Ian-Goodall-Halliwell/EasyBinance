from EasyBinance.pull_data import download

out = download(clients= "C:/Users/Ian/Desktop/clients.csv",
               datapath="data",starttime="2023-01-01 00:00:00",
               endtime="now",
               freq="5m",
               mcap="5000k",
               tokens=["BUSD"])
    