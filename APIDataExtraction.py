from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json

class DataExtraction:
    def __init__(self):
        self.url1 = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
        self.url2 = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/map'
        self.url3 = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/info'
        self.session = None

    def get_data_from_api2(self):
        parameters = {
            'symbol': 'RTE,ZEC,TKN,EDO,SCR,UOS,AUC,CLO,ELF,XTZ,BNT,IMP,VEE,BFT,ORS,LRC,VET,MGO,DAI,DAT,OKB,AMP,GEN,'
                      'TRI,BAT,XRP,TRX,EVT,BTT,ZCN,WLO,BTG,BTC,SEN,ODE,NEO,ESS,FTT,REP,REQ,NEC,ZRX,CND,ABS,DSH,CNN,'
                      'GOT,XVG,OMG,AST,RLC,LEO,WTC,UFR,BOX,ETP,ATM,CTX,LTC,ETC,ETH,RDN,ONL,POA,BBN,SWM,AVT,RCN,UST,'
                      'MKR,ZIL,PNK,DTH,GNT'
        }
        response = self.session.get(self.url2, params=parameters)
        data = json.loads(response.text)
        File_object = open(r"meta.csv", "a")
        for index in data["data"]:
            File_object.write("\n" + str(data["data"][index]["id"]) + "," + str(data["data"][index]["name"]) +
                              "," + str(data["data"][index]['symbol']) + "," + str(data["data"][index]["slug"])
                              + "," + str(data["data"][index]["is_active"]) + ","
                              + str(data["data"][index]["first_historical_data"]) + ","
                              + str(data["data"][index]["last_historical_data"]))
        File_object.close()

    def get_data_from_api1(self):
        parameters = {
            'start': '1',
            'limit': '2500',
            'convert': 'USD',
            'aux': 'num_market_pairs,cmc_rank,date_added,tags,platform,max_supply,circulating_supply,total_supply,'
                   'market_cap_by_total_supply,volume_24h_reported,volume_7d,volume_7d_reported,'
                   'volume_30d,volume_30d_reported'
        }
        response = self.session.get(self.url1, params=parameters)
        data = json.loads(response.text)
        File_object = open(r"quotedata.csv", "a")
        for index in data["data"]:
            File_object.write("\n" + str(index["id"]) + "," + str(index["cmc_rank"]) + "," + str(
                index["num_market_pairs"]) + "," + str(index["circulating_supply"]) + "," + str(
                index["total_supply"]) + "," + str(index["max_supply"]) + "," + str(
                index["quote"]["USD"]["price"]) + "," + str(index["quote"]["USD"]["volume_24h"]) + "," + str(
                index["quote"]["USD"]["volume_24h_reported"]) + "," + str(
                index["quote"]["USD"]["volume_7d"]) + "," + str(
                index["quote"]["USD"]["volume_7d_reported"]) + "," + str(
                index["quote"]["USD"]["volume_30d"]) + "," + str(
                index["quote"]["USD"]["volume_30d_reported"]) + "," + str(
                index["quote"]["USD"]["percent_change_1h"]) + "," + str(
                index["quote"]["USD"]["percent_change_24h"]) + "," + str(
                index["quote"]["USD"]["percent_change_7d"]) + "," + str(
                index["quote"]["USD"]["market_cap"]) + "," + str(index["quote"]["USD"]["last_updated"]))
        File_object.close()

    def get_data_from_api3(self):
        parameters = {
            'symbol': 'RTE,ZEC,TKN,EDO,SCR,UOS,AUC,CLO,ELF,XTZ,BNT,IMP,VEE,BFT,ORS,LRC,VET,MGO,DAI,DAT,OKB,AMP,GEN,'
                      'TRI,BAT,XRP,TRX,EVT,BTT,ZCN,WLO,BTG,BTC,SEN,ODE,NEO,ESS,FTT,REP,REQ,NEC,ZRX,CND,ABS,DSH,CNN,'
                      'GOT,XVG,OMG,AST,RLC,LEO,WTC,UFR,BOX,ETP,ATM,CTX,LTC,ETC,ETH,RDN,ONL,POA,BBN,SWM,AVT,RCN,UST,'
                      'MKR,ZIL,PNK,DTH,GNT'
        }
        response = self.session.get(self.url3, params=parameters)
        data = json.loads(response.text)
        File_object = open(r"urldata.csv", "a")
        for index in data["data"]:
            File_object.write("\n" + str(data["data"][index]["id"]) + "," + str(data["data"][index]["logo"]) +
                              "," + str(data["data"][index]['urls']["website"][0]))
        File_object.close()

    def main(self):
        headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': '1aa13516-894f-4ea9-98eb-bb809e8f5bff',
        }
        self.session = Session()
        self.session.headers.update(headers)
        self.get_data_from_api1()
        self.get_data_from_api2()
        self.get_data_from_api3()
        print("Successfully loaded data from all APIs")

if __name__ == '__main__':
    de_obj = DataExtraction()
    de_obj.main()
