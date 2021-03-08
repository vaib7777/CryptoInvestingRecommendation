import json
import os
import csv
import datetime

class DataPreProcessing:
    def __init__(self):
        self.input_path = "C:\\Users\\malha\\OneDrive\\Documents\\Mays Docs\\Fall 2019\\ADM\\Project 1\\limited_data\\"
        self.output_path = "C:\\Users\\malha\\OneDrive\\Documents\\Mays Docs\\Fall 2019\\ADM\\Project 1\\final_data\\"
        self.json_path = "C:\\Users\\malha\\PycharmProjects\\FirstProject\\coin_currency.json"
        self.cc_map = json.load(open(self.json_path, 'r'))

    def get_valid_files(self):
        coins = []
        path = "C:\\Users\\malha\\OneDrive\\Documents\\Mays Docs\\Fall 2019\\ADM\\Project 1\\extracted_data\\"

        valid_coins = ['RTE', 'ZEC', 'TKN', 'EDO', 'SCR', 'UOS', 'AUC', 'CLO', 'ELF', 'XTZ', 'BNT', 'IMP', 'VEE', 'BFT',
                       'ORS', 'LRC', 'VET', 'MGO', 'DAI', 'DAT', 'OKB', 'AMP', 'GEN', 'TRI', 'BAT', 'XRP', 'TRX', 'EVT',
                       'BTT', 'ZCN', 'WLO', 'BTG', 'BTC', 'SEN', 'ODE', 'NEO', 'ESS', 'FTT', 'REP', 'REQ', 'NEC', 'ZRX',
                       'CND', 'ABS', 'DSH', 'CNN', 'GOT', 'XVG', 'OMG', 'AST', 'RLC', 'LEO', 'WTC', 'UFR', 'BOX', 'ETP',
                       'ATM', 'CTX', 'LTC', 'ETC', 'ETH', 'RDN', 'ONL', 'POA', 'BBN', 'SWM', 'AVT', 'RCN', 'UST', 'MKR',
                       'ZIL', 'PNK', 'DTH', 'GNT']
        valid_file_list = []
        for file in os.listdir(path):
            if file[:3].upper() in valid_coins:
                valid_file_list.append(file)
        new_file_list = list(set(valid_file_list))
        print(len(new_file_list))

        #move data to limited data folder
        for file in new_file_list:
            os.rename(path + file, self.input_path + file)

    def add_ccid_to_valid_files(self):
        count = 1
        for file in os.listdir(self.input_path):
            if file.endswith(".csv"):
                with open(self.input_path + file, 'r') as input_csv_file:
                    with open(self.output_path + file, 'w') as output_csv_file:
                        csv_writer = csv.writer(output_csv_file, lineterminator='\n')
                        csv_reader = csv.reader(input_csv_file)
                        data = []
                        row = next(csv_reader)
                        #add to header
                        row.append("cc_id")
                        data.append(row)
                        for row in csv_reader:
                            row[0] = datetime.datetime.fromtimestamp(int(row[0]) / 1000).strftime('%Y-%m-%d %H:%M:%S')
                            row.append(self.cc_map[file[:6]])
                            data.append(row)
                        csv_writer.writerows(data)
            print(str(count) + " files written successfully!")
            count = count + 1

if __name__ == '__main__':
    adm_obj = DataPreProcessing()
    adm_obj.get_valid_files()
    adm_obj.add_ccid_to_valid_files()