import traceback
import pymysql
import os
import json

class DBOperations:
    def __init__(self):
        self.host = "3.208.229.247"
        self.user = "adm-user"
        self.password = "123456"
        self.port = 3306
        #self.config_json_path = "C:\\Users\\malha\\PycharmProjects\\FirstProject\\config.json"
        self.config_json_path = "/home/centos/adm/code/config.json"
        self.config = json.load(open(self.config_json_path, 'r'))

    def get_db_connection(self, db):
        try:
            conn = pymysql.connect(host=self.host, user=self.user, passwd=self.password, db=db, port=self.port,
                                   local_infile=True, autocommit=True)
            print("Connection to db created successfully")
            return conn
        except:
            traceback.print_exc()

    def create_databases(self):
        db = "mysql"
        conn_obj = self.get_db_connection(db=db)
        cursor = conn_obj.cursor()
        for database in self.config["databases"]:
            cmd = self.config["databases"][database]
            try:
                cursor.execute(cmd)
                for row in cursor:
                    print(row)
            except:
                traceback.print_exc()
            print("Successfully created database: " + str(database))
        cursor.close()
        conn_obj.close()

    def create_tables(self):
        db = "mysql"
        conn_obj = self.get_db_connection(db=db)
        cursor = conn_obj.cursor()
        for database in self.config["databases"]:
            for table in self.config[database]:
                cmd = self.config[database][table].replace("$$db", database)
                try:
                    cursor.execute(cmd)
                    for row in cursor:
                        print(row)
                except:
                    traceback.print_exc()
                print("Successfully created table: " + str(table))
        cursor.close()
        conn_obj.close()

    def insert_cmc_api_data(self):
        db="adm_landing"
        conn_obj = self.get_db_connection(db=db)
        cursor = conn_obj.cursor()
        cmc_dict = {
            "cmc_url_data" : "/home/centos/adm/data/cmc_csv_data/urldata.csv",
            "cmc_meta_api_data" : "/home/centos/adm/data/cmc_csv_data/meta.csv",
            "cmc_listing_data" : "/home/centos/adm/data/cmc_csv_data/quotedata.csv"
        }

        for table in cmc_dict:
            cmd = "LOAD DATA LOCAL INFILE '" + cmc_dict[table] + "' " \
                                                             "INTO TABLE " + db + "." + table + \
                  " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'"
            print("Executing Command: " + cmd)
            try:
                cursor.execute(cmd)
                for row in cursor:
                    print(row)
            except:
                traceback.print_exc()
            print("Data loaded successfully for table: " + table)
        cursor.close()
        conn_obj.close()

    def insert_git_twitter_data(self):
        db = "adm_landing"
        conn_obj = self.get_db_connection(db=db)
        cursor = conn_obj.cursor()

        table_dict = {
            "adm_landing.coin_currency_mapping" : "/home/centos/adm/data/cmc_csv_data/coin_currency_mapping.csv",
            "adm_landing.git_commits" : "/home/centos/adm/data/cmc_csv_data/git_commit_data.csv"
        }

        for table in table_dict:
            cmd = "LOAD DATA LOCAL INFILE '" + table_dict[table] + "' " \
                                                             "INTO TABLE " + table + \
                  " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' IGNORE 1 LINES"
            print("Executing Command: " + cmd)
            try:
                cursor.execute(cmd)
                for row in cursor:
                    print(row)
            except:
                traceback.print_exc()
            print("Data loaded successfully for table: " + table)
        #git data
        cmd = "LOAD DATA LOCAL INFILE 'home/centos/adm/data/twitter_followers.csv' INTO TABLE adm_working.twitter_data " \
              "FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'"
        print("Executing Command: " + cmd)
        cursor.execute(cmd)
        cursor.close()
        conn_obj.close()

    def insert_kaggle_csv_data(self):
        db="adm_landing"
        conn_obj = self.get_db_connection(db=db)
        cursor = conn_obj.cursor()
        # table details
        table_name = "kaggle_csv_data"
        # get list of csv files
        path = "/home/centos/adm/data/kaggle_csv_data/"
        count = 1
        for file in os.listdir(path):
            if file.endswith(".csv"):
                cmd = "LOAD DATA LOCAL INFILE '" + path + file + "' " \
                                                                 "INTO TABLE " + db + "." + table_name + \
                      " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\r\\n' IGNORE 1 LINES"
                print("Executing Command: " + cmd)
                try:
                    cursor.execute(cmd)
                    for row in cursor:
                        print(row)
                except:
                    traceback.print_exc()
            print("File #" + str(count) + " loaded successfully!")
            count = count + 1
        cursor.close()
        conn_obj.close()

    def load_volatality_data(self):
        vol_queries = []
        vol_queries.append(self.config["volatality_query"])
        vol_queries.append(self.config["volatality_final_query"])
        db = "adm_landing"
        conn_obj = self.get_db_connection(db=db)
        cursor = conn_obj.cursor()
        for query in vol_queries:
            try:
                cursor.execute(query)
                for row in cursor:
                    print(row)
            except:
                traceback.print_exc()
        print("Volatality query executed!")


    def main(self):
        self.create_databases()
        self.create_tables()
        #self.insert_kaggle_csv_data()
        self.insert_cmc_api_data()
        self.insert_git_twitter_data()
        self.load_volatality_data()

if __name__ == '__main__':
    dbo = DBOperations()
    dbo.main()