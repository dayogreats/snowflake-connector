# This is a sample Python script.
# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
# !/usr/bin/env python

import pandas as pd  # for analysis
from scipy import stats  # for mode() methods and other science methods
import matplotlib.pyplot as plt  # plotting
import seaborn as sns  # for better data visualization
import snowflake.connector
import numpy as np


class SnowflakeConnection:
    def __init__(self, user, password, account):
        self.con = snowflake.connector.connect(
            user=user,
            password=password,
            account=account
        )
        self.cs = self.con.cursor()

    def execute_query(self, query):
        self.cs.execute(query)

    def fetch_one(self):
        return self.cs.fetchone()

    def close(self):
        self.con.close()

    def create_wh(self, name):
        sql = f"CREATE WAREHOUSE IF NOT EXISTS {name}"
        self.cs.execute(sql)
        print(f"Warehouse '{name}' created")

    def create_db(self, name):
        sql = f"CREATE DATABASE IF NOT EXISTS {name}"
        self.cs.execute(sql)
        print(f"Database '{name}' created")

    def create_schema(self, name):
        sql = f"CREATE OR REPLACE SCHEMA {name}"
        self.cs.execute(sql)
        print(f"Schema {name} created created")

    def use_resource(self, name, rtype):
        sql = f"USE {rtype} {name}"
        self.cs.execute(sql)
        print(f"Using {name} create")

    def create_table(self, tab_name):
        sql = f"CREATE OR REPLACE TABLE {tab_name} (id integer autoincrement unique, name string, age integer, height float, register integer, created timestamp)"
        self.cs.execute(sql)
        print(f" Table {tab_name} created successfully")

    def insert_table(self, tab_name):
        sql = f"INSERT INTO {tab_name} (name, age, height, register, created) values ('John Doe', 40, 5.6, 1001, current_timestamp() )"
        self.cs.execute(sql)

        sql = f"INSERT INTO {tab_name} (name, age, height, register, created) values ('John Doe', 40, 5.6, 1001, " \
              f"current_timestamp() )"
        self.cs.execute(sql)

        sql = f"INSERT INTO {tab_name} (name, age, height, register, created) values ('Dayo Greats', 38, 5.8, 1004, " \
              f"current_timestamp() )"
        self.cs.execute(sql)

        sql = f"INSERT INTO {tab_name} (name, age, height, register, created) values ('Bisi Odu', 35, 5.6, 1003, " \
              f"current_timestamp() )"
        self.cs.execute(sql)

        sql = f"INSERT INTO {tab_name} (name, age, height, register, created) values ('Leslie Jones', 52, 5.4, 1004, " \
              f"current_timestamp() )"
        self.cs.execute(sql)

        sql = f"INSERT INTO {tab_name} (name, age, height, register, created) values ('barack Obama', 55, 5.9, " \
              f"1002, current_timestamp() )"
        self.cs.execute(sql)

        print("4 rows of data inserted into project_table @ ")
        self.cs.execute(sql)
        print(f" Table {tab_name} inserted successfully")

    #     DATA FETCHER WITH PANDAS METHODS
    def fetcher(self, tab_name):
        #  LETS SELECT  / FETCH SOME DATA FROM SNOWFLAKE DB
        print(" Fetching data from the snowflake db")
        sql = f"SELECT * from {tab_name}"
        self.cs.execute(sql)

        """ you need to do ... """
        """ pip install snowflake-connector-python[secure-local-storage,pandas] """
        """ for panda dependencies to work """
        # df = pd.DataFrame(self.cs.fetchall())  # t

        df = self.cs.fetch_pandas_all()  # or you can get same result by looping through using the loop process below

        #  then print the returned dataframe
        print(f"Printing dataframe now from {tab_name}!!!")
        print("+++++++++++++STARTS++++++++++++++++++")
        print(df)
        # print(df.info())  # use to see info() about the dataframe
        print("+++++++++++++++ENDS+++++++++++++++++++")

        return df

    def explorer(self, tab_name):
        df = self.fetcher(tab_name)
        # EXPLORING DATASET FROM THE DATAFRAME
        print('+ Exploring data with pandas +')

        print("Extracting AGE column into X")
        x = df["AGE"]
        print("Printing AGE as X")
        print(x)

        print("Extracting HEIGHT column into Y")
        y = df["HEIGHT"]
        print("Printing HEIGHT as Y")
        print(y)

        # DATA ANALYSIS BEGINS

    def analyzer(self, tab_name):
        df = self.fetcher(tab_name)
        x = df["AGE"]
        y = df["HEIGHT"]

        print("DATA ANALYSIS BEGINS")
        mean = np.mean(y)
        print(f"Mean: {mean}")

        median = np.median(y)
        print(f"Median: {median}")

        mode = stats.mode(y)
        print(f"Mode: {mode}")

        # using mode method that comes pandas to get mode of the dataset
        mode = df.mode(
            numeric_only=True)  # Setting numeric_only=True, only the mode of numeric columns is computed, and columns of other types are ignored
        print(f"Mode: {mode}")

        sd = np.std(y)
        print(f"Standard deviation:{sd}")

        # processing 75% of the age dataset
        percentile = np.percentile(x, 75)
        print(f"printing '75%' of dataset are below {percentile} of age")
        print(percentile)

    def visualizer(self, tab_name):
        print("------VISUALIZING STARTS------")
        df = self.fetcher(tab_name)
        x = df["AGE"]
        y = df["HEIGHT"]

        # DATA VISUALIZATION
        df.plot(kind='scatter', x='AGE', y='HEIGHT')
        plt.show()

        print("------VISUALIZING ENDS--------")

        # SEABORN MODULE

        # SCATTER PLOTTING
        print("------SCATTER PLOTTING BEGINS--------")
        sns.scatterplot(data=df, x=x, y=y, hue="CREATED", style="REGISTER")
        print("Seaborn finished plotting")

        # HISTO PLOTTING
        print("PLOTTING HISTOGRAM")
        sns.distplot(y, hist=False)  # seaborn without drawing histogram
        plt.show()

        sns.distplot(y, hist=True)  # seaborn with drawing histogram
        plt.show()


def main():
    snowflake_conn = SnowflakeConnection(user='<username>', password='<password>', account='<account>')
    snowflake_conn.execute_query("SELECT current_version()")
    print(snowflake_conn.fetch_one())
    snowflake_conn.create_wh('hero_wh')
    snowflake_conn.create_db('hero_db')
    snowflake_conn.use_resource('hero_wh', 'warehouse')
    snowflake_conn.use_resource('hero_db', 'database')
    # snowflake_conn.create_schema('hero_schema')
    snowflake_conn.use_resource('hero_schema', 'schema')
    snowflake_conn.create_table('hero_tb')
    snowflake_conn.insert_table('hero_tb')

    # FETCHING DATA FROM SNOWFLAKE
    snowflake_conn.fetcher('hero_tb')

    # ANALYSIZING DATA
    snowflake_conn.analyzer('hero_tb')

    # VISUALIZING DATA
    snowflake_conn.visualizer('hero_tb')

    snowflake_conn.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
