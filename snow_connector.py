#!/usr/bin/env python
import pandas as pd  # for analysis
import numpy as np
from scipy import stats  # for mode() methods and other science methods
import matplotlib.pyplot as plt  # plotting
import snowflake.connector
import seaborn as sns  # for better data visualization

# Gets the version
ctx = snowflake.connector.connect(
    user='<username>',
    password='<password>',
    account='<account>'  # 
)
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])

    # print()
    sql = "CREATE WAREHOUSE IF NOT EXISTS project_warehouse"
    cs.execute(sql)
    print("Warehouse create")

    sql = "CREATE WAREHOUSE IF NOT EXISTS project_warehouse"
    cs.execute(sql)
    print("Warehouse created")

    sql = "USE WAREHOUSE project_warehouse"
    cs.execute(sql)
    print("Warehouse project_warehouse now in use ")

    sql = "CREATE DATABASE IF NOT EXISTS project_db"
    cs.execute(sql)
    print("Database project_db created")

    sql = "USE DATABASE project_db"
    cs.execute(sql)
    print("Database project_db in use now")

    sql = "CREATE SCHEMA IF NOT EXISTS project_schema"
    cs.execute(sql)
    print("Schema project_schema created ")

    sql = "USE SCHEMA project_schema"
    cs.execute(sql)
    print(" Schema project_schema in use")

    # DB RESOUURSES CREATED
    print("ALL DB RESOURCES NEED FOR TABLE COMPLETED")
    #######################

    sql = "CREATE OR REPLACE TABLE project_table (id integer autoincrement unique, name string, age integer, height float, register integer, created timestamp) "
    cs.execute(sql)
    print(" Table project_table created successfully")

    sql = "INSERT INTO project_table (name, age, height, register, created) values ('John Doe', 40, 5.6, 1001, current_timestamp() )"
    cs.execute(sql)

    sql = "INSERT INTO project_table (name, age, height, register, created) values ('Dayo Greats', 38, 5.8, 1004, current_timestamp() )"
    cs.execute(sql)

    sql = "INSERT INTO project_table (name, age, height, register, created) values ('Bisi Odu', 35, 5.6, 1003, current_timestamp() )"
    cs.execute(sql)

    sql = "INSERT INTO project_table (name, age, height, register, created) values ('Leslie Jones', 52, 5.4, 1004, current_timestamp() )"
    cs.execute(sql)

    sql = "INSERT INTO project_table (name, age, height, register, created) values ('barack Obama', 55, 5.9, 1002, current_timestamp() )"
    cs.execute(sql)
    print("4 rows of data inserted into project_table @ ")

    #  LETS SELECT  / FETCH SOME DATA FROM SNOWFLAKE DB
    print(" Fetching data from the snowlake db")

    sql = "SELECT * from project_table"
    cs.execute(sql)

    df = cs.fetch_pandas_all()  # or you can get same result by looping through using the loop process below

    # # LOOPING THROUGH THE DB
    # for row in cs.fetchall():
    #     print(row)
    # print("Panda dataframe begins ")
    # print("+++++++++++++++++++++++++++++++")
    # # USE PANDA MODULE TO PASS INTO DATAFRAME
    # df = pd.DataFrame(row)

    print("printing dataframe 'dset")
    print(df)
    print("+++++++++++++++++++++++++++++++")
    # print(df.info())  # to see info about the data
    print("+++++++++++++++++++++++++++++++")

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
    print("print '75%' of dataset")
    print(percentile)

    # DATA VISUALIZATION
    # df.plot(kind = 'scatter', x = 'AGE', y = 'HEIGHT')
    # plt.show()

    # SEABORN MODULE

    # SCATTER PLOTTING
    # sns.scatterplot(data=df, x=x, y=y, hue="CREATED", style="REGISTER")
    # print("Seaborn finished plotting")

    # HISTO PLOTTING
    # sns.distplot(y, hist=False) #seaborn without drawing histogram
    # plt.show()

    # sns.distplot(y, hist=True) #seaborn with drawing histogram
    # plt.show()

finally:
    cs.close()
ctx.close()