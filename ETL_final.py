#Importing libraries
import pyinputplus as pyip # --- library for console menu options
import warnings
warnings.filterwarnings("ignore")

import pyspark as py
from pyspark.sql import SparkSession

from pyspark.sql.functions import*
from pyspark.sql.types import*
import json
import requests

import pymysql

import pandas as pd
import numpy as np
import datetime

#Creating Spark session
spark = SparkSession.builder.appName('Sparkappdemo').getOrCreate()

#Reading json files to df
branchdf = spark.read.json("C:/Capstone/JSON Databases/cdw_sapp_branch.json")
creditdf = spark.read.json("C:/Capstone/JSON Databases/cdw_sapp_credit.json")
customerdf = spark.read.json("C:/Capstone/JSON Databases/cdw_sapp_custmer.json")

# Converting df to PANDAS df
branchpd = branchdf.toPandas()
creditpd = creditdf.toPandas()
customerpd = customerdf.toPandas()

#Replacing BRANCH_ZIP NULL values in branch df
branchpd["BRANCH_ZIP"].fillna("00000", inplace = True)
#Change the format of phone number to (XXX)XXX-XXXX
branchpd['BRANCH_PHONE'] = '(' + branchpd['BRANCH_PHONE'].str[:3] + ')' + branchpd['BRANCH_PHONE'].str[3:6] + '-' + branchpd['BRANCH_PHONE'].str[6:11]
#Convert object type to timestamp
branchpd['LAST_UPDATED'] = pd.to_datetime(branchpd['LAST_UPDATED'])

#Convert DAY, MONTH, and YEAR into a TIMEID (YYYYMMDD)
creditpd['TIMEID']=creditpd['YEAR'].astype(str) + creditpd['MONTH'].astype(str).str.zfill(2)+ creditpd['DAY'].astype(str).str.zfill(2)
creditpd.drop(columns=['YEAR','MONTH','DAY'], inplace = True) #-- drop columns
#Rename Column
creditpd.rename(columns={"CREDIT_CARD_NO": "CUST_CC_NO"}, inplace=True)

#Convert the First Name & Last Name to Title Case, and Middle Name to Lower Case
customerpd['FIRST_NAME'] = customerpd['FIRST_NAME'].str.title()
customerpd['LAST_NAME'] = customerpd['LAST_NAME'].str.title()
customerpd['MIDDLE_NAME'] = customerpd['MIDDLE_NAME'].str.lower()

#Change the format of phone number to (XXX)XXX-XXXX
#customerpd['CUST_PHONE'] = '(XXX)' + customerpd['CUST_PHONE'].astype(str).str[:3] + '-' + customerpd['CUST_PHONE'].astype(str).str[3:8]
customerpd['CUST_PHONE'] = '(' + customerpd['CUST_PHONE'].astype(str).str[:3] + ')' + customerpd['CUST_PHONE'].astype(str).str[3:6] + '-' + customerpd['CUST_PHONE'].astype(str).str[6:11]

#Concatenate Apartment no and Street name of customer's Residence with comma as a seperator (Street, Apartment)
customerpd['FULL_STREET_ADDRESS'] = customerpd['STREET_NAME'] + ', ' + customerpd['APT_NO']
customerpd.drop(columns=['APT_NO','STREET_NAME'], inplace=True) #-- drop columns
#Convert object type to integer
customerpd['CUST_ZIP'] = customerpd['CUST_ZIP'].astype(int)
#Convert object type to timestamp
customerpd['LAST_UPDATED'] = pd.to_datetime(customerpd['LAST_UPDATED'])

url = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"
r = requests.get(url)
loans = json.loads(r.text)
loans_pd = pd.DataFrame.from_dict(loans)
print('Status code: ' + str(r.status_code))

#Converting pandas df to spark df
branchdf=spark.createDataFrame(branchpd)
creditdf=spark.createDataFrame(creditpd)
customerdf=spark.createDataFrame(customerpd)
loansdf=spark.createDataFrame(loans_pd)

with open('secrets.txt', 'r') as f:
    for line in f:
        if line.startswith('PASSWORD='):
            password = line.split('=')[1].strip()
        elif line.startswith('USER='):
            user = line.split('=')[1].strip()
        elif line.startswith('URL='):
            url = line.split('=')[1].strip()
        elif line.startswith('HOST='):
            host = line.split('=')[1].strip()

conn = pymysql.connect(user=user, password=password, host=host)
cursor = conn.cursor()
#Create a New Database
cursor.execute("CREATE DATABASE IF NOT EXISTS creditcard_capstone")

# Loading BRANCH df to Maria DB Heidi SQL
branchdf.write.format("jdbc") \
  .mode("append") \
  .option("url", url) \
  .option("dbtable", "CDW_SAPP_BRANCH") \
  .option("user", user) \
  .option("password", password) \
  .save()

# Loading CREDIT df to Maria DB Heidi SQL
creditdf.write.format("jdbc") \
  .mode("append") \
  .option("url", url) \
  .option("dbtable", "CDW_SAPP_CREDIT_CARD") \
  .option("user", user) \
  .option("password", password) \
  .save()

# Loading CUSTOMER df to Maria DB Heidi SQL
customerdf.write.format("jdbc") \
  .mode("append") \
  .option("url", url) \
  .option("dbtable", "CDW_SAPP_CUSTOMER") \
  .option("user", user) \
  .option("password", password) \
  .save()

# Loading LOANS df to Maria DB Heidi SQL
loansdf.write.format("jdbc") \
  .mode("append") \
  .option("url", url) \
  .option("dbtable", "CDW_SAPP_loan_application") \
  .option("user", user) \
  .option("password", password) \
  .save()

conn.close()