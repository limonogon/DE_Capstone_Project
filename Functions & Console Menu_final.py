#Importing libraries
import pyinputplus as pyip # --- library for console menu options
import pymysql
import pyspark as py
from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from pyspark.sql.types import*
import json
import requests
import re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime
import warnings
warnings.filterwarnings("ignore")

spark = SparkSession.builder.appName('Sparkappdemo').getOrCreate()

with open('secrets.txt', 'r') as f:
    for line in f:
        if line.startswith('PASSWORD='):
            password = line.split('=')[1].strip()
        elif line.startswith('USER='):
            user = line.split('=')[1].strip()
        elif line.startswith('URL='):
            url = line.split('=')[1].strip()

#Create a connection to MYSQL Database
branch_df=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user=user,\
                                     password=password,\
                                     url=url,\
                                     dbtable="CDW_SAPP_BRANCH").load()

credit_df=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user=user,\
                                     password=password,\
                                     url=url,\
                                     dbtable="CDW_SAPP_CREDIT_CARD").load()

customer_df=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user=user,\
                                     password=password,\
                                     url=url,\
                                     dbtable="CDW_SAPP_CUSTOMER").load()

loan_df=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user=user,\
                                     password=password,\
                                     url=url,\
                                     dbtable="CDW_SAPP_loan_application").load()

credit_df = credit_df.withColumnRenamed("BRANCH_CODE","CC_BRANCH_CODE")
#Join 3 df to 1
joined_df = credit_df.join(customer_df, credit_df['CUST_SSN'] == customer_df['SSN']).join(branch_df, credit_df['CC_BRANCH_CODE'] == branch_df['BRANCH_CODE'])
dup_cols = ["CC_BRANCH_CODE", "CUST_CC_NO", "CUST_SSN"]
joined_df = joined_df.drop(*dup_cols)

#################################################################################################################################################
# 2.1.1.Used to display the transactions made by customers living in a given zip code for a given month and year. Order by day in descending order.
def transactions_by_zip_my(zip, month, year):
    joined_df.select('CREDIT_CARD_NO', 'TIMEID',
                'TRANSACTION_ID',
                'TRANSACTION_TYPE',
                'TRANSACTION_VALUE',
                'CUST_ZIP').filter((joined_df.CUST_ZIP == zip) &
                                      (joined_df.TIMEID.startswith(year+str(month).zfill(2)))
                                      ).sort(joined_df.TIMEID.desc()).show()

# 2.1.2.Used to display the number and total values of transactions for a given type.
def transactions_by_type(type):
    joined_df.groupby('TRANSACTION_TYPE').agg(count('TRANSACTION_VALUE').alias("Total Number of Transactions"),
                                         round(sum('TRANSACTION_VALUE'),2).alias("Total Amount of Transactions")
                                         ).filter(joined_df.TRANSACTION_TYPE == type).show()

# 2.1.3.Used to display the number and total values of transactions for branches in a given state.
def transactions_of_state_branches(state):
    joined_df.groupby('BRANCH_STATE').agg(count('TRANSACTION_VALUE').alias("Total Number of Transactions"),
                                        round(sum('TRANSACTION_VALUE'),2).alias("Total Amount of Transactions")
                                        ).filter(joined_df.BRANCH_STATE == state).show()

## 2.2 Customer Details Module
# 2.2.1.Used to check the existing account details of a customer.
def account_by_ssn(ssn):
    customer_df.select('CREDIT_CARD_NO',
                'FIRST_NAME', 'LAST_NAME', 'MIDDLE_NAME',
                'CUST_PHONE', 'CUST_EMAIL',
                'FULL_STREET_ADDRESS',
                'CUST_CITY', 'CUST_STATE', 'CUST_ZIP'
                ).filter(joined_df.SSN == ssn).show()

# 2.2.2. Used to modify the existing account details of a customer.
def update_customer(ssn_data):    
    conn = pymysql.connect(user='root', password='Pass1234', host='127.0.0.1', database='creditcard_capstone')
    cursor = conn.cursor()
    
    update_query = "UPDATE cdw_sapp_customer SET \
            FIRST_NAME = '{}',\
            LAST_NAME = '{}',\
            MIDDLE_NAME = '{}',\
            CUST_PHONE = '{}',\
            CUST_EMAIL = '{}',\
            FULL_STREET_ADDRESS = '{}',\
            CUST_CITY = '{}',\
            CUST_STATE = '{}',\
            CUST_ZIP = '{}',\
            CUST_COUNTRY = '{}'\
        WHERE SSN = '{}'".format(ssn_data.loc[0,'FIRST_NAME'], ssn_data.loc[0,'LAST_NAME'], \
        ssn_data.loc[0,'MIDDLE_NAME'], ssn_data.loc[0,'CUST_PHONE'], ssn_data.loc[0,'CUST_EMAIL'],\
        ssn_data.loc[0,'FULL_STREET_ADDRESS'], ssn_data.loc[0,'CUST_CITY'], ssn_data.loc[0,'CUST_STATE'], \
        ssn_data.loc[0,'CUST_ZIP'], ssn_data.loc[0,'CUST_COUNTRY'], ssn_data.loc[0,'SSN'])

    try:
        cursor.execute(update_query)
        conn.commit()
        print('Customer Information was Successfully Updated!')
    except:
        conn.rollback()
        print('Customer Information was NOT Updated! Check the database variable restrictions!')
    
    conn.close()

# 2.2.3. Used to generate a monthly bill for a credit card number for a given month and year.
def monthly_bill(cc_no, month, year):
    # Creating temporary filtered df of CC transactions for the given period
    temp = credit_df.filter((credit_df.CUST_CC_NO == cc_no) &
                        (credit_df.TIMEID.startswith(year+month.zfill(2)))
                        ).sort(credit_df.TIMEID)

    #Printing all transactions list
    temp.show()
    #Printing the total bill amount
    temp.select(round(sum(temp.TRANSACTION_VALUE),2).alias("Total Monthly Bill Amount")).show()

# 2.2.4. Used to display the transactions made by a customer between two dates. Order by year, month, and day in descending order.
def transactions_between_dates(cc_no, inp_s_date, inp_e_date):

    #get_date function to get date from string values
    import datetime

    def get_date(inp_date):
        date_patterns = ["%m-%d-%Y", "%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"]

        for pattern in date_patterns:
            try:
                return datetime.datetime.strptime(inp_date, pattern).date()
            except:
                pass
        print("Date is not in expected format: %s" %(inp_date))

    start_date = str(get_date(inp_s_date).year) + str(get_date(inp_s_date).month).zfill(2) + str(get_date(inp_s_date).day).zfill(2)
    end_date = str(get_date(inp_e_date).year) + str(get_date(inp_e_date).month).zfill(2) + str(get_date(inp_e_date).day).zfill(2)

    credit_df.filter((credit_df.CUST_CC_NO == cc_no) &
                    (credit_df.TIMEID >= start_date) & (credit_df.TIMEID <= end_date)
                    ).sort(credit_df.TIMEID).show()
    
def validate_date(date_string):
    try:
        datetime.datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False

##3 - Functional Requirements - Data analysis and Visualization
# 3.1. Find and plot which transaction type has a high rate of transactions.
def plot_high_rate_trans_avg():
    tr_t_avg_df = credit_df.groupby('TRANSACTION_TYPE').agg(count('TRANSACTION_VALUE').alias("Number of Transactions")
                                            ).sort(desc("Number of Transactions")).toPandas()

    # function to add value labels
    def addlabels(x,y):
        for i in range(len(x)):
            plt.text(i, y[i], y[i], ha = 'center')

    x = tr_t_avg_df.iloc[:,0].tolist()
    y = tr_t_avg_df.iloc[:,1].tolist()

    tr_t_avg_df.plot(kind='bar', x='TRANSACTION_TYPE', y='Number of Transactions', figsize=(10,6), color='coral', legend=False)
    addlabels(x, y)
    plt.title('Number of Transactions by Transaction Type')
    plt.xlabel('Transaction Type')
    plt.xticks(rotation=0)
    plt.show()

# 3.2. Find and plot which state has a high number of customers.
def plot_numb_cust_state():
    st_cust_count_df = customer_df.groupby('CUST_STATE').agg((count('CUST_STATE')).alias("COUNT_CUST")).sort(count('CUST_STATE').desc()).toPandas()

    # function to add value labels
    def addlabels(x,y):
        for i in range(len(x)):
            plt.text(i, y[i], y[i], ha = 'center')
    # lists of values
    x = st_cust_count_df.iloc[:,0].tolist()
    y = st_cust_count_df.iloc[:,1].tolist()

    st_cust_count_df.plot(kind='bar', x='CUST_STATE', y='COUNT_CUST', figsize=(10,6), color='aqua', legend=False)
    addlabels(x, y)
    plt.title('Customers Number by State')
    plt.xlabel('State')
    plt.ylabel('Customers Number')
    plt.xticks(rotation=0)
    plt.show()

# 3.3. Find and plot the sum of all transactions for each customer, and which customer has the highest transaction amount. hint(use CUST_SSN).
def plot_top_cust_by_trans_2():

    top_cust = credit_df.groupby('CUST_SSN').agg((round(sum('TRANSACTION_VALUE'),2)).alias("SUM_TRANSACTION_VALUE")).sort(sum('TRANSACTION_VALUE').desc()).toPandas()
    # top customer id
    top_cust_id = top_cust.iloc[0,0]
    # hide SSN
    top_cust['CUST_SSN'] = 'XXX-XX-' + top_cust['CUST_SSN'].astype(str).str[-4:]
    # DF for top 10 customers bar chart
    top10_cust = top_cust.iloc[:11,:]
    #top 1 customer data for pie chart
    top_cust1_tr = credit_df.filter(credit_df.CUST_SSN == str(top_cust_id)) \
                    .groupby('TRANSACTION_TYPE').agg((round(sum('TRANSACTION_VALUE'),2)).alias("SUM_TRANSACTION_VALUE")) \
                    .sort(sum('TRANSACTION_VALUE').desc()).toPandas().set_index('TRANSACTION_TYPE')


    fig = plt.figure() # create figure

    ax0 = fig.add_subplot(1, 2, 1) # add subplot 1 (1 row, 2 columns, first plot)
    ax1 = fig.add_subplot(1, 2, 2) # add subplot 2 (1 row, 2 columns, second plot)


    # Subplot 1: Bar chart
    # function to add value labels
    def addlabels(x,y):
        for i in range(len(x)):
            ax0.text(i, y[i]-1000, y[i], ha = 'center', va='center', rotation = 'vertical')
    # lists of values
    x = top10_cust.iloc[:,0].tolist()
    y = top10_cust.iloc[:,1].tolist()

    top10_cust.plot(kind='bar', x='CUST_SSN', y='SUM_TRANSACTION_VALUE', ax=ax0, figsize=(20,12), color='g', legend=False)

    addlabels(x,y)
    ax0.set_title('Top 10 Customers by Transaction Amount')
    ax0.set_xlabel('Customer ID')
    ax0.set_ylabel('Total Transactions Amount')

    # Subplot 2: Pie chart
    colors_list = ['gold', 'yellowgreen', 'lightcoral', 'lightskyblue', 'lightgreen', 'pink', 'teal']
    explode_list = [0.1, 0, 0, 0, 0, 0, 0]

    top_cust1_tr['SUM_TRANSACTION_VALUE'].plot(kind='pie',
                                figsize=(20,12),
                                ax=ax1,
                                explode=explode_list,
                                colors = colors_list,
                                autopct='%1.1f%%',
                                startangle=10,
                                shadow=True,
                                )

    ax1.set_title("Top Customer's Transaction by Type", y=1.12)
    ax1.axis('equal')
    ax1.set_ylabel('')

    plt.show()

##5 - Functional Requirements - Data Analysis and Visualization for Loan Application
# 5.1. Find and plot the percentage of applications approved for self-employed applicants.
def self_empl_apprvl_percentage():
    total_self_emp = loan_df.filter(loan_df.Self_Employed == 'Yes').count()
    self_emp_pd = loan_df.filter(loan_df.Self_Employed == 'Yes').groupby('Application_Status').\
        agg(round((count('Application_Status')/total_self_emp * 100),2).alias("Percentage")).toPandas()

    x = self_emp_pd.iloc[:,0].tolist()
    x = list(map(lambda x: x.replace('Y', 'Approved'), x))
    x = list(map(lambda x: x.replace('N', 'Not Approved'), x))

    self_emp_pd['Percentage'].plot(kind='pie',
                                    figsize=(10,8),
                                    explode=[0, 0.1],
                                    colors=['teal','coral'],
                                    labels=x,
                                    autopct='%1.1f%%',
                                    startangle=80,
                                    shadow=True,
                                    )

    plt.title("Self-employed applicants approval rates", y=1.12)
    plt.axis('equal')
    plt.show()

# 5.2. Find the percentage of rejection for married male applicants.
def smar_male_apprvl_percentage():
    total_married_male = loan_df.filter((loan_df.Gender == 'Male') & (loan_df.Married == 'Yes')).count()
    married_male_pd = loan_df.filter((loan_df.Gender == 'Male') & (loan_df.Married == 'Yes')).groupby('Application_Status').\
        agg(round((count('Application_Status')/total_married_male * 100),2).alias("Percentage")).toPandas()

    x = married_male_pd.iloc[:,0].tolist()
    x = list(map(lambda x: x.replace('Y', 'Approved'), x))
    x = list(map(lambda x: x.replace('N', 'Not Approved'), x))

    married_male_pd['Percentage'].plot(kind='pie',
                                    figsize=(10,8),
                                    explode=[0, 0.1],
                                    colors=['teal','coral'],
                                    labels=x,
                                    autopct='%1.1f%%',
                                    startangle=80,
                                    shadow=True,
                                    )

    plt.title("Married male applicants approval rates", y=1.12)
    plt.axis('equal')
    plt.show()

# 5.3. Find and plot the top three months with the largest transaction data.
def top_month_trans():
    n_credit_df = credit_df.withColumn('Month', credit_df['TIMEID'][0:6])
    top_mon_pd = n_credit_df.groupby('Month').agg(count(n_credit_df.TRANSACTION_VALUE).alias('Number of transactions')).sort(desc('Number of transactions')).toPandas()
    top_3mon_pd = top_mon_pd.iloc[:3,:]
    top_3mon_pd['Month'] = top_3mon_pd['Month'].str[-2:] + '/' + top_3mon_pd['Month'].str[0:4]

    # function to add value labels
    def addlabels(x,y):
        for i in range(len(x)):
            plt.text(i, y[i], y[i], ha = 'center')
    # lists of values
    x = top_3mon_pd.iloc[:,0].tolist()
    y = top_3mon_pd.iloc[:,1].tolist()

    top_3mon_pd.plot(kind='bar', x='Month', y='Number of transactions', figsize=(10,6), color='olivedrab', legend=False)
    addlabels(x, y)
    plt.title('Top 3 Months by Number of transactions')
    plt.ylabel('Number of transactions')
    plt.xticks(rotation=0)
    plt.show()

# 5.4. Find and plot which branch processed the highest total dollar value of healthcare transactions.
def top10_bran_health():
    healthcare_pd = joined_df.filter(joined_df.TRANSACTION_TYPE == 'Healthcare').groupby('BRANCH_CODE').\
    agg(round(sum('TRANSACTION_VALUE'),2).alias("Healthcare Transactions Amount")).sort(desc('Healthcare Transactions Amount')).toPandas()

    healthcare_10_pd = healthcare_pd.iloc[:11,:]

    # function to add value labels
    def addlabels(x,y):
        for i in range(len(x)):
            plt.text(i, y[i]-1000, y[i], ha = 'center', va='center', rotation = 'vertical')
    # lists of values
    x = healthcare_10_pd.iloc[:,0].tolist()
    y = healthcare_10_pd.iloc[:,1].tolist()

    healthcare_10_pd.plot(kind='bar', x='BRANCH_CODE', y='Healthcare Transactions Amount', figsize=(10,6), color='teal', legend=False)
    addlabels(x, y)
    plt.title('Top 10 Branches by Healthcare Transactions Amount')
    plt.xlabel('Branch Code')
    plt.ylabel('Healthcare Transactions Amount')
    plt.xticks(rotation=0)
    plt.show()

#################################################################################################################################################
zip_pd = customer_df.select('CUST_ZIP').distinct().toPandas()
zip_pd['CUST_ZIP'] = zip_pd['CUST_ZIP'].astype(str).str.zfill(5)
cust_zip_list = zip_pd.iloc[:,0].to_list()
months_list = ['01','02','03','04','05','06','07','08','09','10','11','12']
years_list = credit_df.withColumn('YEAR',credit_df.TIMEID.substr(1,4)).select('YEAR').distinct().toPandas().iloc[:,0].to_list()
cc_no_list = customer_df.select('CREDIT_CARD_NO').distinct().toPandas().astype(str).iloc[:,0].to_list()
#################################################################################################################################################

# Function to run Main Menu Console
def console():
    print('Main Menu')
# outer main_menu loop
    run_main_menu = True

    while run_main_menu:
        # inner 3 module menu loops
        run_t_menu = True
        run_c_menu = True
        run_v_menu = True

        # Main Menu options list
        main_menu = pyip.inputMenu(['Transaction Details Menu', 'Customer Details Menu', 'Data Analysis and Visualization Menu', 'Exit'], numbered=True)
        
        # Main Menu Option #1 - Transaction Menu Start
        if main_menu == 'Transaction Details Menu':
            print('this is the Transaction Details Menu')
            while run_t_menu:
                
                # Transaction Menu options list
                t_menu = pyip.inputMenu(['Customer Transactions by ZIP',
                                        'Transactions by Transaction Type',
                                        'Transactions of State Branch',
                                        'Back to Main Menu',
                                        'Quit'], numbered=True)
                
                # Transaction Menu Function #1
                if t_menu == 'Customer Transactions by ZIP':
                    print('This is the Customer Transactions by ZIP function')
                                        
                    run_211zip = True
                    while run_211zip:
                        run_211month = True
                        zip = input('Please enter 5-digit zip code or enter 0 to Exit: ')
                        if zip in cust_zip_list:
                            while run_211month:
                                run_211year = True
                                t1m = input('Please enter 1 or 2-digit month number or enter 0 to Exit: ').zfill(2)
                                if t1m in months_list:
                                    while run_211year:
                                        t1y = input('Please enter 4-digit year number or enter 0 to Exit: ')
                                        if t1y in years_list:
                                            print('Result for {} zip code for {} month and {} year is:'.format(zip,t1m,t1y))
                                            transactions_by_zip_my(zip, t1m, t1y)
                                            run_211year = False
                                            run_211month = False
                                        elif t1y == '0':
                                            run_211year = False
                                            run_211month = False
                                            run_211zip = False
                                        else:
                                            print('Results for {} year are not found. Please choose another year or press 0 to exit'.format(t1y))
                                elif t1m == '00':
                                    run_211month = False
                                    run_211zip = False
                                else:
                                    print('Incorrect MONTH value. Please try again or press 0 to exit')
                        elif zip == '0':
                            run_211zip = False
                        else:
                            print('ZIP Code is not found. Please try again or press 0 to exit')

                # Transaction Menu Function #2
                elif t_menu == 'Transactions by Transaction Type':
                    print('This is the Transactions by Transaction Type function')
                    tr_type_list = credit_df.select('TRANSACTION_TYPE').distinct().toPandas().iloc[:,0].to_list()
                    while True:
                        print('Choose transaction type from the following list: {}'.format(tr_type_list))
                        type = input('Please enter transaction type or enter 0 to Cancel: ')
                        if type in tr_type_list:
                            print('Result for transaction type {} is: '.format(type))
                            transactions_by_type(type)
                        elif type == '0':
                            break
                        else:
                            print('Not a valid transaction type.')
                        
                # Transaction Menu Function #3
                elif t_menu == 'Transactions of State Branch':
                    print('This is the Transactions of State Branch function')
                    states_list = branch_df.select('BRANCH_STATE').distinct().toPandas().iloc[:,0].to_list()
                    while True:
                        state = input('Please enter 2-letter state abbreviation or enter 0 to Cancel: ').upper()
                        if state in states_list:
                            print('Result for {} state branch transactions: '.format(state))
                            transactions_of_state_branches(state)
                        elif state == '0':
                            break
                        else:
                            print('Not a valid state value.')

                # Transaction Menu Option to Go Back
                elif t_menu =='Back to Main Menu':
                    run_t_menu = False

                # Transaction Menu Option to Exit    
                elif t_menu =='Quit':
                    run_t_menu = False
                    run_main_menu = False

                # Transaction Menu ELSE Option
                else:
                    print('Please make a valid selection')

        # Main Menu Option #2 - Customer Details Menu Start
        elif main_menu == 'Customer Details Menu':
            print('this is the Customer Details Menu')
            while run_c_menu:
                # Customer Details Menu options list
                c_menu = pyip.inputMenu(['Check the Account Details of the Customer',
                                        'Modify the Account Details of the Customer',
                                        'Generate a Monthly Bill by Credit Card for a given Month and Year',
                                        'Display Customer Transactions between Two Dates',
                                        'Back to Main Menu',
                                        'Quit'], numbered=True)
                
                # Customer Details Menu Function #1
                if c_menu == 'Check the Account Details of the Customer':
                    print('This is the Check the Account Details of the Customer function')
                    ssn_list = customer_df.select('SSN').distinct().toPandas().astype(str).iloc[:,0].to_list()
                    while True:
                        ssn = input('Please enter 9-digit Customer SSN to update the account details or enter 0 to Cancel: ')
                        if ssn in ssn_list:
                            print('Result - Account details by SSN: {}: '.format(ssn))
                            account_by_ssn(ssn)
                        elif ssn == '0':
                            break
                        else:
                            print('Not a valid SSN value. Please try again or enter 0 to Cancel')
                     
                # Customer Details Menu Function #2
                elif c_menu == 'Modify the Account Details of the Customer':
                    print('This is the Modify the Account Details of the Customer function')
                    ssn_list = customer_df.select('SSN').distinct().toPandas().astype(str).iloc[:,0].to_list()
                    parameters_list = ['First Name','Last Name','Middle Name','Phone','Email','Street Address','City','State','ZIP','Country','Exit to the Previous Menu']
                    run_222_menu = True
                    while run_222_menu:
                        et_ssn = input('Please enter 9-digit Customer SSN to update the account details or enter 0 to Cancel: ')
                        if et_ssn in ssn_list:
                            ssn_data = customer_df.filter(customer_df.SSN==et_ssn).toPandas()

                            run_ssn_menu = True
                            while run_ssn_menu:
                                #print('Please choose the parameter to update:')
                                c2_menu = pyip.inputMenu(parameters_list, numbered=True, prompt='Please choose the parameter to update: \n')

                                run_param_menu = True
                                while run_param_menu:
                                
                                    if c2_menu == 'First Name':
                                        print('Current First Name: ' + ssn_data.loc[0,'FIRST_NAME'])
                                        n_f_name = input('Please enter a new value to update the First Name or enter 0 to Cancel: ').capitalize()
                                        if n_f_name == '0':
                                            run_param_menu = False
                                        elif all(c.isalpha() or c == '-' or c == ' ' for c in n_f_name):
                                            run_yn = True
                                            while run_yn:                                                
                                                yesorno = input('Please enter Y to confirm edit or N to cancel: ')
                                                if yesorno.upper() == 'Y':
                                                        ssn_data.loc[0,'FIRST_NAME'] = n_f_name                                              
                                                        update_customer(ssn_data)
                                                        run_yn = False
                                                        run_param_menu = False
                                                elif yesorno.upper() == 'N':
                                                    run_yn = False
                                                    run_param_menu = False
                                                else:
                                                    print('Invalid Y/N selection')
                                        else:
                                            print("Invalid input. Please enter only alphabetical characters, spaces, and dashes.")

                                    elif c2_menu == 'Last Name':
                                        print('Current Last Name: ' + ssn_data.loc[0,'LAST_NAME'])
                                        n_l_name = input('Please enter a new value to update the Last Name or enter 0 to Cancel: ').capitalize()
                                        if n_l_name == '0':
                                            run_param_menu = False
                                        elif all(c.isalpha() or c == '-' or c == ' ' for c in n_l_name):
                                            run_yn = True
                                            while run_yn:                                                
                                                yesorno = input('Please enter Y to confirm edit or N to cancel: ')
                                                if yesorno.upper() == 'Y':
                                                        ssn_data.loc[0,'LAST_NAME'] = n_l_name                                              
                                                        update_customer(ssn_data)
                                                        run_yn = False
                                                        run_param_menu = False
                                                elif yesorno.upper() == 'N':
                                                    run_yn = False
                                                    run_param_menu = False
                                                else:
                                                    print('Invalid Y/N selection')
                                        else:
                                            print("Invalid input. Please enter only alphabetical characters, spaces, and dashes.")

                                    elif c2_menu == 'Middle Name':
                                        print('Current Middle Name: ' + ssn_data.loc[0,'MIDDLE_NAME'])
                                        n_m_name = input('Please enter a new value to update the Middle Name or enter 0 to Cancel: ').lower()
                                        if n_m_name == '0':
                                            run_param_menu = False
                                        elif all(c.isalpha() or c == '-' or c == ' ' for c in n_m_name):
                                            run_yn = True
                                            while run_yn:                                                
                                                yesorno = input('Please enter Y to confirm edit or N to cancel: ')
                                                if yesorno.upper() == 'Y':
                                                        ssn_data.loc[0,'MIDDLE_NAME'] = n_m_name                                              
                                                        update_customer(ssn_data)
                                                        run_yn = False
                                                        run_param_menu = False
                                                elif yesorno.upper() == 'N':
                                                    run_yn = False
                                                    run_param_menu = False
                                                else:
                                                    print('Invalid Y/N selection')
                                        else:
                                            print("Invalid input. Please enter only alphabetical characters, spaces, and dashes.")

                                    elif c2_menu == 'Phone':
                                        print('Current Phone: ' + ssn_data.loc[0,'CUST_PHONE'])
                                        n_phone = input('Please enter a new 10-digit Phone number to update or enter 0 to Cancel: ')

                                        if n_phone == '0':
                                            run_param_menu = False

                                        elif ((len(n_phone) == 10) & (n_phone.isnumeric())):
                                            run_yn = True
                                            while run_yn:                                                
                                                yesorno = input('Please enter Y to confirm edit or N to cancel: ')
                                                if yesorno.upper() == 'Y':
                                                        ssn_data.loc[0,'CUST_PHONE'] = '('+ n_phone[:3] +')'+n_phone[3:6]+'-'+n_phone[6:11]                                             
                                                        update_customer(ssn_data)
                                                        run_yn = False
                                                        run_param_menu = False
                                                elif yesorno.upper() == 'N':
                                                    run_yn = False
                                                    run_param_menu = False
                                                else:
                                                    print('Invalid Y/N selection')
                                        else:
                                            print("Invalid input. Please try again.")              
                                        
                                    elif c2_menu == 'Email':
                                        print('Current Email: ' + ssn_data.loc[0,'CUST_EMAIL'])
                                        n_email = input('Please enter a new value to update the Email or enter 0 to Cancel: ')
                                        if n_email == '0':
                                            run_param_menu = False
                                        elif re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', n_email):
                                            run_yn = True
                                            while run_yn:                                                
                                                yesorno = input('Please enter Y to confirm edit or N to cancel: ')
                                                if yesorno.upper() == 'Y':
                                                        ssn_data.loc[0,'CUST_EMAIL'] = n_email                                              
                                                        update_customer(ssn_data)
                                                        run_yn = False
                                                        run_param_menu = False
                                                elif yesorno.upper() == 'N':
                                                    run_yn = False
                                                    run_param_menu = False
                                                else:
                                                    print('Invalid Y/N selection')
                                        else:
                                            print("Invalid email format. Please enter email address in 'username@domain.com' format.")

                                    elif c2_menu == 'Street Address':
                                        print('Current Street Address: ' + ssn_data.loc[0,'FULL_STREET_ADDRESS'])
                                        n_street = input('Please enter a new value to update the Street Address or enter 0 to Cancel: ')
                                        if n_street == '0':
                                            run_param_menu = False
                                        elif re.match(r'^[\w\s\-\.,]+$', n_street):
                                            run_yn = True
                                            while run_yn:                                                
                                                yesorno = input('Please enter Y to confirm edit or N to cancel: ')
                                                if yesorno.upper() == 'Y':
                                                        ssn_data.loc[0,'FULL_STREET_ADDRESS'] = n_street                                              
                                                        update_customer(ssn_data)
                                                        run_yn = False
                                                        run_param_menu = False
                                                elif yesorno.upper() == 'N':
                                                    run_yn = False
                                                    run_param_menu = False
                                                else:
                                                    print('Invalid Y/N selection')
                                        else:
                                            print("Invalid address format. Please try again.")

                                    elif c2_menu == 'City':
                                        print('Current City: ' + ssn_data.loc[0,'CUST_CITY'])
                                        n_city = input('Please enter a new value to update the City or enter 0 to Cancel: ').capitalize()
                                        if n_city == '0':
                                            run_param_menu = False
                                        elif all(c.isalpha() or c == '-' or c == ' ' for c in n_city):
                                            run_yn = True
                                            while run_yn:                                                
                                                yesorno = input('Please enter Y to confirm edit or N to cancel: ')
                                                if yesorno.upper() == 'Y':
                                                        ssn_data.loc[0,'CUST_CITY'] = n_city                                              
                                                        update_customer(ssn_data)
                                                        run_yn = False
                                                        run_param_menu = False
                                                elif yesorno.upper() == 'N':
                                                    run_yn = False
                                                    run_param_menu = False
                                                else:
                                                    print('Invalid Y/N selection')
                                        else:
                                            print("Invalid input. Please enter only alphabetical characters, spaces, and dashes.")

                                    elif c2_menu == 'State':
                                        print('Current State: ' + ssn_data.loc[0,'CUST_STATE'])
                                        n_state = input('Please enter new 2-letter State abbreviation to update or enter 0 to Cancel: ').upper()
                                        if n_state == '0':
                                            run_param_menu = False
                                        elif ((len(n_state) == 2) & (n_state.isalpha())):
                                            run_yn = True
                                            while run_yn:                                                
                                                yesorno = input('Please enter Y to confirm edit or N to cancel: ')
                                                if yesorno.upper() == 'Y':
                                                        ssn_data.loc[0,'CUST_STATE'] = n_state                                              
                                                        update_customer(ssn_data)
                                                        run_yn = False
                                                        run_param_menu = False
                                                elif yesorno.upper() == 'N':
                                                    run_yn = False
                                                    run_param_menu = False
                                                else:
                                                    print('Invalid Y/N selection')
                                        else:
                                            print("Invalid input. Please try again.")

                                    elif c2_menu == 'ZIP':
                                        print('Current ZIP: ' + str(ssn_data.loc[0,'CUST_ZIP']))
                                        n_zip = input('Please enter a new 5-digit ZIP code to update or enter 0 to Cancel: ')
                                        if n_zip == '0':
                                            run_param_menu = False
                                        elif ((len(n_zip) == 5) & (n_zip.isnumeric())):
                                            run_yn = True
                                            while run_yn:                                                
                                                yesorno = input('Please enter Y to confirm edit or N to cancel: ')
                                                if yesorno.upper() == 'Y':
                                                        ssn_data.loc[0,'CUST_ZIP'] = n_zip                                              
                                                        update_customer(ssn_data)
                                                        run_yn = False
                                                        run_param_menu = False
                                                elif yesorno.upper() == 'N':
                                                    run_yn = False
                                                    run_param_menu = False
                                                else:
                                                    print('Invalid Y/N selection')
                                        else:
                                            print("Invalid input. Please try again.")

                                    elif c2_menu == 'Country':
                                        print('Current Country: ' + ssn_data.loc[0,'CUST_COUNTRY'])
                                        n_country = input('Please enter a new value to update the Country or enter 0 to Cancel: ').capitalize()
                                        if n_country == '0':
                                            run_param_menu = False
                                        elif all(c.isalpha() or c == '-' or c == ' ' for c in n_country):
                                            run_yn = True
                                            while run_yn:                                                
                                                yesorno = input('Please enter Y to confirm edit or N to cancel: ')
                                                if yesorno.upper() == 'Y':
                                                        ssn_data.loc[0,'CUST_COUNTRY'] = n_country                                              
                                                        update_customer(ssn_data)
                                                        run_yn = False
                                                        run_param_menu = False
                                                elif yesorno.upper() == 'N':
                                                    run_yn = False
                                                    run_param_menu = False
                                                else:
                                                    print('Invalid Y/N selection')
                                        else:
                                            print("Invalid input. Please enter only alphabetical characters, spaces, and dashes.")

                                    elif c2_menu == 'Exit to the Previous Menu':
                                        run_param_menu = False
                                        run_ssn_menu = False
                                        run_222_menu = False
            
                                    else:
                                        print('Please make a valid selection')

                        elif et_ssn == '0':
                            run_ssn_menu = False
                            run_222_menu = False

                        else:
                            print('Not a valid SSN value. Please try again.')

                # Customer Details Menu Function #3
                elif c_menu == 'Generate a Monthly Bill by Credit Card for a given Month and Year':
                    print('This is the Generate a Monthly Bill by Credit Card for a given Month and Year function')
                    run_223cc = True
                    while run_223cc:
                        run_223month = True
                        cc_no = input('Please enter 16-digit Credit Card Number or enter 0 to Cancel: ')
                        if cc_no in cc_no_list:
                            while run_223month:
                                run_223year = True
                                t3m = input('Please enter 1 or 2-digit month number or enter 0 to Cancel: ').zfill(2)
                                if t3m in months_list:
                                    while run_223year:
                                        t3y = input('Please enter 4-digit year number or enter 0 to Cancel: ')
                                        if t3y in years_list:
                                            print('Result for Credit Card #{} for {} month and {} year is:'.format(cc_no,t3m,t3y))
                                            monthly_bill(cc_no, t3m, t3y)
                                            run_223year = False
                                            run_223month = False
                                        elif t3y == '0':
                                            run_223year = False
                                            run_223month = False
                                            run_223cc = False
                                        else:
                                            print('Results for {} year are not found. Please choose another year'.format(t3y))
                                elif t3m == '00':
                                    run_223month = False
                                    run_223cc = False
                                else:
                                    print('Incorrect MONTH value.')
                        elif cc_no == '0':
                            run_223cc = False
                        else:
                            print('Credit Card # is not found.')

                # Customer Details Menu Function #4
                elif c_menu == 'Display Customer Transactions between Two Dates':
                    print('This is the Display Customer Transactions between Two Dates function')
                    run_224cc = True
                    while run_224cc:

                        cc_no = input('Please enter 16-digit Credit Card Number or enter 0 to Cancel: ')
                        if cc_no in cc_no_list:

                            run_s_date = True
                            while run_s_date:
                                
                                inp_s_date = input('Please enter the start date in YYYY-MM-DD format or enter 0 to Cancel: ')
                                if validate_date(inp_s_date):
                                    
                                    run_e_date = True
                                    while run_e_date:

                                        inp_e_date = input('Please enter the end date in YYYY-MM-DD format or enter 0 to Cancel: ')
                                        if validate_date(inp_e_date):

                                            if inp_s_date <= inp_e_date:
                                                print('Result for {}-{} period for {} CC No is : '.format(inp_s_date,inp_e_date,cc_no))
                                                transactions_between_dates(cc_no, inp_s_date, inp_e_date)
                                                run_e_date = False
                                                run_s_date = False
                                            else:
                                                print("Start date can't be more than End date")
                                                run_e_date = False

                                        elif inp_e_date == '0':
                                            run_e_date = False
                                            run_s_date = False
                                            run_224cc = False
                                        else:
                                            print('Invalid date format.')
                                elif inp_s_date == '0':
                                    run_s_date = False
                                    run_224cc = False
                                else:
                                    print('Invalid date format.')
                        elif cc_no == '0':
                            run_224cc = False
                        else:
                            print('Credit Card # is not found.')
                    
                # Customer Details Menu Option to Go Back
                elif c_menu == 'Back to Main Menu':
                    run_c_menu = False

                # Customer Details Menu Option to Exit
                elif c_menu =='Quit':
                    run_c_menu = False
                    run_main_menu = False

                # Customer Details Menu ELSE Option
                else:
                    print('Please make a valid selection')

        # Main Menu Option #3 - Data Analysis and Visualization Menu Start
        elif main_menu == 'Data Analysis and Visualization Menu':
            print('This is the Data Analysis and Visualization Menu')
            while run_v_menu:
                v_menu = pyip.inputMenu(['Transaction type with a high rate of transactions plot', #--- 3.1
                                        'State with a high number of customers plot', #--- 3.2
                                        'Customer with the highest transaction amount plot (all customers)', #--- 3.3
                                        'Approved applications for self-employed applicants percentage plot', #--- 5.1
                                        'Rejection for married male applicants percentage plot', #--- 5.2
                                        'Top 3 months with the largest transaction data plot', #--- 5.3
                                        'Branch with the highest total value of healthcare transactions plot', #--- 5.4
                                        'Back to Main Menu',
                                        'Quit'], numbered=True)
                
                #--- 3.1
                if v_menu == 'Transaction type with a high rate of transactions plot': 
                    plot_high_rate_trans_avg()
                #--- 3.2
                elif v_menu == 'State with a high number of customers plot':
                    plot_numb_cust_state()
                #--- 3.3
                elif v_menu == 'Customer with the highest transaction amount plot (all customers)':
                    plot_top_cust_by_trans_2()
                #--- 5.1        
                elif v_menu == 'Approved applications for self-employed applicants percentage plot':
                    self_empl_apprvl_percentage()
                #--- 5.2
                elif v_menu == 'Rejection for married male applicants percentage plot':
                    smar_male_apprvl_percentage()
                #--- 5.3
                elif v_menu == 'Top 3 months with the largest transaction data plot':
                    top_month_trans()
                #--- 5.4
                elif v_menu == 'Branch with the highest total value of healthcare transactions plot':
                    top10_bran_health()

                elif v_menu == 'Back to Main Menu':
                    run_v_menu = False
                elif v_menu =='Quit':
                    run_v_menu = False
                    run_main_menu = False
                else:
                    print('Please make a valid selection')

        # Main Menu Option #4 - Exit
        elif main_menu == 'Exit':
            run_main_menu = False
        
        # Main Menu ELSE Option
        else:
            print('Please make a valid selection')

console()