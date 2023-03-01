# DE_Capstone_Project

ETL and Console Program
"ETL_final.py" file is a Python-based ETL (Extract, Transform, Load) program that allows users to extract data from a data source, transform it, and load it into a MariaDB HeidiSQL Database.
"Functions & Console Menu_final.py" is a Python-based console program that allows users to make data queries based on interacting with a program console menu. 

Features
Extract data from a JSON data sources using pyspark.sql SparkSession and API Endpoint JSON file using requests and json.loads.
Transform data using custom Python pandas and pyspark functions based on the requrements from "Mapping Document.xlsx".
Creating connection via pymysql connector.
Creating a new database nad load transformed data into a MariaDB HeidiSQL created database.

Run the "Functions & Console Menu_final.py" console program.
The program reads loaded database via pyspark.sql SparkSession to make data queries.

Here's the Console Menu Skeleton:
Main Menu:
1. Transaction Details Menu \n
    1.1. Customer Transactions by ZIP \n
        inputs: zip, month, year
    1.2. Transactions by Transaction Type
        inputs: type
    1.3. Transactions of Branches by State
        inputs: state
    1.4. Back to Main Menu
    1.5. Quit
2. Customer Details Menu
    2.1. Check the Account Details of the Customer
        inputs: ssn
    2.2. Modify the Account Details of the Customer
        inputs: ssn
        Choose the parameter to update Menu:
        2.2.1. First Name
            inputs: first_name
            Confirm Y/N Menu:
                inputs: y, n
        2.2.2. Last Name
            inputs: last_name
            Confirm Y/N Menu:
                inputs: y, n
        2.2.3. Middle Name
            inputs: middle_name
            Confirm Y/N Menu:
                inputs: y, n
        2.2.4. Phone
            inputs: phone
            Confirm Y/N Menu:
                inputs: y, n
        2.2.5. Email
            inputs: emaile
            Confirm Y/N Menu:
                inputs: y, n
        2.2.6. Street Address
            inputs: street_address
            Confirm Y/N Menu:
                inputs: y, n
        2.2.7. City
            inputs: city
            Confirm Y/N Menu:
                inputs: y, n
        2.2.8. State
            inputs: state
            Confirm Y/N Menu:
                inputs: y, n
        2.2.9. ZIP
            inputs: zip
            Confirm Y/N Menu:
                inputs: y, n
        2.2.10. Country
            inputs: country
            Confirm Y/N Menu:
                inputs: y, n
        2.2.11. Exit to the Previous Menu
    2.3. Generate a Monthly Bill by Credit Card for a given Month and Year
        inputs: credit_card_no, month, year
    2.4. Display Customer Transactions between Two Dates
        inputs: credit_card_no, start_date, end_date
    2.5. Back to Main Menu
    2.6. Quit
3. Data Analysis and Visualization Menu
    3.1. Transaction type with a high rate of transactions plot
    3.2. State with a high number of customers plot
    3.3. Customer with the highest transaction amount plot (all customers)
    3.4. Approved applications for self-employed applicants percentage plot
    3.5. Rejection for married male applicants percentage plot
    3.6. Top 3 months with the largest transaction data plot
    3.7. Branch with the highest total value of healthcare transactions plot
    3.8. Back to Main Menu
    3.9. Quit
4. Exit


To use this program, follow these steps:
1) Clone the repository to your local machine using git clone https://github.com/<your-username>/etl-console.git
2) Install the required dependencies by running pip install -r requirements.txt
3) Run the program using python

Contributing
Contributions to this project are welcome. To contribute, follow these steps:
1) Fork the repository
2) Create a new branch for your feature or bug fix
3) Make your changes and write tests if applicable
4) Submit a pull request

Contact
If you have any questions or comments, please contact me at limonogon@gmail.com

Credits
Thanks to the Per Scholas Institution for providing the opportunity and makes dreams come true with their Data Engineering Intense Course Program.
Special thanks to Samantha Grzegorzewski and Benjamin Witter.
