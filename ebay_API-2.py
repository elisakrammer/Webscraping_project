""" This document is using requests and BeautifulSoup to get data from Ebay API
It has been developed with Pycharm
Authors : Elisa Krammer and Dan Slama """

import json
import requests
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import pymysql

pymysql.install_as_MySQLdb()


def api_request(list_of_keywords):
    list_of_results = []
    # Build a GET HTTP request with the requests module
    for keyword in list_of_keywords:
        response = requests.get(
            "http://svcs.ebay.com/services/search/FindingService/v1?OPERATION-NAME=findItemsByKeywords&SERVICE-VERSION=1.0.0&SECURITY-APPNAME=ElisaKra-ElisaKra-PRD-dc22b8256-9ed27192&RESPONSE-DATA-FORMAT=JSON&itemFilter(0).name=LocatedIn\&itemFilter(0).value=IL&keywords=" + keyword)
        result = response.json()
        result = json.loads(response.content.decode("utf-8"))
        list_of_results.append(result)
    return list_of_results


def build_dataframe(list_of_results):
    list_of_dataframes = []
    # Build a dataframe with the Price, Category, Reference and Time fopr each product specified in the keyword parameter from the api_request
    for result in list_of_results:
        category = []
        price = []
        time = []
        reference = []
        for item in (result["findItemsByKeywordsResponse"][0]["searchResult"][0]["item"]):
            for i in item["itemId"]:
                if i == "" or i == None:
                    reference.append("NA")
                else:
                    reference.append(i)

            if item["primaryCategory"][0]["categoryName"] == "" or item["primaryCategory"][0]["categoryName"] == None:
                category.append("NA")
            else:
                for i in item["primaryCategory"][0]["categoryName"]:
                    category.append(i)

            if item["sellingStatus"][0]["convertedCurrentPrice"][0]["__value__"] == "" or \
                    item["sellingStatus"][0]["convertedCurrentPrice"][0]["__value__"] == None:
                price.append("NA")
            else:
                price.append(item["sellingStatus"][0]["convertedCurrentPrice"][0]["__value__"])
            for i in result["findItemsByKeywordsResponse"][0]["timestamp"]:
                date = datetime.strptime(i[:-5].replace("T", " "), '%Y-%m-%d %H:%M:%S')
                time.append(date.strftime('%Y-%m-%d %H:%M:%S'))

        data = pd.DataFrame({"Reference": reference, "Category": category, "Price": price, "Time": time})
        list_of_dataframes.append(data)
    return pd.concat(list_of_dataframes)


def table_sql_Ebay(dataframe):
    # Connect to the database
    cnx = pymysql.connect(host='localhost',
                          user='root',
                          password='ITC2018O')
    # Create a cursor object
    cursorObject = cnx.cursor()

    query_database = "CREATE DATABASE IF NOT EXISTS EBAY;"

    # Execute the sqlQuery
    cursorObject.execute(query_database)

    # Use the database
    use = "USE EBAY"

    # run the query_use
    cursorObject.execute(use)

    # Create our Table Zabilo_Price in under to follow the evolution of price and stock
    query_table_products = "CREATE TABLE IF NOT EXISTS ebay_price (ID int(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY (ID), Reference BIGINT(255) , Category CHAR(250), Price INT(50), Time DATETIME(6));"

    # Execute the sqlQuery
    cursorObject.execute(query_table_products)

    # SQL query to insert values in the columns of Zabilo_Price table
    engine = create_engine("mysql://root:ITC2018O@localhost/EBAY")
    dataframe.to_sql('ebay_price', con=engine, if_exists='append', index=1)


def main():
    list_of_keywords = ['Washing Machine', 'Dryer', 'dishwashers', 'refrigerators', 'ovens', 'cooktops',
                        'washing-machines', 'tumble-dryers', 'vacuums', 'televisions', 'air-conditioners']
    list_of_result = api_request(list_of_keywords)
    concat_dataframe = build_dataframe(list_of_result)
    table_sql_Ebay(concat_dataframe)


if __name__ == '__main__':
    main()
