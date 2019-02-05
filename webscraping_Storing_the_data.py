""" This document is using requests and BeautifulSoup to scrape pages from Zabilo website
It has been developed with Pycharm
Authors : Elisa Krammer and Dan Slama """

# Import of packages
import requests
from bs4 import BeautifulSoup
import pandas as pd
import click
import pymysql
from pymysql import connect
import datetime


def start(url_home_page):
    # This funtion allows to scrape the url of "Today's Hot Deal"
    url_home_page = requests.get(url_home_page)
    content_of_home = url_home_page.content
    soup_of_home = BeautifulSoup(content_of_home, 'html.parser')
    link_of_deals = [i['href'] for i in soup_of_home.select('a[class*="navdeals"]')]
    return link_of_deals


def deal_pages(url_deal):
    # This function allows to scrape the url of the different categories of product
    url_deal = requests.get(url_deal[0])
    content_deal = url_deal.content
    soup_of_deal = BeautifulSoup(content_deal, 'html.parser')
    link_of_category_of_product =[]
    for category in soup_of_deal.select('[class="landing_categories_little"]'):
        for http in category.select('a'):
            link_of_category_of_product.append(http['href'])
    return link_of_category_of_product


def link_of_product(list_of_url):
    # We scrape the link of product pages and outputs a list of product urls
    list_of_product = []
    for url in list_of_url:
        url_product_machine = requests.get(url)
        content_product = url_product_machine.content
        soup_of_product = BeautifulSoup(content_product, 'html.parser')
        for category in soup_of_product.select('a[class*="product-name"]'):
            list_of_product.append(category['href'])
    return list_of_product


def get_product_characteristics(list_url):
    # This function allows to get the characteristics for each url of product and outputs a list of dictionaries.
    # Each of these dictionaries corresponds to the characteristics of one product: price, availability, category etc
    list_product = []
    keys = []
    values = []
    for url in list_url:
        char_dict = {}
        r = requests.get(url)
        c = r.content
        soup = BeautifulSoup(c, 'html.parser')

        reviews = soup.find_all("strong", {"class": "comment_title"})
        reviews = [i.get_text() for i in reviews]
        reviews = str(reviews[0:len(reviews)//2])

        stock = soup.find('span', {'class': 'stock'})
        stock = stock.get_text()

        ref = soup.find('span', {'class': 'editable'})
        ref = ref.get_text()

        characteristics = []
        table = soup.find_all("table", {"class": "table-data-sheet data_sheet"})[0]
        for row in table.find_all('tr'):
            element = row.find_all('td')
            for td in element:
                characteristics.append(td.text)

            for i in range(0, len(characteristics)):
                if i % 2 == 0:
                    keys.append(characteristics[i])
                else:
                    values.append(characteristics[i])

            keys = keys[0:14]
            values = values[0:14]

        if soup.find('span', attrs={'class': 'reviewCount'}):
            main_ratings = soup.find('span', attrs={'class': 'reviewCount'})
            ratings = main_ratings.get_text()
            ratings = int(ratings[1])
        else :
            ratings='No ratings'
        keys.append('Ratings')
        values.append(ratings)

        # add reference
        keys.append('Reference')
        values.append(ref)
        # add price and reviews
        keys.append('Price')
        price = soup.find('span', attrs = {'itemprop':'price'}).get_text()
        price =price.replace(',','')
        values.append(float(price))

        keys.append('Old Price')
        old_price = soup.find('span', attrs = {'id':'old_price_display'}).get_text()
        old_price = old_price.replace(',','')
        old_price = old_price[1:old_price.index('t')]
        values.append(float(old_price))

        keys.append('Reviews')
        values.append(reviews)
        # add date of scraping
        keys.append('Date')
        dt= datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        values.append(dt)
        #Add stock
        keys.append('Stock')
        values.append(stock)
        # # Add ratings
        # keys.append('Ratings')
        # values.append(ratings)
        char_dict = {k: v for k, v in zip(keys, values)}
        list_product.append(char_dict)

        # we filter the dictionary to get only the Prduct, the Price, the Old Price and the Date
        d=[]
        for i in range(0,len(list_product)):
            dictfilt = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])
            wanted_keys = ('Product','Reference','Price','Old Price','Stock','Date')
            new_price_dict = dictfilt(list_product[i], wanted_keys)
            d.append(new_price_dict)

        # we filter the dictionary to get only the reviews and the ratings for each product
        d_bis=[]
        for i in range(0,len(list_product)):
            dictfilt = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])
            wanted_keys = ('Product','Reference','Reviews','Ratings','Date')
            new_price_dict = dictfilt(list_product[i], wanted_keys)
            d_bis.append(new_price_dict)
    return d, d_bis


def import_to_sql_database(list_of_dictionnary):
    # Connect to the database
    cnx = pymysql.connect(host='localhost',
                          user='root',
                          password='ITC2018O')
    # Create a cursor object
    cursorObject = cnx.cursor()

    query_database = "CREATE DATABASE IF NOT EXISTS ZABILO;"

    # Execute the sqlQuery
    cursorObject.execute(query_database)

    # Use the database
    use = "USE ZABILO"

    # Run the query_use
    cursorObject.execute(use)

    # Create our Table Zabilo_Price in under to follow the evolution of price and stock
    query_table_products = "CREATE TABLE IF NOT EXISTS ZABILO_PRICE (Reference CHAR(100) PRIMARY KEY NOT NULL,Product CHAR(100),Price INT,OldPrice INT, Stock CHAR(250), Date DATETIME(6));"

    # Execute the sqlQuery
    cursorObject.execute(query_table_products)

    # SQL query to insert values in the columns of Zabilo_Price table
    query ='INSERT INTO ZABILO_PRICE (Reference, Product, Price, OldPrice, Stock, Date) VALUES (%(Reference)s, %(Product)s, %(Price)s, %(Old Price)s ,%(Stock)s ,%(Date)s) ON DUPLICATE KEY UPDATE Price = (Price) ;'
    cursorObject.executemany(query,list_of_dictionnary)
    cnx.commit()

     # Update our Database if the price is changing from the last scrapping
    query_update =" UPDATE ZABILO_PRICE SET Reference = %(Reference)s, Price =%(Price)s, OldPrice = %(Old Price)s, Stock=%(Stock)s, Date = %(Date)s WHERE Reference = %(Reference)s AND (Price <> %(Price)s OR OldPrice <> %(Old Price)s OR Stock <> %(Stock)s) ;"
    cursorObject.executemany(query_update,list_of_dictionnary)
    cnx.commit()


def import_to_sql_database_reviews_ratings(list_of_dictionnary):
    # Connect to the database
    cnx = pymysql.connect(host='localhost',
                          user='root',
                          password='ITC2018O')
    # Create a cursor object
    cursorObject = cnx.cursor()

    # Use the database
    use = "USE ZABILO"

    # run the query_use
    cursorObject.execute(use)

    # set our tables to utf8
    query_utf8 = "SET NAMES 'utf8';"
    cursorObject.execute(query_utf8)

    # Create our Table Zabilo_Price in under to follow the evolution of price and stock
    query_table_products = "CREATE TABLE IF NOT EXISTS zabilo_reviews (Reference CHAR(100) PRIMARY KEY NOT NULL,Product CHAR(100),Ratings CHAR(250) ,Reviews CHAR(250), Date DATETIME(6));"

    # Execute the sqlQuery
    cursorObject.execute(query_table_products)

    # SQL query to insert values in the columns of Zabilo_Price table
    query ="INSERT INTO zabilo_reviews (Reference, Product, Ratings, Reviews, Date) VALUES (%(Reference)s, %(Product)s, %(Ratings)s, %(Reviews)s, %(Date)s) ON DUPLICATE KEY UPDATE Reference = (Reference) ;"
    cursorObject.executemany(query,list_of_dictionnary)
    cnx.commit()

     # Update our Database if the price is changing from the last scrapping
    query_update ="UPDATE zabilo_reviews SET Reference = %(Reference)s, Ratings = %(Ratings)s, Reviews = %(Reviews)s, Date = %(Date)s WHERE Reference = %(Reference)s AND (Reviews <> %(Reviews)s OR (Ratings <> %(Ratings)s));"
    cursorObject.executemany(query_update,list_of_dictionnary)
    cnx.commit()


def main():
    url_home_page = "https://www.zabilo.com/en/"
    # We scrape the link of deal pages
    start(url_home_page)
    # We scrape the url for each category of product in deal pages
    deal_pages(start(url_home_page))
    # We scrape the link of every product for each catgegory
    link_of_product(deal_pages(start(url_home_page)))
    # We try our scrqping code on a subset of urls that are the following
    urls = ["https://www.zabilo.com/en/top-loading-washing-machines/2629-constructa-top-loading-washer-6kg-1000rpm-cwt10r16il.html","https://www.zabilo.com/en/front-loading-washing-machines/2423-washing-machine-haier-hw80-1203-8kg-1200-rpm.html","https://www.zabilo.com/en/top-air-conditionner/3196-electra-air-conditioner-125-hp-12300-btu-platinum-140.html","https://www.zabilo.com/en/tvs-55/2308-fujicom-smart-tv-55-inches-4k-ultra-hd-fj554k.html"]
    # We scrape all informations for each Product
    print(get_product_characteristics(urls))
    # We create our Zabilo Database and a table zabilo_price prices and availability of our products
    import_to_sql_database(get_product_characteristics(urls)[0])
    # We create a tabla zabilo_reviews to keep track of the Reviews and ratings of our products
    import_to_sql_database_reviews_ratings(get_product_characteristics(urls)[1])


if __name__ == '__main__':
    main()
