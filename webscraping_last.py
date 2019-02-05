""" This document is using requests and BeautifulSoup to scrape pages from Zabilo website
It has been developed with Pycharm
Authors : Elisa Krammer and Dan Slama """

# Import of packages
import requests
from bs4 import BeautifulSoup
import click
import pymysql
import datetime
import logging

url_home_page = "https://www.zabilo.com/en/"


def start(url_home_page):
    # This function allows to scrape the url of "Today's Hot Deal"
    logging.info('Home url scrapped')
    url_home_page = requests.get(url_home_page)
    content_of_home = url_home_page.content
    soup_of_home = BeautifulSoup(content_of_home, 'html.parser')
    link_of_deals = [i['href'] for i in soup_of_home.select('a[class*="navdeals"]')]
    logging.info("GET {}".format(link_of_deals))
    return link_of_deals


def deal_pages(url_deal):
    # This function allows to scrape the url of the different categories of product
    logging.debug('Debug for each category')
    url_deal = requests.get(url_deal[0])
    content_deal = url_deal.content
    soup_of_deal = BeautifulSoup(content_deal, 'html.parser')
    link_of_category_of_product = []
    select_of_category_product = []
    num_of_url = ["104", "36", "43", "35", "96", "276", "81", "92"]
    for category in soup_of_deal.select('[class*="desktop"]'):
        for http in category.select('a'):
            link_of_category_of_product.append(http['href'])
    for link in link_of_category_of_product:
        for j in num_of_url:
            if link.find(j) != -1:
                select_of_category_product.append(link)
    select_of_category_product = select_of_category_product[1:]
    logging.info("GET {}".format(select_of_category_product))
    return select_of_category_product


def link_of_product(list_of_url, category):
    # We scrape the link of product pages and outputs a list of product urls
    logging.debug('Debug for each product')
    list_of_product = []
    dic = {"dishwashers": "104", "portable_dishwashers": "296", "refrigerators": "35", "ovens": "96", "cooktops": "81",
           "washing_machines": "36", "tumble_dryers": "43", "vacuums": "92", "televisions": "32",
           "air_conditioners": "276"}
    if category == "All":
        for url in list_of_url:
            url_product_machine = requests.get(url)
            content_product = url_product_machine.content
            soup_of_product = BeautifulSoup(content_product, 'html.parser')
            for category in soup_of_product.select('a[class*="product-name"]'):
                list_of_product.append(category['href'])
    else:
        for i in dic.keys():
            if category == i:
                for url in list_of_url:
                    if url.find(dic.get(i)) != -1:
                        url_product_machine = requests.get(url)
                        content_product = url_product_machine.content
                        soup_of_product = BeautifulSoup(content_product, 'html.parser')
                        for category in soup_of_product.select('a[class*="product-name"]'):
                            list_of_product.append(category['href'])
    logging.info("GET {}".format(list_of_product))
    return list_of_product


def scrape_website_table(url):
    # This function scrape for each product a table of characteristics
    logging.debug('Debug for each product table characteristics')
    keys = []
    values = []
    url_request = requests.get(url)
    content = url_request.content
    soup = BeautifulSoup(content, 'html.parser')
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
    return keys[0], values[0]


def get_product_characteristics(list_url):
    # This function allows to get the characteristics for each url of product and outputs a list of dictionaries.
    # Each of these dictionaries corresponds to the characteristics of one product: price, availability, category etc
    LIMIT = 20
    list_product = []
    # values = []
    for url in list_url:
        values = []
        char_dict = {}
        r = requests.get(url)
        c = r.content
        soup = BeautifulSoup(c, 'html.parser')

        reviews = soup.find_all("strong", {"class": "comment_title"})
        reviews = [i.get_text() for i in reviews]
        reviews = str(reviews[0:len(reviews) // 2])
        reviews = reviews[:LIMIT]
        if reviews == '[]':
            reviews = 'No Reviews'
        else:
            reviews = reviews[:LIMIT]
        values.append(reviews)

        if soup.find('span', {'class': 'stock'}):
            stock = soup.find('span', {'class': 'stock'}).get_text()
        else:
            stock = 'No stock'
        values.append(stock)

        ref = soup.find('span', {'class': 'editable'}).get_text()
        values.append(ref)

        if soup.find('span', attrs={'class': 'reviewCount'}):
            main_ratings = soup.find('span', attrs={'class': 'reviewCount'})
            ratings = main_ratings.get_text()
            ratings = int(ratings[1])
        else:
            ratings = 'No ratings'
        values.append(ratings)

        if soup.find('span', attrs={'itemprop': 'price'}):
            price = soup.find('span', attrs={'itemprop': 'price'}).get_text()
            price = price.replace(',', '')

        else:
            price = 0
        values.append(float(price))

        if soup.find('span', attrs={'id': 'old_price_display'}):
            old_price = soup.find('span', attrs={'id': 'old_price_display'}).get_text()
            old_price = old_price.replace(',', '')
            try:
                old_price = old_price[1:old_price.index('t')]
            except ValueError:
                old_price = 0
        else:
            old_price = 0
        values.append(old_price)
        values.append(datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
        values.append(scrape_website_table(url)[1])
        keys = ['Reviews', 'Stock', 'Reference', 'Ratings', 'Price', 'Old Price', 'Date', 'Product']
        char_dict = {k: v for k, v in zip(keys, values)}
        list_product.append(char_dict)
    return list_product


def fill_dict_for_tables(list_product):
    # This function returns a DataFrame for each characteristics price, reviews, product, stock
    # we filter the dictionary to get only the Product, the Price, the Old Price and the Date
    logging.debug('Build Dataframes for each product characteristics')
    df_price = []
    for i in range(0, len(list_product)):
        dictfilt = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])
        wanted_keys = ('Reference', 'Price', 'Old Price', 'Date')
        new_price_dict = dictfilt(list_product[i], wanted_keys)
        df_price.append(new_price_dict)

    # we filter the dictionary to get only the reviews and the ratings for each product
    df_reviews = []
    for i in range(0, len(list_product)):
        dictfilt = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])
        wanted_keys = ('Reference', 'Reviews', 'Ratings', 'Date')
        new_reviews_dict = dictfilt(list_product[i], wanted_keys)
        df_reviews.append(new_reviews_dict)

    df_product = []
    for i in range(0, len(list_product)):
        dictfilt = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])
        wanted_keys = ('Product', 'Reference')
        new_product_dict = dictfilt(list_product[i], wanted_keys)
        df_product.append(new_product_dict)

    df_stock = []
    for i in range(0, len(list_product)):
        dictfilt = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])
        wanted_keys = ('Reference', 'Stock', 'Date')
        new_stock_dict = dictfilt(list_product[i], wanted_keys)
        df_stock.append(new_stock_dict)

    return df_price, df_reviews, df_product, df_stock


def product_table(list_of_dictionnary):
    # Connect to the database
    cnx = pymysql.connect(host='localhost',
                          user='root',
                          password='ITC2018O')
    logging.debug('Create SQL table for each product')
    # Create a cursor object
    cursorObject = cnx.cursor()

    # if it's already exist and create Zabilo Database
    # query_database = "CREATE DATABASE IF NOT EXISTS ZABILO;"

    # Execute the sqlQuery
    # cursorObject.execute(query_database)

    # Use the database
    use = "USE ZABILO"

    # run the query_use
    cursorObject.execute(use)

    # Create our Table Zabilo_Product in under to follow the evolution of price and stock
    query_table_products = "CREATE TABLE IF NOT EXISTS Zabilo_Product (PRIMARY KEY (Reference), Reference CHAR(100), Product CHAR(100));"

    # Execute the sqlQuery
    cursorObject.execute(query_table_products)

    # SQL query to insert values in the columns of Zabilo_Product table
    query = 'INSERT INTO Zabilo_Product (Reference, Product) VALUES (%(Reference)s, %(Product)s) ON DUPLICATE KEY UPDATE Reference = (Reference) ;'
    cursorObject.executemany(query, list_of_dictionnary)
    cnx.commit()


def price_table(list_of_dictionnary):
    # Connect to the database
    cnx = pymysql.connect(host='localhost',
                          user='root',
                          password='ITC2018O')
    logging.debug('Create SQL table for each product price')
    # Create a cursor object
    cursorObject = cnx.cursor()

    # Use the database
    use = "USE ZABILO"

    # run the query_use
    cursorObject.execute(use)

    # Create our Table Zabilo_Price in under to follow the evolution of price and stock
    query_table_products = "CREATE TABLE IF NOT EXISTS ZABILO_Price (ID int(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY (ID),Reference CHAR(100), FOREIGN KEY (Reference) REFERENCES Zabilo_Product(Reference), Price INT, OldPrice INT, Date DATETIME(6));"

    # Execute the sqlQuery
    cursorObject.execute(query_table_products)

    # SQL query to insert values in the columns of Zabilo_Price table
    query = 'INSERT INTO ZABILO_Price (Reference,Price,OldPrice, Date) VALUES (%(Reference)s, %(Price)s,%(Old Price)s,%(Date)s) ON DUPLICATE KEY UPDATE Price = (Price) ;'
    cursorObject.executemany(query, list_of_dictionnary)
    cnx.commit()


def stock_table(list_of_dictionnary):
    logging.debug('Create SQL table for each stock')
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

    # Create our Table Zabilo_Stock in under to follow the evolution of price and stock
    query_table_products = "CREATE TABLE IF NOT EXISTS Zabilo_Stock (ID int(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY (ID), Reference CHAR(100), FOREIGN KEY (Reference) REFERENCES Zabilo_Product(Reference),Stock CHAR(100), Date DATETIME(6));"

    # Execute the sqlQuery
    cursorObject.execute(query_table_products)

    # SQL query to insert values in the columns of Zabilo_Stock table
    query = 'INSERT INTO Zabilo_Stock (Reference, Stock, Date) VALUES (%(Reference)s, %(Stock)s, %(Date)s) ON DUPLICATE KEY UPDATE Stock = (Stock) ;'
    cursorObject.executemany(query, list_of_dictionnary)
    cnx.commit()


def table_reviews_ratings(list_of_dictionnary):
    logging.debug('Create SQL table for each product reviews and ratings')
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
    query_table_products = "CREATE TABLE IF NOT EXISTS Zabilo_reviews (ID int(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY (ID), Reference CHAR(100),Ratings CHAR(250) ,Reviews CHAR(250), Date DATETIME(6));"

    # Execute the sqlQuery
    cursorObject.execute(query_table_products)

    # SQL query to insert values in the columns of Zabilo_Reviews table
    query = "INSERT INTO Zabilo_reviews (Ratings,Reference, Reviews, Date) VALUES (%(Ratings)s,%(Reference)s, %(Reviews)s, %(Date)s) ON DUPLICATE KEY UPDATE Reference = (Reference) ;"
    cursorObject.executemany(query, list_of_dictionnary)
    cnx.commit()


@click.command()
@click.option('--category', '-c',
              help="Enter one of the different categories that you want to scrap :dishwashers,portable_dishwashers,refrigerators,ovens,cooktops,washing_machines,tumble_dryers,vacuums,televisions,air_conditioners. If you want all categories to be scrapped, write: All",
              default="dishwashers")
def main(category):
    logging.basicConfig(filename="Webscraping_project.log", level=logging.DEBUG,
                        format='%(asctime)s -- %(name)s -- %(levelname)s -- %(message)s')
    # We scrape the link of deal pages
    start_url = start(url_home_page)
    # We scrape the url for each category of product in deal pages
    deal_page = deal_pages(start_url)
    # We scrape the link of every product for each catgegory
    link_products = link_of_product(deal_page, category)
    # We scrape all informations for each Product
    characteristic = get_product_characteristics(link_products)
    # We create a table especially for products
    product_table(fill_dict_for_tables(characteristic)[2])
    # We create our Zabilo Database and a table zabilo_price prices and availability of our products
    price_table(fill_dict_for_tables(characteristic)[0])
    # We create a tabla zabilo_reviews to keep track of the Reviews and ratings of our products
    table_reviews_ratings(fill_dict_for_tables(characteristic)[1])
    # get the stock table
    stock_table(fill_dict_for_tables(characteristic)[3])


if __name__ == '__main__':
    main()
