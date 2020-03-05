# Path hack:
import sys
sys.path.append("..")
# Importing data managment/manipulation packages:
import pandas as pd
import re
# Importing Kijiji raw data scraper:
from Entropy_database_backend.canada_realestate_listings_db.kijiji_raw_data_scraper\
.kijiji_listings_scraper import Kijiji
# Importing SQLAlchemy ORM:
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Float, Date, String, Integer, MetaData, Table, DECIMAL

# Importing Geocoding packages:
from geopy import OpenCage
from geopy.extra.rate_limiter import RateLimiter


class kijiji_geoprocessor(object):
    """
    This object is meant to contain all the necessary data transformation methods
    and builds the completed transformed dataframe.

    Parameters
    ----------
    raw_data : pandas dataframe
        This is the raw RE listings data that is pulled from the SQL database
        via the mySQL_database_connector package

    api_key : str
        This is the api key that will be used to access the geocoder api to perform
        the geoprocessing data. For the moment this method uses the geocoder api
        OpenCage

    Raises
    ------
    SettingWithCopyWarning : pandas warning for set a df slice as a dataframe.
    """

    def __init__(self, raw_data, api_key):
        # Declaring the instance data:
        self.raw_data = raw_data
        self.api_key = api_key

        # Declaring instance of geocoder: OpenCage
        self.geolocator = OpenCage(self.api_key)
        # Adding rate limiter to avoid API bottleneck:
        self.geocode = RateLimiter(self.geolocator.geocode, min_delay_seconds=1)

        # Building the transformed dataframe:
        self.data = self.build_geoprocessed_data()


    def build_geoprocessed_data(self):

        # re-deffining raw dataframe:
        df = self.raw_data

        # Performing geotransformation on dataset:

        # Creating a colum of geocode objects to be called to manipulate other colums:
        df['geocode'] = df['Address'].apply(lambda x: self.geocode(x))

        # Adding Lat and Long columns
        df['Lat'] = df['geocode'].apply(lambda x: x.latitude)
        df['Long'] = df['geocode'].apply(lambda x: x.longitude)

        # Refactoring the Address column using geocoder formatting:
        df['Address'] = df['geocode'].apply(lambda x: x.address)

        # Dropping geocode table:
        df.drop(['geocode'], axis=1, inplace=True)


        # Performing data formatting:

        # Removing all non numeric characters from the 'Bedrooms', 'Bathrooms
        # columns:
        df['Bedrooms'] = df['Bedrooms'].apply(lambda x: re.sub(r'[^0-9 . NaN]', '', x))

        df['Bathrooms'] = df['Bathrooms'].apply(lambda x: re.sub(r'[^0-9 . NaN]', '', x))

        # replacing every instance of 'NaN' with "0" and 'Please Contact' with 'NaN':
        df.replace(to_replace='NaN', value='0')
        df.replace(to_replace='NULL', value='0')


        # Removing rows where there is no price listed:
        df = df[df.Price != 'Please Contact']

        # Converting data within df to the correct data type:
        print(df.Price)
        df.Price = df.Price.dropna()
        print(df.Bedrooms)
        df.Bedrooms = df.Bedrooms.dropna()
        print(df.Bathrooms)
        df.Bathrooms = df.Bathrooms.dropna()

        # Setting Address column as index:
        df = df.set_index(['Address'])

        return df


# Creating class that uses sqlalchemy to write a table:
class kijiji_re_listings_table(object):
    """"
    A class that contains all the sqlalchemy functions necessary to maintain a
    MySQL table on the real estate listings scraped from listings websites.

    When initalized, the class connects to a mysql database, creates a sql table
    if it does not already exist and then executes the update_table() method that
    writes unique data to the table.
    """

    def __init__(self, city_name, kijiji_url, num_pages, user, password, host,
     port, dbname, api_key):
        '''
        Parameters
        ----------
        city_name : str
            The name of the city for which data is being collected. It is used
            to determine the table name.

        kijiji_url : str
            The intial url that is used to initalizes the web scraping tools.
            This is manually input and unique to each Kijiji search.

        num_pages : str
            This is the number of pages the web scraper parses through and runs.
            It determines the length of the dataframe that is compiled and also
            the runtime of the web secraping application.

        user: str
            The MySQL user

        password : str
            The password of the MySQL user. If the user has no password input
            variable as 'NO'.

        host : str
            The host of the MySQL database.

        port : str
            The port that the MySQL database.

        dbname : str
            The name of the MySQL database.

        api_key : str
            This is the api key that will be used to access the geocoder api to perform
            the geoprocessing data. For the moment this method uses the geocoder api
            OpenCage

        Methods
        -------
        self.table_update()
            Writes each unique row of data to the MySQL table based on the built
            dataframe.
.
        '''

        # Declaring instance variables:
        self.city_name = city_name
        self.kijiji_url = kijiji_url
        self.num_pages = num_pages
        self.api_key = api_key

        # Building the table name:
        self.table_name = f'kijiji_{city_name.lower()}_real_estate_listings'

        # Conditional that selects if password or no password for db user:
        if password == 'NO':
            connect_string = f'mysql+mysqldb://{user}@{host}:{port}/{dbname}'
        else:
            connect_string = f'mysql+mysqldb://{user}:{password}@{host}:{port}/\
{dbname}'


        # Creating the sqlalchemy engine:s
        self.engine = create_engine(connect_string, echo=True)

        if not self.engine.dialect.has_table(self.engine, self.table_name):
            metadata = MetaData(self.engine)

            # Creating table if not exists:
            Table(self.table_name, metadata,
            Column('Address', String(250), primary_key=True, unique=True),
            Column('Price', Float),
            Column('Date', Date),
            Column('Bedrooms', Float),
            Column('Bathrooms', Float),
            Column('Size', Float),
            Column('Lat', DECIMAL(38,6)),
            Column('Long', DECIMAL(38,6)))

            # Commiting changes:
            metadata.create_all()

        # Initalzing an instance variable of the web_scraped data:
        raw_data = Kijiji(kijiji_url, num_pages).data
        self.geoprocessed_data = kijiji_geoprocessor(raw_data, api_key).data

        # Updating the table:
        self.table_update()


    def table_update(self):
        '''Writes each unique row of data to the MySQL table.
        '''

        # Itterative loop writing uniqe rows to the Mysql table:
        for row in self.geoprocessed_data.iterrows():

            # Constructing each row:
            entry_row = pd.DataFrame(row[1]).transpose()

            # Writing individual entry rows to the Mysql table:
            try:
                # try catch to ensure only entry_rows unqiue to table are written
                entry_row.to_sql(name=self.table_name, con=self.engine,
                if_exists='append', index_label='Address')

                print(entry_row)

            except:
                continue



# Testing:
kijiji_url = 'https://www.kijiji.ca/b-for-sale/kelowna/c30353001l1700228'
api_key = '335b16e9c7734f1985fc5e4dfa8a767b'
kijiji_re_listings_table('kelowna', kijiji_url, 4, 'root', 'NO', 'localhost',
 '3306', 'canada_real_estate_listings_database', api_key)
