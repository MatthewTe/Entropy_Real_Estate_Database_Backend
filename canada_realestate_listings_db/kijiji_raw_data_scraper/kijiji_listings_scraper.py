# Package sys.path hack:
import sys
sys.path.append("..")

# Importing scraping packages:
import bs4
import requests
# Importing data managment packages:
import pandas as pd
import datetime

'''
Script contains a set of objects meant to represent the RE listings data scraped
from various public websites.
The main function of these objects will be as a means of integerating the scraped
data into the listings database via database connection objects and methods.
'''

class Kijiji(object):
    """
    This object is mean to represent the data scraped from the canadian listings
    website Kijiji.ca.
    The object will contain various methods for selecting
    specific instances of data scraped from the site however the objects focus
    will be on itterating through the listings website's various pages and
    extracting all listings data into a large dataframe given a Kijiji url as a
    starting point.

    Parameters
    ----------
    init_url : str
        This url will serve as the starting point for the web scraping methods in
        this object. Using this url as a starting point it will itterate through
        the number of pages specificed by the page variable, constructing a
        large dataframe of individual listings

    page : int
        An integer that specifies the number of pages the web scraping methods
        will itterate through in constructing the main dataframe.
    """

    def __init__(self, init_url, page):

        # Declaring instance variables:
        self.inital_url = init_url
        self.page = page

        # Instance of main listings dataframe:
        self.data = self.build_main_dataframe()

    def build_main_dataframe(self):
        '''This main method of the Kijiji object uses several other Kijiji object
        methods to construct the main dataframe of listings scraped from
        the inital Kijiji.ca url.

        Returns
        -------
        main_listings_dataframe : pandas dataframe
            The main dataframe that contains all the listings data scraped
            from Kijiji.ca
        '''

        # Creating main_listings_dataframe according to schema:
        main_listings_dataframe = pd.DataFrame(columns=['Address', 'Price', 'Date',
        'Bedrooms', 'Bathrooms', 'Size'])

        # Internally deffining url:
        url = self.inital_url

        # Itteration that loops through the number of listings pages specificed
        # by the page number, starting at the inital url and constructing the
        # main dataframe:
        for page_numer in range(self.page):

            # converting the page specified by url to a dataframe:
            page_dataframe = Kijiji.page_to_dataframe(url)

            # Appending current page_dataframe to main_dataframe:
            main_listings_dataframe = main_listings_dataframe.append(page_dataframe)

            # re-deffining url variable to the url to the next listings page:
            url = Kijiji.get_next_url(url)


        return main_listings_dataframe


    def page_to_dataframe(url):
        '''This method parses a Kijiji.ca RE listings page and converts all
        listings shown on the page to a pandas dataframe.

        Parameters
        ----------
        url : str
            This is the Kijiji.ca listings url that will be parsed and converted
            to a dataframe.

        Returns
        -------
        page_dataframe : pandas dataframe
            The dataframe containing all the RE listings found on the Kijiji.ca
            page
        '''

        # Acessing webpage and parsing the html:
        res = requests.get(url)
        soup = bs4.BeautifulSoup(res.text, 'lxml')

        # Creating a dataframe that will store all listings data for the page:
        page_dataframe = pd.DataFrame(columns=['Address', 'Price', 'Date',
        'Bedrooms', 'Bathrooms', 'Size'])


        # extracting list of listings div tags: <div class = 'search-item regular-ad'>
        listings = soup.findAll('div', {'class': 'search-item regular-ad'})


        # itteration that converts ever listings to a series and constructs dataframe:
        for listing in listings:

            # parse each individual listing for the listing's href and date:
            # <a class = 'title'>
            listing_href = listing.findAll('a',{'class': 'title'})[0]['href']
            # building url to main listings page:
            listing_url = 'https://www.kijiji.ca' + listing_href


            # parsing for listings date <div class = 'location'>
            listings_date = listing.findAll('div', {'class': 'location'})[0].span.text
            # date string formatting:
            listings_date = listings_date.replace('<', '').replace(' ', '')
            # Converting listings date to a datetime object:
            try:
                Date = datetime.datetime.strptime(listings_date, '%d/%m/%Y')
            except:
                Date = datetime.datetime.now().date()


            # Extracting the data from the listings url:
            listings_data = Kijiji.href_parser(listing_url)

            # Overwriting 'NaN' date value with extracted listings_date value:
            listings_data['Date'] = Date


            # Constructing page_dataframe with each listing found on the page:
            page_dataframe = page_dataframe.append(listings_data, ignore_index=True)


        return page_dataframe

    def href_parser(href):
        '''This method takes the href link found on a Kijiji.ca main listings
        page and parses the webpage for an individual listing to extract and
        return a pandas series containing the basic data of each listings.

        Parameters
        ----------
        href : str
            This is the href link that links to an individual listings main
            webpage.

        Returns
        -------
        listing : pandas series
            This is a series that contains the listings data for an individual
            listing
        '''
        # Accessing and parsing the webpage:
        res = requests.get(href)
        soup = bs4.BeautifulSoup(res.text, 'lxml')

        # Parsing the html for the listigns data:

        # Address: <span itemprop = 'address'>
        Address = soup.findAll('span', {'itemprop': 'address'})[0].text


        # Price: <span class = 'currentPrice-2842943473'>
        Price = soup.findAll('span', {'class':
         'currentPrice-2842943473'})[0].text

        # Converting price string to appropriate format:
        Price = Price.replace('$', '').replace(',', '')


        # Number of Beds, Bathrooms and Square Feet:
        attribute_tags = soup.findAll('dt', {'class': 'attributeLabel-240934283'})
        attribute_values = soup.findAll('dd', {'class': 'attributeValue-2574930263'})

        # Creating a dictionary that will store the various attributes independent
        # of there listing order on the website:
        attributes_dict = {}

        # Itterative loop appending attribute data to attributes_dict:
        counter = 0 # Counter to track attribute_values in loop
        for attribute in attribute_tags:

            attribute_instance = {attribute.text : attribute_values[counter].text}

            counter = counter + 1

            # adding values to main dict:
            attributes_dict.update(attribute_instance)


        # Extracting attributes from built attributes dict:
        try:
            Bedrooms = attributes_dict['Bedrooms']
        except:
            Bedrooms = 'NULL'

        try:
            Bathrooms = attributes_dict['Bathrooms']
        except:
            Bathrooms = 'NULL'

        try:
            Size = attributes_dict['Size (sqft)'].replace(',', '')
        except:
            Size = '0'

        # Declaring date_posed as a dummy variable to be overwritten later in the
        # data model:
        Date = 'NaN'


        # Creating series:
        listing = pd.Series([Address, Price, Date, Bedrooms, Bathrooms, Size],
        index = ['Address', 'Price', 'Date', 'Bedrooms', 'Bathrooms', 'Size'])

        return listing

    def get_next_url(url):
        '''This method parses a Kijiji.ca RE listings page and extracts and
        returns the url to the next Kijiji listings page.

        Parameters
        ----------
        url : str
            This is the Kijiji.ca url that will be paresed and the 'next url'
            extracted.

        Returns
        -------
        next_url : str
            This is the url to the next Kijiji.ca RE listings page
        '''

        # Accessing webpage and parsing html:
        res = requests.get(url)
        soup = bs4.BeautifulSoup(res.text, 'lxml')

        # Extracting the next url from the html: <a title = 'Next'>
        next_href = soup.findAll('a', {'title': 'Next'})[0]['href']

        # Building full url from href link:
        next_url = 'https://www.kijiji.ca' + next_href

        return next_url


#print(Kijiji('https://www.kijiji.ca/b-for-sale/kelowna/c30353001l1700228',1).data)
