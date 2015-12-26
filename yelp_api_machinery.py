# -*- coding: utf-8 -*-

import sys
import pdb
from collections import namedtuple
from operator import itemgetter
import psycopg2
from psycopg2.extras import NamedTupleConnection
import requests
from requests_oauthlib import OAuth1
from constants import   SEARCH_ADDR_BASE_URL, \
                        SEARCH_PHONE_BASE_URL, \
                        DB_NAME, \
                        DOH_INSPECTIONS_TABLE_NAME, \
                        DOH_RESTAURANTS_TABLE_NAME, \
                        MAX_GET_ATTEMPTS
from secret_constants import CONSUMER_KEY, CONSUMER_SECRET, TOKEN, TOKEN_SECRET


RESTAURANT_YELP_EXTRACT_FIELDS = [  
                                    'yelp_name',
                                    'yelp_id',
                                    'yelp_address',
                                    'yelp_city',
                                    'yelp_zipcode',
                                    'yelp_phone',
                                    'yelp_review_count',
                                    'yelp_rating',
                                    'yelp_categories',
                                    'yelp_neighborhoods',
                                 ]

RestaurantYelpExtract = namedtuple('RestaurantYelpExtract', RESTAURANT_YELP_EXTRACT_FIELDS)


class YelpApiResponseParser():
    ''' Parse method takes the json style response and 
    extracts the restaurant informtion for every business in the list.'''
        
    def parse(self, response_json):
        
        if response_json.get('total') == 0:
            return []
        
        restaurant_extracts = []
        for restaurant_dict in response_json.get('businesses'):
            
            restaurant_attributes = self._get_attributes(restaurant_dict)            

            restaurant_extract = self._build_restaurant_extract(restaurant_attributes)

            restaurant_extracts.append(restaurant_extract)
            
        return restaurant_extracts

    def _get_attributes(self, restaurant_dict):

        # if a 'get' fails in any of the following, return the empty type of
        # the type that would be returned on success.
        
        #id info
        yelp_name = restaurant_dict.get('name', '')
        yelp_id = restaurant_dict.get('id', '')
        yelp_phone = restaurant_dict.get('phone', '').replace('+','')
        
        #location info
        location = restaurant_dict.get('location', '')
        yelp_city = location.get('city', '')
        yelp_address = location.get('address', '')
        yelp_address = yelp_address[0] if len(yelp_address) > 0 else ''
        yelp_zipcode = location.get('postal_code', '')
        yelp_neighborhoods = location.get('neighborhoods', []) 
        
        #food/service info
        yelp_categories = restaurant_dict.get('categories', [])
        yelp_categories = map(itemgetter(0), yelp_categories)
        
        yelp_review_count = restaurant_dict.get('review_count')
        yelp_rating = restaurant_dict.get('rating')

        restaurant_attributes = [
                                    yelp_name,
                                    yelp_id, 
                                    yelp_address, 
                                    yelp_city, 
                                    yelp_zipcode, 
                                    yelp_phone, 
                                    yelp_review_count, 
                                    yelp_rating, 
                                    yelp_categories, 
                                    yelp_neighborhoods
                                ]

        return restaurant_attributes

    def _build_restaurant_extract(self, restaurant_attributes):

        return RestaurantYelpExtract._make(restaurant_attributes)


####--------------------------------------------------------------------------------------------------#### 
####--------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------####


class YelpApiInterfacer():
    '''Take a doh restaurant description (tuple) and interfaces 
    with the yelp api, then passes the response to a response parser.
    '''
    
    def __init__(self, report_interval = 500):
        
        self.AUTH = OAuth1( CONSUMER_KEY, 
                            CONSUMER_SECRET,
                            TOKEN,
                            TOKEN_SECRET)

        self.report_interval = report_interval
        self.parser = YelpApiResponseParser()
    
    def pull_restaurants(self, restaurants):
        
        restaurant_infos = []

        for i, r in enumerate(restaurants):
            
            self._report_pull_progress_init(i, n_restaurants)   

            restaurant_infos.append(self._pull_restaurant(r))
            
            self._report_pull_progress_completed(i, len(restaurants))
        
        return restaurant_infos
         

    def _pull_restaurant(self, restaurant_tuple):
        raise NotImplementedError


    def _fetch(self, restaurant_tuple, payload, base_url):
        
        response = None
        for _ in range(MAX_GET_ATTEMPTS):
            
            try:
                response = requests.get(url = base_url, params = payload, auth = self.AUTH)
                break
            except requests.ConnectionError:
                self._report_connection_error(restaurant_tuple, payload)

        if response is None:
            return []
         
        try:
            return self.parser.parse(response.json())
        
        except:
            self._report_parse_error(restaurant_tuple, response)
            response_json = response.json()
            if 'error' in response_json:
                return []

            else:
                raise

    #     MAKE INTO SEPERATE CLASS?
    def _report_pull_progress_init(self, i, total):

        if i == 0:
            print "{0} Restaurants to pull.".format(total)

        if not (i % self.report_interval):
            print "Pulling restaurant number {0}...".format(i)

        sys.stdout.flush()


    def _report_pull_progress_completed(self, i, total):

        if not (i % self.report_interval):     
            print "Restaurant {0} completed.".format(i)

        if i == total - 1:
            print "Completion: {0} restaurants pulled.".format(total)

        sys.stdout.flush()   


    def _report_error(self, messages):

            print '*'*60
            print '*'*60
            for message in messages:
                print message
            print '*'*60
            print '*'*60


    def _report_parse_error(self, restaurant_tuple, response):

            messages = [
                        'Could not parse response.',
                        restaurant_tuple._asdict(),
                        response.json(),
                        ]

            self._report_error(messages)
        
        

    def _report_connection_error(self, restaurant_tuple, payload):
            
            messages = [
                        'Connection Error encountered.',
                        restaurant_tuple._asdict(),
                        "\tUrl:",
                        SEARCH_ADDR_BASE_URL,
                        "\tParams:",
                        payload,
                        ]

            self._report_error(messages)

        
####--------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------####


class YelpApiPhoneInterfacer(YelpApiInterfacer):

    def __init__(self, **kwargs):

        YelpApiInterfacer.__init__(self, **kwargs)

    def _pull_restaurant(self, restaurant_tuple):

        phone = restaurant_tuple.doh_phone
        if not phone:
            return (restaurant_tuple, [])
        
        cc = 'US'
        limit = 2
        payload = {'phone':phone,
                    'cc':cc,
                    'limit':limit 
            }
        
        extract = self._fetch(restaurant_tuple, payload, SEARCH_PHONE_BASE_URL) #returns a list of one if successful
        if len(extract) > 0:
            extract = extract[0]

        return (restaurant_tuple, extract)


####--------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------####


class YelpApiAddressInterfacer(YelpApiInterfacer):

    def __init__(self, limit = 1, sort = 0, **kwargs):

        YelpApiInterfacer.__init__(self, **kwargs)
        self.limit = limit
        self.sort = sort

    def _pull_restaurant(self, restaurant_table_extract):

        name = restaurant_table_extract.doh_dba
        addr = "{0}, {1}".format(restaurant_table_extract.doh_address, restaurant_table_extract.doh_zipcode)
        payload = {'term':name,
            'location':addr,
            'limit':self.limit,
            'sort':self.sort
            }
        
        extracts = self._fetch(restaurant_table_extract, payload, SEARCH_ADDR_BASE_URL)
        return (restaurant_table_extract, extracts)        

####--------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------####

class YelpApiCoordinator():

    def __init__(self, api_interfacer, start_read = 0):
            
        self.current = start_read
        self.api_interfacer = api_interfacer
        self.conn = None
    
    def seek(self, n):
        self.current = n

    def open_conn(self):
        
        self.conn = psycopg2.connect("dbname={db_name}".format(db_name = DB_NAME),
                                     cursor_factory = psycopg2.extras.NamedTupleCursor)
        self.c = self.conn.cursor()

    def close_conn(self):

        if self.conn is not None:
            self.conn.close()
        self.conn = None

    def read_next_n(self, n):

        if not self.conn:
            self.open_conn()
           
        q = self.q_n_template.format(   doh_restaurants_table_name = DOH_RESTAURANTS_TABLE_NAME, 
                                        n = n, 
                                        offset = self.current)
        try:
            self.self.c.execute(q)
            query_result_tuples = self.c.fetchall()
        finally:
            self.close_conn()

        extract_tuples = self.api_interfacer.pull_restaurants(query_result_tuples)
        self.current += n
        return extract_tuples

    def read_all(self):

        if not self.conn:
            self.open_conn()
        
        q = self.q_all_template.format( doh_restaurants_table_name = DOH_RESTAURANTS_TABLE_NAME,
                                        offset = self.current)
        
        try:
            self.c.execute(q)
            query_result_tuples = self.c.fetchall()
        finally:
            self.close_conn()    
        
        extract_tuples = self.api_interfacer.pull_restaurants(query_result_tuples)
        self.current = 0
        return extract_tuples  

####--------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------####

class YelpApiFirstPassCoordinator(YelpApiCoordinator):
    '''Interacts with the database and sends 
    (doh_camis, yelp_id, yelp_dba, yelp_address, yelp_zipcode, yelp_phone) tuples to the yelp api handlers.
    '''

    def __init__(self, *args, **kwargs):
        YelpApiCoordinator.__init__(self, *args, **kwargs)

        self.q_n_template = '''
                            SELECT *
                            FROM {doh_restaurants_table_name}
                            ORDER BY doh_camis ASC
                            LIMIT {n} OFFSET {offset};
                            '''
        q_all_template =    '''
                            SELECT *
                            FROM {doh_restaurants_table_name}
                            ORDER BY doh_camis ASC
                            OFFSET {offset};
                            '''
    
    # def read_next_n(self, n):
        
    #     if not self.conn:
    #         self.open_conn()
           
    #     query = q_n_template.format(doh_restaurants_table_name = DOH_RESTAURANTS_TABLE_NAME, 
    #                                 n = n, 
    #                                 offset = self.current)
        
    #     try:
    #         self.c.execute(query)
    #         query_result_tuples = self.c.fetchall()
    #     finally:
    #         self.close_conn()

    #     extract_tuples = self.api_interfacer.pull_restaurants(query_result_tuples)
    #     self.current += n
    #     return extract_tuples
         
    # def read_all(self):
        
    #     if not self.conn:
    #         self.open_conn()
        
    #     q = q_all_template.format(  doh_restaurants_table_name = DOH_RESTAURANTS_TABLE_NAME,
    #                                 offset = self.current)
        
    #     try:
    #         self.c.execute(q)
    #         query_result_tuples = self.c.fetchall()
    #     finally:
    #         self.close_conn()    
        
    #     extract_tuples = self.api_interfacer.pull_restaurants(query_result_tuples)
    #     self.current = 0
    #     return extract_tuples
        
####--------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------####

class YelpApiSecondPassCoordinator(YelpApiCoordinator):

    def __init__(self, *args, **kwargs):

        YelpApiCoordinator.__init__(self, *args, **kwargs)

        self.q_all_template = ''' 
                            SELECT * 
                            FROM {doh_restaurants_table_name} 
                            WHERE doh_camis in ((SELECT doh_camis 
                                                FROM doh_restaurants
                                                OFFSET {offset})
                                                EXCEPT
                                                (SELECT doh_camis FROM yelp_restaurants))
                            ORDER BY doh_camis ASC;
                            '''

        self.q_n_template = ''' 
                            SELECT * 
                            FROM {doh_restaurants_table_name} 
                            WHERE doh_camis in (  
                                            (SELECT doh_camis 
                                            FROM doh_restaurants
                                            ORDER BY doh_camis
                                            LIMIT {n} OFFSET {offset})
                                            EXCEPT
                                            (SELECT doh_camis FROM yelp_restaurants)  
                                            ) 
                            ORDER BY doh_camis ASC;
                            '''

    
    # def read_next_n(self, n):
        
    #     if not self.conn:
    #         self.open_conn()
           
    #     q = self.q_n_template.format(doh_restaurants_table_name = DOH_RESTAURANTS_TABLE_NAME, 
    #                         n = n, offset = self.current)
        
    #     try:
    #         self.c.execute(q)
    #         query_result_tuples = self.c.fetchall()
    #     finally:
    #         self.close_conn()

    #     extract_tuples = self.api_interfacer.pull_restaurants(query_result_tuples)
    #     self.current += n
    #     return extract_tuples
         
    # def read_all(self):
        
    #     if not self.conn:
    #         self.open_conn()
        
    #     q = self.q_all_template.format(doh_restaurants_table_name = DOH_RESTAURANTS_TABLE_NAME, 
    #                                     n = n,
    #                                     offset = self.current)
        
    #     try:
    #         self.c.execute(q)
    #         query_result_tuples = self.c.fetchall()
    #     finally:
    #         self.close_conn()    
        
    #     extract_tuples = self.api_interfacer.pull_restaurants(query_result_tuples)
    #     self.current = 0
    #     return extract_tuples    

    

