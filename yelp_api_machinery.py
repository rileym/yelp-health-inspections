import sys
import pdb
from collections import namedtuple
from operator import itemgetter
import psycopg2
from psycopg2.extras import NamedTupleConnection
import requests
from requests_oauthlib import OAuth1


fields = 'yelp_name, yelp_id, yelp_address, yelp_city, yelp_zipcode, yelp_phone, review_count, rating, categories, neighborhoods'
RestaurantExtract = namedtuple('RestaurantExtract', fields)


class YelpApiResponseParser():
    ''' Takes the json style response and extracts the restaurant informtion for every business in the list.'''
        
    def parse(self, response_json):
        
        if response_json.get('total') == 0:
            return []
        
        restaurant_extracts = []
        for restaurant in response_json.get('businesses'):
            
            # if a get fails in any of the following, return the empty type of
            # the type that would be returned on success.
            
            #id info
            yelp_name = restaurant.get('name', '')
            yelp_id = restaurant.get('id', '')
            yelp_phone = restaurant.get('phone', '').replace('+','')
            
            #location info
            location = restaurant.get('location', '')
            yelp_city = location.get('city', '')
            yelp_address = location.get('address', '')
            yelp_address = yelp_address[0] if len(yelp_address) > 0 else ''
            yelp_zipcode = location.get('postal_code', '')
            neighborhoods = location.get('neighborhoods', []) 
            
            #food/service info
            cat_list = restaurant.get('categories', [])
            categories = map(itemgetter(0), cat_list)
            
            review_count = restaurant.get('review_count')
            rating = restaurant.get('rating')

            attrs = [yelp_name, yelp_id, yelp_address, yelp_city, yelp_zipcode, yelp_phone, 
                             review_count, rating, categories, neighborhoods]
            extract = RestaurantExtract._make(attrs)
            restaurant_extracts.append(extract)
            
        return restaurant_extracts
    
####--------------------------------------------------------------------------------------------------#### 
####--------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------####


class YelpApiInterfacer():
    '''Take a restaurant description (tuple) and interfaces 
    with the yelp api, then passes the response to a response parser.
    '''
    
    def __init__(self, by_phone = True, by_addr_0 = False, by_addr_1 = False, by_addr_limit = 1, report_interval = 500):
        
        self.CONSUMER_KEY = 'kvO4vOdaI2jjLjA0eUEXiQ'
        self.CONSUMER_SECRET = 'H7na91jgidG_k3_iiHi_2qbkJMQ'
        self.TOKEN = '6WN41TOPmVeEgu-w0iM8gyyKhMM5rCu7'
        self.TOKEN_SECRET = 'erbNVdX7x6oG0ZCbsZVzfCZg7dg'
        self.SEARCH_ADDR_BASE_URL = 'http://api.yelp.com/v2/search'
        self.SEARCH_PHONE_BASE_URL = 'http://api.yelp.com/v2/phone_search'
        self.AUTH = OAuth1(self.CONSUMER_KEY, self.CONSUMER_SECRET,
                  self.TOKEN, self.TOKEN_SECRET)
        self.MAX_GET_ATTEMPTS = 3
        
        self.report_interval = report_interval
        
        self.by_addr_limit = by_addr_limit
        self.by_phone = by_phone
        self.by_addr_0 = by_addr_0
        self.by_addr_1 = by_addr_1
        
        self.parser = YelpApiResponseParser()
    
    def pull_restaurants(self, restaurants):
        
        restaurant_infos = []
        print "{0} Restaurants to pull.".format(len(restaurants))
        for i,r in enumerate(restaurants):
            
            verbose = (i % self.report_interval == 0)
            if verbose:
                print "Pulling restaurant number {0}...".format(i)
                sys.stdout.flush()
                
            restaurant_infos.append(self._pull_restaurant(r))
            
            if verbose:
                print "Restaurant {0} completed.".format(i)
                sys.stdout.flush()
        
        print "Completion: {0} restaurants pulled.".format(len(restaurants))
        return restaurant_infos
    
    def _pull_restaurant(self, rest_tuple):
        
        result_dict = {}
        if self.by_phone:
            result_dict['by_phone'] = self._pull_restaurant_by_phone(rest_tuple)
        if self.by_addr_0:
            result_dict['by_addr_0'] = self._pull_restaurant_by_address(rest_tuple, sort = 0)
        if self.by_addr_1:
            result_dict['by_addr_1'] = self._pull_restaurant_by_address(rest_tuple, sort = 1)      

        return (rest_tuple, result_dict)
    
    
    def _pull_restaurant_by_address(self, rest_tuple, sort = 0):
        
        name = rest_tuple.dba
        addr = "{0}, {1}".format(rest_tuple.address, rest_tuple.zipcode)
        payload = {'term':name,
            'location':addr,
            'limit':self.by_addr_limit,
            'sort':sort
            }
        
        extracts = self._fetch(rest_tuple, payload, phone = False)
        return extracts
    
    
    
    def _pull_restaurant_by_phone(self, rest_tuple):
        
        phone = rest_tuple.phone
        if not phone:
            return None
        
        cc = 'US'
        payload = {'phone':phone,
            'cc':cc,
            'limit':1 
            }
        
        extract = self._fetch(rest_tuple, payload, phone = True) #returns a list of one if successful
        if len(extract) > 0:
            extract = extract[0]
        return extract
        

    def _fetch(self, rest_tuple, payload, phone):
        
#         pdb.set_trace()
        base_url = self.SEARCH_PHONE_BASE_URL if phone else self.SEARCH_ADDR_BASE_URL
        response = None
        for _ in xrange(self.MAX_GET_ATTEMPTS):
            
            try:
                response = requests.get(url = base_url, params = payload, auth = self.AUTH)
                break
            except requests.ConnectionError:
                self._report_connection_error(rest_tuple, payload)

        if response is None:
            return None
         
        try:
            return self.parser.parse(response.json())
        
        except:
            self._report_parse_error(rest_tuple, response)
            response_json = response.json()
            if 'error' in response_json:
                return None

            else:
                raise
        
        
    def _report_parse_error(self, rest_tuple, response):
            print '************************'
            print 'Could not parse response.'
            print rest_tuple._asdict()
            print response.json()
            print '************************'        
        
    def _report_connection_error(self, rest_tuple, payload):
            print '************************'
            print "Connection Error encountered."
            print rest_tuple._asdict()
            print "\tUrl:"
            print self.SEARCH_ADDR_BASE_URL
            print "\tParams:"
            print payload
            print '************************'
        

####--------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------####


class YelpApiCoordinator():
    '''Interacts with the database and sends 
    (camis, dba, address, zipcode, phone) tuples to the yelp api handlers.
    '''
    
    def __init__(self, start_read = 0, **kwargs):
            
        self.current = start_read
        self.interface = YelpApiInterfacer(**kwargs)
        self.conn = None
        self.temp_table = 'trwlr_table'
    
    def seek(self, n):
        self.current = n
    
    
    def read_next_n(self, n):
        
        if not self.conn:
            self.open_conn()
           
        q = '''SELECT camis, dba, address, zipcode, phone
            FROM {temp_table}
            LIMIT {0} OFFSET {1};
        '''.format(n, self.current, temp_table = self.temp_table)
        
        self.c.execute(q)
        query_result_tuples = self.c.fetchall()
        extract_tuples = self.interface.pull_restaurants(query_result_tuples)
        self.current += n
        return extract_tuples
         
    def read_all(self):
        
        if not self.conn:
            self.open_conn()
        
        q = '''SELECT *
            FROM {temp_table};
            '''.format(self.current, temp_table = self.temp_table)
        self.c.execute(q)
        query_result_tuples = self.c.fetchall()
        
        extract_tuples = self.interface.pull_restaurants(query_result_tuples)
        self.current = 0
        return extract_tuples
    
    def open_conn(self):
        
        self.conn = psycopg2.connect("dbname=yelp", cursor_factory=psycopg2.extras.NamedTupleCursor)
        self.c = self.conn.cursor()
        q = '''CREATE TEMP TABLE {temp_table} AS (
            SELECT camis, dba, address, zipcode, phone
            FROM inspections
            GROUP BY camis, dba, address, zipcode, phone
            ORDER BY camis ASC
            );
            '''.format(temp_table = self.temp_table)
        self.c.execute(q)
    
    def close_conn(self):
        if self.conn is not None:
            self.conn.close()
        self.conn = None
        


    