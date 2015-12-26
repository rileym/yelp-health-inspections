import re
import time
import pdb
import sys
import numpy as np
from collections import namedtuple
from operator import itemgetter
from itertools import takewhile, count, islice

import psycopg2
from psycopg2.extras import NamedTupleConnection

import requests
from urlparse import urljoin
from bs4 import BeautifulSoup



class YelpPageParser():
    
    def __init__(self):
          
        self._PageExtract = self._build_packager()
    
    def _build_packager(self):

        # INSIDE HERE OR AT CLASS LEVEL?
        
        fields = ['avg_rating', 'total_count', 'english_count',
                 'dollar_signs', 'reviews']
        
        return namedtuple('Single_Page_Extract', fields)
    
    def parse(self, raw_html):
        
        soup = BeautifulSoup(raw_html)
        avg_rating = self._get_avg_rating(soup)
        total_count, english_count = self._get_review_counts(soup)
        dollar_signs = self._get_dollar_signs(soup)
        reviews = self._get_reviews(soup)
            

        
        return self._package_results(avg_rating = avg_rating, 
                                    total_count = total_count,
                                    english_count = english_count,
                                    dollar_signs = dollar_signs, 
                                    reviews = reviews
                                    )
    
    def _get_avg_rating(self, page_soup):
        
        selection = page_soup.select('div.biz-page-header-left meta')[0]
        return float(selection.attrs['content'])
    
    def _get_review_counts(self, page_soup):
        
        selection = page_soup.select('div.biz-page-header-left span[itemprop=reviewCount]')[0]
        total_count = int(selection.text)
        
        selection = page_soup.select('div.feed-sorts-filters.clearfix span.count')[0]
        english_count = int(selection.text.strip())
        
        return (total_count, english_count)
    
    def _get_dollar_signs(self, page_soup):
        
        try:
            selection = page_soup.select('div.iconed-list-avatar > span.business-attribute.price-range')[0]
            dollar_signs = len(selection.text)
        
        except:
            self._report_parse_error(page_soup, "Could not find Pricing Info. Returning 'NULL'.")
            return 'NULL'
        

    #             selection = page_soup.select('dd.nowrap.price-description')[0]
    #         price_range = tuple(map(int, tuple(selection.text.strip()[1:].split('-'))))
        
        return dollar_signs
        
    def _get_reviews(self, page_soup):
        
        reviews = []
        
        review_soup_list = page_soup.select('div.review-list li div.review-content')
        for review_soup in review_soup_list:

            rating_soup = review_soup.select('meta[itemprop=ratingValue]')
            rating = float(rating_soup[0].attrs['content'])
            
            date_soup = review_soup.select('meta[itemprop=datePublished]')
            date = date_soup[0].attrs['content']
            
            review_soup = review_soup.select('p[itemprop=description]')
            review_text = review_soup[0].text

            reviews.append((date, rating, review_text))
            
        return reviews
    
    def _package_results(self, **kwargs):

        return self._PageExtract(**kwargs)
    
    def _report_parse_error(self, soup, *args):
        # To implement once I get an idea of what errors beautiful soup throws.
        print '************************'
        name = unicode(soup.select('h1.biz-page-title')[0].text)
        print u'Encountered Parse Error for the {0} business.'.format(name.strip())
        print '************************'
        for arg in args:
            print args
        print '************************' 
        sys.stdout.flush()
    
    

    
class YelpWebsiteInterfacer():
    
    def __init__(self):
        
        self.MAX_GET_ATTEMPTS = 3
        
    def get_page(self, url, params = {}):
        
        page_html = None
        for k in xrange(1, self.MAX_GET_ATTEMPTS+1):
            
            try:  
                response = requests.get(url, params = params)
                page_html = response.content
                break
            
            except requests.ConnectionError:
                self._report_connection_error(url, k)
                
                
        if page_html is None:
            return None
        
        return page_html
    
    def _report_connection_error(self, url, k):
        
            print '************************'
            print "Connection Error encountered on attempt number {0}.".format(k)
            print "URL:"
            print url
            
            if k < self.MAX_GET_ATTEMPTS:
                print "Attempting to get page again..."
            else:
                print "Will not attempt to get this page anymore."
                
            print '************************'
        




class YelpPaginator():
    
    def __init__(self, max_page_pulls = 4):
        
        self.max_page_pulls = max_page_pulls
        self.REVIEWS_PER_PAGE = 40 #OUTSIDE CONST?
        self.BASE_YELP_BIZ_URL = 'http://www.yelp.com/biz/'
        self.web_interfacer = YelpWebsiteInterfacer()
        self.page_parser = YelpPageParser()
        
    def pull_business(self, yelp_biz_id):
        
        base_page_url = urljoin(self.BASE_YELP_BIZ_URL, yelp_biz_id)
        full_restaurant_extract = self._paginate_pull(base_page_url)
        return full_restaurant_extract
    
    def _paginate_pull(self, base_page_url):
        
        page_html = self.web_interfacer.get_page(base_page_url)
        restaurant_extract = self.page_parser.parse(page_html)
        
        english_review_count = restaurant_extract.english_count #number of english reviews
        
        for i in count(1):
            
            offset = i*self.REVIEWS_PER_PAGE
            if offset >= english_review_count or i >= self.max_page_pulls:
                break
            
            params = {'start':offset}
            page_html = self.web_interfacer.get_page(base_page_url, params = params)
            page_extract = self.page_parser.parse(page_html)
            
            restaurant_extract.reviews.extend(page_extract.reviews)
        
        return restaurant_extract
            



class YelpScrapeCoordinator():
    
    def __init__(self, start = 0, max_page_pulls = 2):
        
        self._paginator = YelpPaginator(max_page_pulls=max_page_pulls)
        self._reviews_table = ReviewsTableBuilder()
        if start == 0:
            self._reviews_table.build_table()
        
        try:
            self._open_conn()
            self._restaurant_iter = iter(self._get_ids()[start:])
        finally:
            self._close_conn()
 
    def pull_all_restaurant_pages(self):
        pass
    
    def pull_n_restaurant_pages(self, n):
        
        extracts = []
        for i,r_id in enumerate(islice(self._restaurant_iter, n)):
            
            camis = r_id.camis
            yelp_id = r_id.yelp_id
            print yelp_id
            verbose = i % 5 == 0
            if verbose:
                print "Pulling restaurant number {0}...".format(i)
                sys.stdout.flush()
                
            extract = self._paginator.pull_business(yelp_id)
    #       self.restautants_table.update_records(extract) ## Need to implement ##
            extracts.append((yelp_id, extract))
            if verbose:
                print "Pull {0} complete.".format(i)
                sys.stdout.flush()
        
        self._record(extracts)
    
        ########################################################################################################################
        ########################################################################################################################
        ########################################################################################################################
        ###############                             MAKE INTO SEPERATE CLASS                                ####################
        ########################################################################################################################
        ########################################################################################################################
        ########################################################################################################################

    def _get_ids(self):
        
        q = '''
        SELECT camis, yelp_id
        FROM restaurants
        ORDER BY camis ASC;
        '''
        self.c.execute(q)
        restaurant_ids = self.c.fetchall()
        return restaurant_ids
    
    def _record(self, extracts):
        
        self._reviews_table.add_records(extracts)
        
    def _open_conn(self):
        
        self.conn = psycopg2.connect("dbname=yelp", cursor_factory=psycopg2.extras.NamedTupleCursor) #PARAMETERIZE DBNAME?
        self.c = self.conn.cursor()

    def _close_conn(self):
        
        if self.conn is not None:
            self.conn.close()
        self.conn = None
