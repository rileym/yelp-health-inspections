# -*- coding: utf-8 -*-

import psycopg2
from operator import itemgetter
from psycopg2.extras import NamedTupleConnection
from yelp_api_machinery import RestaurantYelpExtract
from inspection_data_machinery import DohInspectionExtract
from constants import DB_NAME, DOH_RESTAURANTS_TABLE_NAME, DOH_INSPECTIONS_TABLE_NAME, \
                        YELP_RESTAURANTS_TABLE_NAME, YELP_CATEGORIES_TABLE_NAME, \
                        YELP_REVIEWS_TABLE_NAME, YELP_NEIGHBORHOODS_TABLE_NAME


####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####


class TableBuilder:
    '''Baseclases for the classes which build database tables 
    from ______Extract named tuples.'''
    
    def __init__(self):

        self.conn = None
        self.c = None
    
    
    def _open_conn(self):
        
        self.conn = psycopg2.connect("dbname={db_name}".format(db_name = DB_NAME), 
                                        cursor_factory=psycopg2.extras.NamedTupleCursor)
        self.c = self.conn.cursor()

        
    def _close_conn(self):
        
        if self.conn is not None:
            self.conn.close()
        self.conn = None
        
        
    def build_table(self, extracts = None):
        
        try:

            self._open_conn()
            self._create_table()
            if extracts is not None:
                self._add_records(extracts)
        
        finally:
            self._close_conn()
    
    
    def add_records(self, extracts):
        
        try:
            self._open_conn()
            self._add_records(extracts)
        
        finally:
            self._close_conn()
    
    def _psql_safe_format(self, text):
        
        # try:
            # text = text.replace("'", "''") if text else None
        # return text if text != '' else None

        # except AttributeError:
        #     pass

        return text            

    def _format_extract(self, extract):  
        
        # d = {k: self._psql_safe_format(v) for k,v in extract._asdict().iteritems()}
        d = extract._asdict()
        return d
    
    def _create_table(self):
        raise NotImplementedError
        
    def _add_records(self, extracts):
        raise NotImplementedError   


####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####


class DohRestaurantsTableBuilder(TableBuilder):


    def _create_table(self):

        self.c.execute("DROP TABLE IF EXISTS {doh_restaurants_table_name} CASCADE".format(doh_restaurants_table_name=DOH_RESTAURANTS_TABLE_NAME))
        self.c.execute(
                '''CREATE TABLE {doh_restaurants_table_name} (
                    doh_camis varchar(10) PRIMARY KEY,
                    doh_dba varchar(255),
                    doh_address  varchar(100),
                    doh_zipcode varchar(5),
                    doh_phone varchar(12)
                )
                '''.format(doh_restaurants_table_name = DOH_RESTAURANTS_TABLE_NAME)
        )
        self.conn.commit()        


    def _add_records(self, inspection_extracts):
        '''Expect extract to be of the form (doh_camis, RestaurantYelpExtract)'''
        
        
        q_template = u'''INSERT INTO {doh_restaurants_table_name} 
                        (doh_camis, doh_dba, doh_address, doh_zipcode, doh_phone)
                        VALUES (
                            %(doh_camis)s,
                            %(doh_dba)s,
                            %(doh_address)s,
                            %(doh_zipcode)s,
                            %(doh_phone)s
                        );
                        '''.format(doh_restaurants_table_name = DOH_RESTAURANTS_TABLE_NAME)
        seen = set() 

        unique_restaurant_extracts = []            
        for extract in inspection_extracts:

            d = self._format_extract(extract) 

            if d.get('doh_camis') not in seen:
            
                unique_restaurant_extracts.append(d)
                seen.add(d.get('doh_camis'))

        self.c.executemany(q_template, unique_restaurant_extracts)
        self.conn.commit()



####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####

class DohInspectionsTableBuilder(TableBuilder):


    def _create_table(self):

        self.c.execute("DROP TABLE IF EXISTS {doh_inspections_table_name} CASCADE".format(doh_inspections_table_name = DOH_INSPECTIONS_TABLE_NAME))
        self.c.execute(
                '''
                CREATE TABLE {doh_inspections_table_name} (
                    doh_camis varchar(10) REFERENCES {doh_restaurants_table_name},
                    doh_inspection_type varchar(64),
                    doh_inspection_date date,
                    doh_action varchar(150),
                    doh_score smallint,
                    doh_grade varchar(1),
                    doh_grade_date date,
                    doh_violation_code varchar(3),
                    doh_critical_flag varchar(1)
                )
                '''.format(doh_inspections_table_name = DOH_INSPECTIONS_TABLE_NAME,
                            doh_restaurants_table_name = DOH_RESTAURANTS_TABLE_NAME)
        )
        self.conn.commit()        


    def _add_records(self, inspection_extracts):
        '''Expect extract to be of the form (doh_camis, RestaurantYelpExtract)'''
        
        
        q_template = u'''INSERT INTO {doh_inspections_table_name} 
                        (doh_camis, doh_inspection_type, doh_inspection_date, doh_action, doh_score, doh_grade, doh_grade_date, doh_violation_code, doh_critical_flag)
                        VALUES (
                            %(doh_camis)s,
                            %(doh_inspection_type)s,
                            %(doh_inspection_date)s,
                            %(doh_action)s,
                            %(doh_score)s,
                            %(doh_grade)s,
                            %(doh_grade_date)s,
                            %(doh_violation_code)s,
                            %(doh_critical_flag)s
                        );
                        '''.format(doh_inspections_table_name = DOH_INSPECTIONS_TABLE_NAME)
             
        formatted_extracts = [self._format_extract(extract) for extract in inspection_extracts]
        
        self.c.executemany(q_template, formatted_extracts)

        self.conn.commit()


####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####


class YelpRestaurantsTableBuilder(TableBuilder):
    
    def __init__(self):
        TableBuilder.__init__(self)
    
    def _create_table(self):
        
        self.c.execute("DROP TABLE IF EXISTS {yelp_restaurants_table_name} CASCADE".format(yelp_restaurants_table_name = YELP_RESTAURANTS_TABLE_NAME))
        q = '''
                CREATE TABLE {yelp_restaurants_table_name} (
                    doh_camis varchar(8) PRIMARY KEY REFERENCES {doh_restaurants_table_name},
                    yelp_id varchar(80),
                    yelp_name varchar(70),
                    yelp_address varchar(60),
                    yelp_city varchar(35),
                    yelp_zipcode varchar(5),
                    yelp_phone varchar(11),
                    yelp_review_count smallint,
                    yelp_rating real
                )
                '''.format(yelp_restaurants_table_name = YELP_RESTAURANTS_TABLE_NAME,
                            doh_restaurants_table_name = DOH_RESTAURANTS_TABLE_NAME)

        self.c.execute(q)
        self.conn.commit()
    
    def _add_records(self, restaurant_extracts):
        '''Expect extract to be of the form (Record, RestaurantYelpExtract)'''

        q_template = u'''INSERT INTO {yelp_restaurants_table_name} 
                        (doh_camis, yelp_id, yelp_name, yelp_address, yelp_city, yelp_zipcode, yelp_phone, yelp_review_count, yelp_rating)
                        VALUES (
                            %(doh_camis)s,
                            %(yelp_id)s,
                            %(yelp_name)s,
                            %(yelp_address)s,
                            %(yelp_city)s,
                            %(yelp_zipcode)s,
                            %(yelp_phone)s,
                            %(yelp_review_count)s,
                            %(yelp_rating)s
                        );
                        '''.format(yelp_restaurants_table_name = YELP_RESTAURANTS_TABLE_NAME)
               
        
        # restaurant_extracts = filter(itemgetter(1), restaurant_extracts) 

        for extract in restaurant_extracts:
            d = self._format_extract(extract)
            self.c.execute(q_template, d)
        
        self.conn.commit()

            
    def _format_extract(self, extract):  
        
        # restaurant_extract = extract[1]._asdict()
        # d = {k: self._psql_safe_format(v) for k,v in restaurant_extract.iteritems()}
        d = extract[1]._asdict()
        d['doh_camis'] = extract[0].doh_camis

        return d  


####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####


class YelpCategoriesTableBuilder(TableBuilder):

    def __init__(self):
        TableBuilder.__init__(self)
    
    def _create_table(self):
        
        self.c.execute("DROP TABLE IF EXISTS {yelp_categories_table_name} CASCADE".format(
                        yelp_categories_table_name = YELP_CATEGORIES_TABLE_NAME))
        q = '''
            CREATE TABLE {yelp_categories_table_name} (
                doh_camis varchar(8) REFERENCES {doh_restaurants_table_name},
                yelp_category varchar(35)
            )
            '''.format(yelp_categories_table_name = YELP_CATEGORIES_TABLE_NAME,
                       doh_restaurants_table_name = DOH_RESTAURANTS_TABLE_NAME)

        self.c.execute(q)
        self.conn.commit()
        
    def _add_records(self, extracts):
        ''' Expects extract to be of the form (Record, RestaurantYelpExtract), 
        where RestaurantYelpExtract.categories is the list of categories.'''
        
        q_template = u'''INSERT INTO {yelp_categories_table_name} 
                        (doh_camis, yelp_category)
                        VALUES (
                            %(doh_camis)s,
                            %(yelp_category)s
                        );
                        '''.format(yelp_categories_table_name = YELP_CATEGORIES_TABLE_NAME)
        
        # extracts = filter(itemgetter(1), extracts)        
        for extract in extracts:
            
            ds = self._format_extract(extract)
            self.c.executemany(q_template, ds)

        self.conn.commit()
            
    def _format_extract(self, extract):  
        
        doh_camis = extract[0].doh_camis
        
        ds = []
        for category in extract[1].yelp_categories:
            
            d = {}
            d['doh_camis'] = doh_camis
            d['yelp_category'] = category
            # d['yelp_category'] = self._psql_safe_format(category)
            
            ds.append(d)
        
        return ds

    
####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####


class YelpNeighborhoodsTableBuilder(TableBuilder):

    def __init__(self):
        TableBuilder.__init__(self)
    
    def _create_table(self):
        
        self.c.execute("DROP TABLE IF EXISTS {yelp_neighborhoods_table_name} CASCADE".format(yelp_neighborhoods_table_name = YELP_NEIGHBORHOODS_TABLE_NAME))
        q = '''
            CREATE TABLE {yelp_neighborhoods_table_name} (
                doh_camis varchar(8) REFERENCES {doh_restaurants_table_name},
                yelp_neighborhood varchar(40)
            )
            '''.format(yelp_neighborhoods_table_name = YELP_NEIGHBORHOODS_TABLE_NAME,
                        doh_restaurants_table_name = DOH_RESTAURANTS_TABLE_NAME)
        self.c.execute(q)
        self.conn.commit()
        
    def _add_records(self, extracts):
        
        q_template = u'''INSERT INTO {yelp_neighborhoods_table_name} 
                        (doh_camis, yelp_neighborhood)
                        VALUES (
                            %(doh_camis)s,
                            %(yelp_neighborhood)s
                        );
                        '''.format(yelp_neighborhoods_table_name = YELP_NEIGHBORHOODS_TABLE_NAME)
        
        # extracts = filter(itemgetter(1), extracts)        
        for extract in extracts:
            
            ds = self._format_extract(extract)
            self.c.executemany(q_template, ds)

        self.conn.commit()
             
        
    def _format_extract(self, extract):  
        
        doh_camis = extract[0].doh_camis
        
        ds = []
        for neighborhood in extract[1].yelp_neighborhoods:
            
            d = {}
            d['doh_camis'] = doh_camis
            d['yelp_neighborhood'] = self._psql_safe_format(neighborhood)
            
            ds.append(d)
        
        return ds


####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####

    
class YelpReviewsTableBuilder(TableBuilder):
    
    def __init__(self):
        TableBuilder.__init__(self)
    
    def _create_table(self):
        
        # reviews
        ## (yelp_id, date, rating, review)
        self.c.execute("DROP TABLE IF EXISTS {yelp_reviews_table_name} CASCADE".format(
                        yelp_reviews_table_name = YELP_REVIEWS_TABLE_NAME))
        q = '''
            CREATE TABLE {yelp_reviews_table_name} (
                yelp_id varchar(80),
                yelp_date date,
                yelp_rating real,
                yelp_review varchar(5000)
            )
            '''.format(yelp_restaurants_table_name = YELP_REVIEWS_TABLE_NAME)
        self.c.execute(q)
        self.conn.commit()        
    
    def _table_exists(self):
        
        self.c.execute("select exists(select * from information_schema.tables where table_name='reviews');")
        return self.c.fetchone()[0]
    
    def _add_records(self, restaurant_extracts):
    
        if not self._table_exists():
            self._create_table()
    
        q_template = u'''INSERT INTO {yelp_reviews_table_name} 
                        (yelp_id, yelp_date, yelp_rating, yelp_review)
                        VALUES (
                            %(yelp_id)s,
                            %(yelp_date)s,
                            %(yelp_rating)s,
                            %(yelp_review)s
                        );
                        '''.format(yelp_reviews_table_name = YELP_REVIEWS_TABLE_NAME)
        
        if not isinstance(restaurant_extracts, list):
            restaurant_extracts = [restaurant_extracts]
        
        
        for restaurant_extract in restaurant_extracts:
        
            yelp_id = restaurant_extract[0]
            pages_extract = restaurant_extract[1]
            reviews = pages_extract.reviews
            for review in reviews:

                date = review[0]
                rating = review[1]
                text = review[2]
                # text = self._psql_safe_format(text)
                q = q_template.format(yelp_id = yelp_id, 
                                    date = date, 
                                    rating = rating, 
                                    review = text,
                                    yelp_reviews_table_name = YELP_REVIEWS_TABLE_NAME)
                self.c.execute(q)

        self.conn.commit()        


    

        

