# -*- coding: utf-8 -*-

import psycopg2
from operator import itemgetter
import itertools
from psycopg2.extras import NamedTupleConnection
from yelp_api_machinery import RestaurantYelpExtract
from inspection_data_machinery import DohInspectionExtract
from constants import   DB_NAME, 
                        DOH_RESTAURANTS_TABLE_NAME, 
                        DOH_INSPECTIONS_TABLE_NAME, \
                        YELP_RESTAURANTS_TABLE_NAME, 
                        YELP_CATEGORIES_TABLE_NAME, \
                        YELP_REVIEWS_TABLE_NAME, 
                        YELP_NEIGHBORHOODS_TABLE_NAME


####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####

def DBConnectionManager(object):

    def __enter__(self):

        self.conn = psycopg2.connect( "dbname={db_name}".format(db_name = DB_NAME), 
                                        cursor_factory=psycopg2.extras.NamedTupleCursor)

    def __exit__(self, type, value, traceback):
        self.conn.__exit__(type, value, traceback)
        self.conn.close()



class TableBuilder(object):
    '''Baseclases for the classes which build database tables 
    from ______Extract named tuples.'''
    
    # def _open_conn(self):
        
    #     self.conn = psycopg2.connect("dbname={db_name}".format(db_name = DB_NAME), 
    #                                     cursor_factory=psycopg2.extras.NamedTupleCursor)
    #     self.c = self.conn.cursor()

        
    # def _close_conn(self):
        
    #     if self.conn is not None:
    #         self.conn.close()
    #     self.conn = None

    # def build_table(self, extracts = None):
        
    #     try:

    #         self._open_conn()
    #         self._create_table()
    #         if extracts is not None:
    #             self._add_records(extracts)
        
    #     finally:
    #         self._close_conn()  

    def create_table(self):

        with DBConnContextManager() as cm:
            with cm.conn.cursor() as c:
                c.execute(self.drop_table_q)
                c.execute(self.create_table_q)

    
    def add_records(self, extracts):
        
        with DBConnContextManager() as cm:
            with cm.conn.cursor() as c:

                q = self.insert_records_q_template
                formatted_extracts = self.format_extracts(extracts)
                c.executemany(q, formatted_extracts)
        
    def _format_extract(self, extract):  
        
        d = extract._asdict()
        return d

    
    def _format_extracts(self, extracts):

        return map(self._format_extract, extracts)

    # def _create_table(self, conn):
    #     raise NotImplementedError
        

    # def _add_records(self, conn, extracts):
    #     raise NotImplementedError   

    


class NestedExtractTableBuilder(TableBuilder):
    

    def _format_extracts(self, extracts):

        return itertools.chain(*map(self._format_extract, extracts))  


# abstract out repeated execute many logic in _add_record andd create_table:
#               self.c.executemany(q_template, formatted_extracts)
#               self.conn.commit()

# YelpTableBuilder
# DohTableBuilder


####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####


class DohRestaurantsTableBuilder(DOHTableBuilder):


    create_table_q = '''
                        CREATE TABLE {doh_restaurants_table_name} (
                            doh_camis varchar(10) PRIMARY KEY,
                            doh_dba varchar(255),
                            doh_address varchar(100),
                            doh_zipcode varchar(5),
                            doh_phone varchar(12)
                        );
                        '''.format(doh_restaurants_table_name = DOH_RESTAURANTS_TABLE_NAME)

    drop_table_q = "DROP TABLE IF EXISTS {doh_restaurants_table_name} CASCADE;".format(doh_restaurants_table_name=DOH_RESTAURANTS_TABLE_NAME)

    insert_records_q_template = '''
                                INSERT INTO {doh_restaurants_table_name} 
                                    (doh_camis, doh_dba, doh_address, doh_zipcode, doh_phone)
                                    VALUES (
                                        %(doh_camis)s,
                                        %(doh_dba)s,
                                        %(doh_address)s,
                                        %(doh_zipcode)s,
                                        %(doh_phone)s
                                    );
                                '''.format(doh_restaurants_table_name = DOH_RESTAURANTS_TABLE_NAME)

    # def __init__(self):

    #     TableBuilder.__init__(self)

    # def _create_table(self, conn):

    #     c = conn.cursor()
    #     with conn.cursor() as c:

    #         c.execute("DROP TABLE IF EXISTS {doh_restaurants_table_name} CASCADE".format(doh_restaurants_table_name=DOH_RESTAURANTS_TABLE_NAME))
    #         c.execute(
    #                     '''CREATE TABLE {doh_restaurants_table_name} (
    #                         doh_camis varchar(10) PRIMARY KEY,
    #                         doh_dba varchar(255),
    #                         doh_address  varchar(100),
    #                         doh_zipcode varchar(5),
    #                         doh_phone varchar(12)
    #                     )
    #                     '''.format(doh_restaurants_table_name = DOH_RESTAURANTS_TABLE_NAME)
    #         )     


    # def _add_records(self, conn, inspection_extracts):
    #     '''Expect extract to be of the form (doh_camis, RestaurantYelpExtract)'''
        

    #     q_template = u'''INSERT INTO {doh_restaurants_table_name} 
    #                     (doh_camis, doh_dba, doh_address, doh_zipcode, doh_phone)
    #                     VALUES (
    #                         %(doh_camis)s,
    #                         %(doh_dba)s,
    #                         %(doh_address)s,
    #                         %(doh_zipcode)s,
    #                         %(doh_phone)s
    #                     );
    #                     '''.format(doh_restaurants_table_name = DOH_RESTAURANTS_TABLE_NAME)


    #     with conn.cursor() as c:
    #         c.executemany(q_template, unique_restaurant_extracts)

    def _format_extracts(self, extracts):

        seen = set() 

        unique_restaurant_extracts = []            
        for extract in inspection_extracts:

            d = self._format_extract(extract) 

            if d.get('doh_camis') not in seen:
            
                unique_restaurant_extracts.append(d)
                seen.add(d.get('doh_camis'))

        return unique_restaurant_extracts


####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####

class DohInspectionsTableBuilder(DOHTableBuilder):


        create_table_q = '''
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
                              

        drop_table_q = "DROP TABLE IF EXISTS {doh_inspections_table_name} CASCADE".format(doh_inspections_table_name = DOH_INSPECTIONS_TABLE_NAME)

        insert_records_q_template = u'''
                                    INSERT INTO {doh_inspections_table_name} 
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

    # def __init__(self):

    #     TableBuilder.__init__(self)


    # def _create_table(self, conn):

    #     with conn.cursor() as c:

    #         c.execute("DROP TABLE IF EXISTS {doh_inspections_table_name} CASCADE".format(doh_inspections_table_name = DOH_INSPECTIONS_TABLE_NAME))
    #         c.execute(
    #                 '''
    #                 CREATE TABLE {doh_inspections_table_name} (
    #                     doh_camis varchar(10) REFERENCES {doh_restaurants_table_name},
    #                     doh_inspection_type varchar(64),
    #                     doh_inspection_date date,
    #                     doh_action varchar(150),
    #                     doh_score smallint,
    #                     doh_grade varchar(1),
    #                     doh_grade_date date,
    #                     doh_violation_code varchar(3),
    #                     doh_critical_flag varchar(1)
    #                 )
    #                 '''.format(doh_inspections_table_name = DOH_INSPECTIONS_TABLE_NAME,
    #                             doh_restaurants_table_name = DOH_RESTAURANTS_TABLE_NAME)
    #         )    

    # def _add_records(self, conn, inspection_extracts):
    #     '''Expect extract to be of the form (doh_camis, RestaurantYelpExtract)'''
        
        
    #     q_template = u'''INSERT INTO {doh_inspections_table_name} 
    #                     (doh_camis, doh_inspection_type, doh_inspection_date, doh_action, doh_score, doh_grade, doh_grade_date, doh_violation_code, doh_critical_flag)
    #                     VALUES (
    #                         %(doh_camis)s,
    #                         %(doh_inspection_type)s,
    #                         %(doh_inspection_date)s,
    #                         %(doh_action)s,
    #                         %(doh_score)s,
    #                         %(doh_grade)s,
    #                         %(doh_grade_date)s,
    #                         %(doh_violation_code)s,
    #                         %(doh_critical_flag)s
    #                     );
    #                     '''.format(doh_inspections_table_name = DOH_INSPECTIONS_TABLE_NAME)
             
    #     formatted_extracts = map([self._format_extract(extract) for extract in inspection_extracts]
        
    #     with conn.cursor() as c:
    #         c.executemany(q_template, formatted_extracts)


####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####


class YelpRestaurantsTableBuilder(TableBuilder):

    create_table_q = '''
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

    drop_table_q = "DROP TABLE IF EXISTS {yelp_restaurants_table_name} CASCADE".format(yelp_restaurants_table_name = YELP_RESTAURANTS_TABLE_NAME)

    insert_records_q_template = '''
                                INSERT INTO {yelp_restaurants_table_name} 
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
    
    # def __init__(self):

    #     TableBuilder.__init__(self)


    # def _create_table(self, conn):
        
    #     q = '''
    #             CREATE TABLE {yelp_restaurants_table_name} (
    #                 doh_camis varchar(8) PRIMARY KEY REFERENCES {doh_restaurants_table_name},
    #                 yelp_id varchar(80),
    #                 yelp_name varchar(70),
    #                 yelp_address varchar(60),
    #                 yelp_city varchar(35),
    #                 yelp_zipcode varchar(5),
    #                 yelp_phone varchar(11),
    #                 yelp_review_count smallint,
    #                 yelp_rating real
    #             )
    #             '''.format(yelp_restaurants_table_name = YELP_RESTAURANTS_TABLE_NAME,
    #                         doh_restaurants_table_name = DOH_RESTAURANTS_TABLE_NAME)

    #     with conn.cursor() as c:
    #         c.execute("DROP TABLE IF EXISTS {yelp_restaurants_table_name} CASCADE".format(yelp_restaurants_table_name = YELP_RESTAURANTS_TABLE_NAME))
    #         c.execute(q)
    

    # def _add_records(self, restaurant_extracts):
    #     '''Expect extract to be of the form (Record, RestaurantYelpExtract)'''

    #     q_template = u'''INSERT INTO {yelp_restaurants_table_name} 
    #                     (doh_camis, yelp_id, yelp_name, yelp_address, yelp_city, yelp_zipcode, yelp_phone, yelp_review_count, yelp_rating)
    #                     VALUES (
    #                         %(doh_camis)s,
    #                         %(yelp_id)s,
    #                         %(yelp_name)s,
    #                         %(yelp_address)s,
    #                         %(yelp_city)s,
    #                         %(yelp_zipcode)s,
    #                         %(yelp_phone)s,
    #                         %(yelp_review_count)s,
    #                         %(yelp_rating)s
    #                     );
    #                     '''.format(yelp_restaurants_table_name = YELP_RESTAURANTS_TABLE_NAME)
               

    #     formatted_extracts = [self._format_extract(extract) for extract in restaurant_extracts]

    #     with conn.cursor() as c:
    #         c.executemany(q_template, formatted_extracts)

    def _format_extract(self, extract):  
        
        d = extract[1]._asdict()
        d['doh_camis'] = extract[0].doh_camis

        return d  


####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####


class YelpCategoriesTableBuilder(NestedExtractTableBuilder):

        create_table_q = '''
                            CREATE TABLE {yelp_categories_table_name} (
                                doh_camis varchar(8) REFERENCES {doh_restaurants_table_name},
                                yelp_category varchar(35)
                            )
                            '''.format(yelp_categories_table_name = YELP_CATEGORIES_TABLE_NAME,
                                       doh_restaurants_table_name = DOH_RESTAURANTS_TABLE_NAME)

        drop_table_q = 'DROP TABLE IF EXISTS {yelp_categories_table_name} CASCADE'.format(
                                        yelp_categories_table_name = YELP_CATEGORIES_TABLE_NAME)

        insert_records_q_template =  u'''
                                        INSERT INTO {yelp_categories_table_name} 
                                        (doh_camis, yelp_category)
                                        VALUES (
                                            %(doh_camis)s,
                                            %(yelp_category)s
                                        );
                                        '''.format(yelp_categories_table_name = YELP_CATEGORIES_TABLE_NAME)

    def __init__(self):

        TableBuilder.__init__(self)                          
    
    # def _create_table(self, conn):
        

    #     with conn.cursor() as c:
    #         c.execute(self.drop_table_q)
    #         c.execute(self.create_table_q)

        
    # def _add_records(self, conn, extracts):
    #     ''' Expects extract to be of the form (Record, RestaurantYelpExtract), 
    #     where RestaurantYelpExtract.categories is the list of categories.'''
        
    #     q_template = u'''INSERT INTO {yelp_categories_table_name} 
    #                     (doh_camis, yelp_category)
    #                     VALUES (
    #                         %(doh_camis)s,
    #                         %(yelp_category)s
    #                     );
    #                     '''.format(yelp_categories_table_name = YELP_CATEGORIES_TABLE_NAME)

    #     with conn.cursor() as c:

    #         for extract in extracts:
    #             ds = self._format_extract(extract)
    #             c.executemany(q_template, ds)

            
    def _format_extract(self, extract):  
        
        doh_camis = extract[0].doh_camis
        
        ds = []
        for category in extract[1].yelp_categories:
            
            d = {}
            d['doh_camis'] = doh_camis
            d['yelp_category'] = category
            
            ds.append(d)
        
        return ds

    
####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####


class YelpNeighborhoodsTableBuilder(NestedExtractTableBuilder):

        create_table_q = '''
                            CREATE TABLE {yelp_neighborhoods_table_name} (
                                doh_camis varchar(8) REFERENCES {doh_restaurants_table_name},
                                yelp_neighborhood varchar(40)
                            )
                        '''.format(yelp_neighborhoods_table_name = YELP_NEIGHBORHOODS_TABLE_NAME,
                                        doh_restaurants_table_name = DOH_RESTAURANTS_TABLE_NAME)

        drop_table_q = "DROP TABLE IF EXISTS {yelp_neighborhoods_table_name} CASCADE".format(yelp_neighborhoods_table_name = YELP_NEIGHBORHOODS_TABLE_NAME)

        insert_records_q_template = u'''
                                        INSERT INTO {yelp_neighborhoods_table_name} 
                                        (doh_camis, yelp_neighborhood)
                                        VALUES (
                                            %(doh_camis)s,
                                            %(yelp_neighborhood)s
                                        );
                                    '''.format(yelp_neighborhoods_table_name = YELP_NEIGHBORHOODS_TABLE_NAME)


    # def __init__(self):

    #     TableBuilder.__init__(self)
    

    # def _create_table(self, conn):
            
    #     q = '''
    #         CREATE TABLE {yelp_neighborhoods_table_name} (
    #             doh_camis varchar(8) REFERENCES {doh_restaurants_table_name},
    #             yelp_neighborhood varchar(40)
    #         )
    #         '''.format(yelp_neighborhoods_table_name = YELP_NEIGHBORHOODS_TABLE_NAME,
    #                     doh_restaurants_table_name = DOH_RESTAURANTS_TABLE_NAME)

    #     with conn.cursor() as c:
    #         c.execute("DROP TABLE IF EXISTS {yelp_neighborhoods_table_name} CASCADE".format(yelp_neighborhoods_table_name = YELP_NEIGHBORHOODS_TABLE_NAME))
    #         c.execute(q)

        
    def _add_records(self, conn, extracts):
        
        q_template = u'''INSERT INTO {yelp_neighborhoods_table_name} 
                        (doh_camis, yelp_neighborhood)
                        VALUES (
                            %(doh_camis)s,
                            %(yelp_neighborhood)s
                        );
                        '''.format(yelp_neighborhoods_table_name = YELP_NEIGHBORHOODS_TABLE_NAME)
             
        
        with conn.cursor() as c:
            for extract in extracts:
                
                ds = self._format_extract(extract)
                c.executemany(q_template, ds)
             
        
    def _format_extract(self, extract):  
        
        doh_camis = extract[0].doh_camis
        
        ds = []
        for neighborhood in extract[1].yelp_neighborhoods:
            
            d = {}
            d['doh_camis'] = doh_camis
            d['yelp_neighborhood'] = neighborhood
            
            ds.append(d)
        
        return ds



####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####

    
class YelpReviewsTableBuilder(NestedExtractTableBuilder):
    
        create_table_q = '''
                            CREATE TABLE {yelp_reviews_table_name} (
                                yelp_id varchar(80),
                                yelp_date date,
                                yelp_rating real,
                                yelp_review varchar(5000)
                            )
                            '''.format(yelp_restaurants_table_name = YELP_REVIEWS_TABLE_NAME)

        drop_table_q = "DROP TABLE IF EXISTS {yelp_reviews_table_name} CASCADE".format(
                            yelp_reviews_table_name = YELP_REVIEWS_TABLE_NAME)    

        insert_records_q_template = u'''
                                    INSERT INTO {yelp_reviews_table_name} 
                                    (yelp_id, yelp_date, yelp_rating, yelp_review)
                                    VALUES (
                                        %(yelp_id)s,
                                        %(yelp_date)s,
                                        %(yelp_rating)s,
                                        %(yelp_review)s
                                    );
                                    '''.format(yelp_reviews_table_name = YELP_REVIEWS_TABLE_NAME)


    def _format_extract(self, restaurant_extract):


        yelp_id = restaurant_extract[0]
        pages_extract = restaurant_extract[1]

        ds = []

        reviews = pages_extract.reviews
        for review in reviews:

            date = review[0]
            rating = review[1]
            text = review[2]

            d = dict(   yelp_id = yelp_id, 
                        date = date, 
                        rating = rating, 
                        review = text)

            ds.append(d)

        return ds                                    

    # def _create_table(self, conn):
        
    #     # reviews
    #     ## (yelp_id, date, rating, review)

    #     q = '''
    #         CREATE TABLE {yelp_reviews_table_name} (
    #             yelp_id varchar(80),
    #             yelp_date date,
    #             yelp_rating real,
    #             yelp_review varchar(5000)
    #         )
    #         '''.format(yelp_restaurants_table_name = YELP_REVIEWS_TABLE_NAME)

    #     with conn.cursor() as c:
    #         c.execute("DROP TABLE IF EXISTS {yelp_reviews_table_name} CASCADE".format(
    #                     yelp_reviews_table_name = YELP_REVIEWS_TABLE_NAME))
    #         c.execute(q)

    
    # def _table_exists(self, conn): #IS THIS A HACK? THERE MUST BE A BETTER WAY
        
    #     with conn.cursor() as c:
            
    #         q = "SELECT EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{yelp_reviews_table_name}');".format(yelp_reviews_table_name=YELP_REVIEWS_TABLE_NAME)
    #         c.execute(q)
    #         return c.fetchone()[0]
    

    # def _add_records(self, conn, restaurant_extracts):
    
    #     if not self._table_exists(conn):
    #         self._create_table()
    
        # q_template = u'''INSERT INTO {yelp_reviews_table_name} 
        #                 (yelp_id, yelp_date, yelp_rating, yelp_review)
        #                 VALUES (
        #                     %(yelp_id)s,
        #                     %(yelp_date)s,
        #                     %(yelp_rating)s,
        #                     %(yelp_review)s
        #                 );
        #                 '''.format(yelp_reviews_table_name = YELP_REVIEWS_TABLE_NAME)
        
    #     if not isinstance(restaurant_extracts, list): # THERE MUST BE A BETTER WAY
    #         restaurant_extracts = [restaurant_extracts]
        
        
    #     with conn.cursor() as c:

    #         for restaurant_extract in restaurant_extracts: # NAMED TUPLE MAYBE? THERE MUST BE A BETTER WAY
            
    #             yelp_id = restaurant_extract[0]
    #             pages_extract = restaurant_extract[1]
    #             reviews = pages_extract.reviews
    #             for review in reviews:

    #                 date = review[0]
    #                 rating = review[1]
    #                 text = review[2]

    #                 q = q_template.format(yelp_id = yelp_id, 
    #                                     date = date, 
    #                                     rating = rating, 
    #                                     review = text,
    #                                     yelp_reviews_table_name = YELP_REVIEWS_TABLE_NAME)
    #                 self.c.execute(q)

    

        

