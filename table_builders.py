import psycopg2
from psycopg2.extras import NamedTupleConnection
from yelp_api_machinery import RestaurantExtract


class TableBuilder:
    '''Baseclases for the classes which build tables in the database.'''
    
    def __init__(self):

        self.conn = None
        self.c = None
    
    
    def _open_conn(self):
        
        self.conn = psycopg2.connect("dbname=yelp", cursor_factory=psycopg2.extras.NamedTupleCursor)
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
        
        text = text.replace("'", "''") if text else 'NULL'
        text = unicode(text)
        return text
    
    def _create_table(self):
        raise NotImplementedError
        
    def _add_records(self, extracts):
        raise NotImplementedError   
        
    def _format_extract(self, extracts):
        raise NotImplementedError

        
####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####


class RestaurantsTableBuilder(TableBuilder):
    
    def __init__(self):
        TableBuilder.__init__(self)
        self.FIELDS = 'camis, yelp_id, yelp_name, yelp_address, yelp_zipcode, yelp_phone, review_count, rating'.split(', ')
    
    def _create_table(self):
        
        # restaurants
        ## (camis, yelp_id, yelp_name, yelp_address, yelp_zipcode, yelp_phone, review_count, rating)
        self.c.execute("DROP TABLE IF EXISTS yelp_restaurants")
        q = '''
        CREATE TABLE yelp_restaurants (
        camis varchar(8),
        yelp_id varchar(80),
        yelp_name varchar(70),
        yelp_address varchar(60),
        yelp_zipcode varchar(5),
        yelp_phone varchar(11),
        review_count smallint,
        rating real
        )
        '''
        self.c.execute(q)
        self.conn.commit()
    
    def _add_records(self, restaurant_extracts):
        '''Expect extract to be of the form (camis, RestaurantExtract)'''
        
        
        q_template = u'''INSERT INTO yelp_restaurants 
        (camis, yelp_id, yelp_name, yelp_address, yelp_zipcode, yelp_phone, review_count, rating)
        VALUES (
        {camis},
        '{yelp_id}',
        '{yelp_name}',
        '{yelp_address}',
        '{yelp_city}',
        {yelp_zipcode},
        {yelp_phone},
        {review_count},
        {rating}
        );
        '''
               
        for extract in restaurant_extracts:
            d = format_extract(extract)
            q = q_template.format(**d)

            self.c.execute(q)
            self.conn.commit()
            
    def _format_extract(self, extract):  
        
        d = {}

        d['camis'] = extract[0]
        r_extract = extract[1]._asdict()
        d.update({k: self._psql_safe_format(v) for k,v in r_extract})
        
        return d  


####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####

class CategoriesTableBuilder(TableBuilder):

    def __init__(self):
        TableBuilder.__init__(self)
    
    def _create_table(self):
        
        # categories
        ## (yeld_id, category)
        self.c.execute("DROP TABLE IF EXISTS categories")
        q = '''
        CREATE TABLE categories (
        yelp_id varchar(80),
        yelp_category varchar(35)
        )
        '''
        self.c.execute(q)
        self.conn.commit()
        
    def _add_records(self, extracts):
        '''Expects extract to be of the form (camis, RestaurantExtract), 
        where RestaurantExtract.categories is the list of categories.'''
        
        q_template = u'''INSERT INTO categories 
        (yelp_id, category)
        VALUES (
        '{yelp_id}',
        '{category}'
        );
        '''
        
        for extract in extracts:
            
            ds = self._format_extract(extract)
            for d in ds:
                q = q_template.format(**d)
                self.c.execute(q)

        self.conn.commit()
            
    def _format_extract(self, extract):  
        
        r_extract = extract[1]
        yelp_id = self._psql_safe_format(r_extract.yelp_id)
        
        ds = []
        for category in r_extract.categories:
            
            d = {}
            d['yelp_id'] = yelp_id
            d['category'] = self._psql_safe_string_format(category)
            
            ds.append(d)
        
        return ds

    
####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####

class NeighborhoodsTableBuilder(TableBuilder):

    def __init__(self):
        TableBuilder.__init__(self)
    
    def _create_table(self):
        
        # neighborhoods
        ## (yelp_id, neighborhood)
        self.c.execute("DROP TABLE IF EXISTS neighborhoods")
        q = '''
        CREATE TABLE neighborhoods (
        yelp_id varchar(80),
        neighborhood varchar(40)
        )
        '''
        self.c.execute(q)
        self.conn.commit()
        
    def _add_records(self, extracts):
        
        q_template = u'''INSERT INTO neighborhoods 
        (yelp_id, neighborhood)
        VALUES (
        '{yelp_id}',
        '{neighborhood}'
        );
        '''
        
        for extract in extracts:
            
            ds = self._format_extract(extract)
            for d in ds:
                q = q_template.format(**d)
                self.c.execute(q)

        self.conn.commit()
             
        
    def _format_extract(self, extract):  
        
        r_extract = extract[1]
        yelp_id = self._psql_safe_format(r_extract.yelp_id)
        
        ds = []
        for neighborhood in r_extract.neighborhoods:
            
            d = {}
            d['yelp_id'] = yelp_id
            d['neighborhood'] = self._psql_safe_format(category)
            
            ds.append(d)
        
        return ds


####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------------------------####

    
class ReviewsTableBuilder(TableBuilder):
    
    def __init__(self):
        TableBuilder.__init__(self)
    
    def _create_table(self):
        
        # reviews
        ## (yelp_id, date, rating, review)
        self.c.execute("DROP TABLE IF EXISTS reviews")
        q = '''
        CREATE TABLE reviews (
        yelp_id varchar(80),
        date date,
        rating real,
        review varchar(5000)
        )
        '''
        self.c.execute(q)
        self.conn.commit()        
    
    def _table_exists(self):
        
        self.c.execute("select exists(select * from information_schema.tables where table_name='reviews');")
        return self.c.fetchone()[0]
    
    def _add_records(self, restaurant_extracts):
    
        if not self._table_exists():
            self._create_table()
    
        q_template = u'''INSERT INTO reviews 
        (yelp_id, date, rating, review)
        VALUES (
        '{yelp_id}',
        to_date('{date}','YYYY-MM-DD'),
        {rating},
        '{review}'
        );
        '''
        
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
                text = self._psql_safe_format(text)
                q = q_template.format(yelp_id = yelp_id, date = date, rating = rating, review = text)
                self.c.execute(q)

        self.conn.commit()
    

        