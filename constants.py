

# inspection data
RAW_INSPECTION_CSV_URL = 'https://data.cityofnewyork.us/api/views/xx67-kt59/rows.csv?accessType=DOWNLOAD'
RAW_INSPECTION_CSV_PATH = './raw_inspection_data.csv'
CLEAN_INSPECTION_CSV_PATH = './cleaned_inspection_data.csv'

# yelp api
SEARCH_ADDR_BASE_URL = 'http://api.yelp.com/v2/search'
SEARCH_PHONE_BASE_URL = 'http://api.yelp.com/v2/phone_search'

MAX_GET_ATTEMPTS = 3

# extract matchers

LOWER_SIMILARITY_THRESHOLD = .70
UPPER_SIMILARITY_THRESHOLD = .80

ADDRESS_SCORE_WEIGHT = .75
NAME_SCORE_WEIGHT = .25

# database

DB_NAME = 'yelp'

DOH_INSPECTIONS_TABLE_NAME = 'doh_inspections'
DOH_RESTAURANTS_TABLE_NAME = 'doh_restaurants'

YELP_RESTAURANTS_TABLE_NAME = 'yelp_restaurants'
YELP_CATEGORIES_TABLE_NAME = 'yelp_categories'
YELP_REVIEWS_TABLE_NAME = 'yelp_reviews'
YELP_NEIGHBORHOODS_TABLE_NAME = 'yelp_neighborhoods'
