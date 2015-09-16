# -*- coding: utf-8 -*-

from yelp_api_machinery import YelpApiPhoneInterfacer, YelpApiFirstPassCoordinator, \
                                YelpApiAddressInterfacer, YelpApiSecondPassCoordinator

from table_builders import YelpRestaurantsTableBuilder, YelpCategoriesTableBuilder, \
                            YelpNeighborhoodsTableBuilder 

from extract_matchers import ByPhoneExtractMatcher, ByAddressExtractMatcher                               
import sys


if __name__ == '__main__':

    n = int(sys.argv[1])

    yelp_restuarants_tb = YelpRestaurantsTableBuilder()
    yelp_categories_tb = YelpCategoriesTableBuilder()
    yelp_neighborhoods_tb = YelpNeighborhoodsTableBuilder()


    api_phone_interfacer = YelpApiPhoneInterfacer(report_interval = 250)
    first_coordinator = YelpApiFirstPassCoordinator(api_interfacer = api_phone_interfacer, start_read = 0)


    # first pass

    extracts = first_coordinator.read_next_n(n = n)
    matched_extracts = ByPhoneExtractMatcher().match_all(extracts)


    yelp_restuarants_tb.build_table(matched_extracts)
    yelp_categories_tb.build_table(matched_extracts)
    yelp_neighborhoods_tb.build_table(matched_extracts)

    # second pass

    api_address_interfacer = YelpApiAddressInterfacer(limit = 10, report_interval = 250)
    second_coordinator = YelpApiSecondPassCoordinator(api_interfacer = api_address_interfacer, start_read = 0)
    
    extracts = second_coordinator.read_next_n(n = n)
    matched_extracts = ByAddressExtractMatcher().match_all(extracts)

    yelp_restuarants_tb.add_records(matched_extracts)
    yelp_categories_tb.add_records(matched_extracts)
    yelp_neighborhoods_tb.add_records(matched_extracts)    

    

    



