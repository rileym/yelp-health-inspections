#!/Users/rileymatthews/anaconda/bin/python
# -*- coding: utf-8 -*-

import argparse

from yelp_api_machinery import YelpApiPhoneInterfacer, YelpApiFirstPassCoordinator, \
                                YelpApiAddressInterfacer, YelpApiSecondPassCoordinator

from table_builders import YelpRestaurantsTableBuilder, YelpCategoriesTableBuilder, \
                            YelpNeighborhoodsTableBuilder 

from extract_matchers import ByPhoneExtractMatcher, ByAddressExtractMatcher                               


def build_argparser():

    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--n_pull', help = 'Number of restuarants to pull.', required = True)
    parser.add_argument('-o', '--offset', help = 'Offset from which to start pull.', required = True)
    parser.add_argument('-r', '--report_interval', help = 'Report interval.', required = False)
    parser.add_argument('-c', '--create_table', help = 'Whether to new create a table.', dest = 'create_table', action = 'store_true')
    parser.add_argument('-s', '--search_limit', help = 'Number of results to pull per address search.', required = False)

    parser.set_defaults(feature=False, report_interval = 250, search_limit = 15) 

    return parser  

if __name__ == '__main__':


    parser = build_argparser()
    args = parser.parse_args()

    n_pull = int(args.n_pull)
    offset = int(args.offset)
    report_interval = int(args.report_interval) if args.report_interval else 250
    create_table = args.create_table
    limit = args.search_limit if args.search_limit else 15


    yelp_restuarants_tb = YelpRestaurantsTableBuilder()
    yelp_categories_tb = YelpCategoriesTableBuilder()
    yelp_neighborhoods_tb = YelpNeighborhoodsTableBuilder()


    api_phone_interfacer = YelpApiPhoneInterfacer(report_interval = report_interval)
    first_coordinator = YelpApiFirstPassCoordinator(api_interfacer = api_phone_interfacer, start_read = offset)


    # first pass

    extracts = first_coordinator.read_next_n(n = n_pull)
    matched_extracts = ByPhoneExtractMatcher().match_all(extracts)

    if create_table:

        yelp_restuarants_tb.create_table()
        yelp_categories_tb.create_table()
        yelp_neighborhoods_tb.create_table()

    yelp_restuarants_tb.add_records(matched_extracts)
    yelp_categories_tb.add_records(matched_extracts)
    yelp_neighborhoods_tb.add_records(matched_extracts)            

    # second pass

    api_address_interfacer = YelpApiAddressInterfacer(limit = limit, sort = 1, report_interval = report_interval)
    second_coordinator = YelpApiSecondPassCoordinator(api_interfacer = api_address_interfacer, start_read = offset)
    
    extracts = second_coordinator.read_next_n(n = n_pull)
    matched_extracts = ByAddressExtractMatcher().match_all(extracts)


    yelp_restuarants_tb.add_records(matched_extracts)
    yelp_categories_tb.add_records(matched_extracts)
    yelp_neighborhoods_tb.add_records(matched_extracts)    

    

    



