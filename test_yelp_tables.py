# -*- coding: utf-8 -*-

from constants import RAW_INSPECTION_CSV_URL, RAW_INSPECTION_CSV_PATH
from inspection_data_machinery import InspectionDataRetriever, InspectionRecordCleaner, InspectionDataCleaner
from table_builders import DohRestaurantsTableBuilder, DohInspectionsTableBuilder


if __name__ == '__main__':

    retriever = InspectionDataRetriever()
    cleaner = InspectionDataCleaner()

    retriever.retrieve(RAW_INSPECTION_CSV_URL)
    retriever.save(RAW_INSPECTION_CSV_PATH)

    extracts = cleaner.clean(RAW_INSPECTION_CSV_PATH)

    doh_restaurants_tb = DohRestaurantsTableBuilder()
    doh_inspections_tb = DohInspectionsTableBuilder()

    doh_restaurants_tb.build_table(extracts)
    doh_inspections_tb.build_table(extracts)


