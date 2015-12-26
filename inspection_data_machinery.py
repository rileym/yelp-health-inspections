# -*- coding: utf-8 -*-

import csv
from collections import namedtuple
import psycopg2
from psycopg2.extras import NamedTupleConnection
import requests
import re


# MOVE TO constants

NULL = None

DOH_CAMIS_NAME = 'doh_camis'
DOH_DBA_NAME = 'doh_dba'
DOH_ADDRESS_NAME = 'doh_address'
DOH_ZIPCODE_NAME = 'doh_zipcode'
DOH_PHONE_NAME = 'doh_phone'
DOH_INSPECTION_TYPE_NAME = 'doh_inspection_type'
DOH_INSPECTION_DATE_NAME = 'doh_inspection_date'
DOH_ACTION_NAME = 'doh_action'
DOH_SCORE_NAME = 'doh_score'
DOH_GRADE_NAME = 'doh_grade'
DOH_GRADE_DATE_NAME = 'doh_grade_date'
DOH_VIOLATION_CODE_NAME = 'doh_violation_code'
DOH_CRITICAL_FLAG_NAME = 'doh_critical_flag'
DOH_RECORD_DATE_NAME = 'doh_record_date'


DOH_FIELDS = [  DOH_CAMIS_NAME,
                DOH_DBA_NAME,
                DOH_ADDRESS_NAME,
                DOH_ZIPCODE_NAME,
                DOH_PHONE_NAME, 
                DOH_INSPECTION_TYPE_NAME,
                DOH_INSPECTION_DATE_NAME,
                DOH_ACTION_NAME,
                DOH_SCORE_NAME,
                DOH_GRADE_NAME,
                DOH_GRADE_DATE_NAME,
                DOH_VIOLATION_CODE_NAME,
                DOH_CRITICAL_FLAG_NAME,
                DOH_RECORD_DATE_NAME,
             ]

DohInspectionExtract = namedtuple('DohInspectionExtract', DOH_FIELDS)

class InspectionDataRetriever:


    def retrieve(self, data_url):

        r = requests.get(data_url)
        self.data =  r.content

    def save(self, data_local_path):

        with open(data_local_path, 'w') as f:
            f.write(self.data)



class FieldCleaners(object):


    FN_TABLE = {                
                DOH_CAMIS_NAME:            FieldCleaners.standard_clean_factory('CAMIS') ,
                DOH_DBA_NAME:              FieldCleaners.dba_clean ,
                DOH_ADDRESS_NAME:          FieldCleaners.addr_clean ,
                DOH_ZIPCODE_NAME:          FieldCleaners.zipcode_clean ,
                DOH_PHONE_NAME,:           FieldCleaners.phone_clean , 
                DOH_INSPECTION_TYPE_NAME:  FieldCleaners.standard_clean_factory('INSPECTION TYPE') ,
                DOH_INSPECTION_DATE_NAME:  FieldCleaners.inspection_date_clean ,
                DOH_ACTION_NAME:           FieldCleaners.action_clean ,
                DOH_SCORE_NAME:            FieldCleaners.standard_clean_factory('SCORE') ,
                DOH_GRADE_NAME:            FieldCleaners.grade_clean ,
                DOH_GRADE_DATE_NAME:       FieldCleaners.standard_clean_factory('GRADE DATE') ,
                DOH_VIOLATION_CODE_NAME:   FieldCleaners.standard_clean_factory('VIOLATION CODE') ,
                DOH_CRITICAL_FLAG_NAME:    FieldCleaners.critical_flag_clean ,
                DOH_RECORD_DATE_NAME:      FieldCleaners.standard_clean_factory('RECORD DATE') ,  
                }

    ACTION_MAP = {
                    'No violations were recorded at the time of this inspection.':'No violations cited.',
                    'Violations were cited in the following area(s).':'Violations cited',
                    'Establishment Closed by DOHMH.  Violations were cited in the following area(s) and those requiring immediate action were addressed.':'Establishment Closed by DOHMH'
                }


    @staticmethod
    standard_clean_factory(field_name):

        def standard_clean(record):
            val = record.get(field_namE)
            return val if val else NULL

        return standard_clean


    @staticmethod
    def clean(out_key, record):

        cleaner_fn = FieldCleaners.FN_TABLE[out_key]
        return cleaner_fn(record)


    @staticmethod
    def dba_clean(record):
        
        raw_str = record.get('DBA')
        return raw_str.replace('Â', '') if raw_str.strip() else NULL

    @staticmethod
    def zipcode_clean(record):

        zipcode = record.get('ZIPCODE')
        r = re.compile('^\d{5}$')
        zipcode = zipcode.strip()
        return zipcode if r.match(zipcode) else NULL

    @staticmethod
    def phone_clean(record):

        phone = record.get('PHONE')
        r = re.compile('^[\d +-]+$')
        r_replace = re.compile('[ +-]')
        return r_replace.sub('', phone) if r.match(phone) else NULL

    @staticmethod    
    def grade_clean(record):

        grade = record.get('GRADE')
        r = re.compile('^[A-Z]$')
        return grade.strip() if r.match(grade.strip()) else NULL

    @staticmethod
    def action_clean(record):

        action = record.get('ACTION')
        if not action:
            return NULL
        else:
            return FieldCleaners.ACTION_MAP.get(action, action)

    @staticmethod
    def critical_flag_clean(record):
        
        crit_val = record.get('CRITICAL FLAG')
        if not crit_val:
            return NULL
        elif crit_val == 'Critical':
            return 1
        else:
            return 0

    @staticmethod
    def addr_clean(record):

        building = record.get('BUILDING')
        street = record.get('STREET')
        r = re.compile('\s+')
        addr = ' '.join((building, street))
        return r.sub(' ', addr).strip()

    @staticmethod
    def inspection_date_clean(record):
        
        inspection_date = record.get('INSPECTION DATE')
        return inspection_date if inspection_date != '01/01/1900' else NULL        




class InspectionRecordCleaner(object):

    @staticmethod
    def clean(record):

        cleaned = { doh_field : FieldCleaners.clean(doh_field, record)} for doh_field in DOH_FIELDS }

        return DohInspectionExtract._make(cleaned)




class InspectionDataCleaner:

    def __init__(self):

        self.record_cleaner = InspectionRecordCleaner()

    def clean(self, read_path):

        with open(read_path, 'rt') as f:
            
            dialect = csv.Sniffer().sniff(f.read(1024))
            f.seek(0)
            reader = csv.reader(f, dialect)
            header = reader.next()
            
            records = []
            for row in reader:
                record_dict =  dict(zip(header, row))
                cleaned_record = self.record_cleaner.clean(record_dict)
                records.append(cleaned_record)

        return records                




