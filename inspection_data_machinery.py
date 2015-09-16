# -*- coding: utf-8 -*-

import csv
from collections import namedtuple
import psycopg2
from psycopg2.extras import NamedTupleConnection
import requests
import re


DOH_FIELDS = 'doh_camis, doh_dba, doh_address, doh_zipcode, doh_phone, doh_inspection_type, doh_inspection_date, doh_action, doh_score, doh_grade, doh_grade_date, doh_violation_code, doh_critical_flag, doh_record_date'
DohInspectionExtract = namedtuple('DohInspectionExtract', DOH_FIELDS)

class InspectionDataRetriever:


    def retrieve(self, data_url):

        r = requests.get(data_url)
        self.data =  r.content

    def save(self, data_local_path):

        with open(data_local_path, 'w') as f:
            f.write(self.data)


class InspectionRecordCleaner:

    
    def __init__(self):
        
        self.NULL = None
        self.NO_FILTER_FIELDS = ['CAMIS',
                                 'VIOLATION CODE',
                                 'SCORE',
                                 'GRADE DATE',
                                 'RECORD DATE',
                                 'INSPECTION TYPE']
        self.ACTION_MAP = {
                      'No violations were recorded at the time of this inspection.':'No violations cited.',
                     'Violations were cited in the following area(s).':'Violations cited',
                     'Establishment Closed by DOHMH.  Violations were cited in the following area(s) and those requiring immediate action were addressed.':'Establishment Closed by DOHMH'
                     }


    def clean(self, record_dict):
             
        na_fill = lambda v: v if v else self.NULL
        
        cleaned_record_dict = {}
        for field in self.NO_FILTER_FIELDS:
            cleaned_record_dict['doh_' + field.replace(' ', '_').lower()] = na_fill(record_dict.get(field))
        
        cleaned_record_dict['doh_dba'] = self._dba_clean(record_dict.get('DBA'))
        cleaned_record_dict['doh_address'] = self._addr_clean(record_dict.get('BUILDING'), record_dict.get('STREET'))
        cleaned_record_dict['doh_zipcode'] = self._zipcode_clean(record_dict.get('ZIPCODE'))
        cleaned_record_dict['doh_critical_flag'] = self._critical_flag_clean(record_dict.get('CRITICAL FLAG'))
        cleaned_record_dict['doh_inspection_date'] = self._inspection_date_clean(record_dict.get('INSPECTION DATE'))
        cleaned_record_dict['doh_action'] = self._action_clean(record_dict.get('ACTION'))
        cleaned_record_dict['doh_grade'] = self._grade_clean(record_dict.get('GRADE'))
        cleaned_record_dict['doh_phone'] = self._phone_clean(record_dict.get('PHONE'))

        cleaned_record = map(lambda field: cleaned_record_dict[field], DohInspectionExtract._fields)
        return DohInspectionExtract._make(cleaned_record)


    def _dba_clean(self, raw_str):
        
        return raw_str.replace('Â', '') if raw_str.strip() else self.NULL

    def _zipcode_clean(self, zipcode):

        r = re.compile('^\d{5}$')
        zipcode = zipcode.strip()
        return zipcode if r.match(zipcode) else self.NULL

    def _phone_clean(self, phone):
        
        r = re.compile('^[\d +-]+$')
        r_replace = re.compile('[ +-]')
        return r_replace.sub('', phone) if r.match(phone) else self.NULL

        
    def _grade_clean(self, grade):
        
        r = re.compile('^[A-Z]$')
        return grade.strip() if r.match(grade.strip()) else self.NULL

    
    def _action_clean(self, action):
        
        if not action:
            return self.NULL
        else:
            return self.ACTION_MAP.get(action,action)

    
    def _critical_flag_clean(self, crit_val):
        
        if not crit_val:
            return self.NULL
        elif crit_val == 'Critical':
            return 1
        else:
            return 0

        
    def _addr_clean(self, building, street):

        
        r = re.compile('\s+')
        addr = ' '.join((building, street))
        return r.sub(' ', addr).strip()

        
    def _inspection_date_clean(self, inspection_date):
    
        return inspection_date if inspection_date != '01/01/1900' else self.NULL



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




