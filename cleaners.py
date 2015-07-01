# -*- coding: utf-8 -*-

import re

class inspection_entry():

    
    def __init__(self, entry_dict, null_val = 'NULL'):
        
        self.NULL = null_val
        self.NO_FILTER_FIELDS = ['CAMIS',
                                 'ZIPCODE',
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
        self.values = self._process(entry_dict)
    
    def _encode_clean(self, raw_str):
        
        return raw_str.replace('Â', '') if raw_str.strip() else self.NULL

    def _phone_process(self, phone):
        
        r = re.compile('^[\d +-]+$')
        r_replace = re.compile('[ +-]')
        return r_replace.sub('', phone) if r.match(phone) else self.NULL
        
    def _grade_process(self, grade):
        
        r = re.compile('^[A-Z]$')
        return grade.strip() if r.match(grade.strip()) else self.NULL
    
    def _action_process(self, action):
        
        if not action:
            return self.NULL
        else:
            return self.ACTION_MAP.get(action,action)
    
    def _crit_process(self, crit_val):
        
        if not crit_val:
            return self.NULL
        elif crit_val == 'Critical':
            return 1
        else:
            return 0
        
    def _addr_process(self, building, street):
        
        r = re.compile('\s+')
        addr = ' '.join((building, street))
        return r.sub(' ', addr).strip()
        
    def _i_date_process(self, i_date):
    
        return i_date if i_date != '01/01/1900' else self.NULL
    
    def _process(self, entry_dict):
             
        na_fill = lambda v: v if v else self.NULL
        
        values = {}
        for field in self.NO_FILTER_FIELDS:
            values[field.replace(' ', '_')] = na_fill(entry_dict.get(field))
        
        values['DBA'] = self._encode_clean(entry_dict.get('DBA'))
        values['ADDRESS'] = self._addr_process(entry_dict.get('BUILDING'), entry_dict.get('STREET'))
        values['CRITIAL_FLAG'] = self._crit_process(entry_dict.get('CRITICAL FLAG'))
        values['INSPECTION_DATE'] = self._i_date_process(entry_dict.get('INSPECTION DATE'))
        values['ACTION'] = self._action_process(entry_dict.get('ACTION'))
        values['GRADE'] = self._grade_process(entry_dict.get('GRADE'))
        values['PHONE'] = self._phone_process(entry_dict.get('PHONE'))

        return values
    
    
    
    
    
    
    