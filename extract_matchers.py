from difflib import SequenceMatcher
from constants import LOWER_SIMILARITY_THRESHOLD, UPPER_SIMILARITY_THRESHOLD, \
                    ADDRESS_SCORE_WEIGHT, NAME_SCORE_WEIGHT
from operator import itemgetter

class ExtractMatcher():
    '''Takes (Record, RestaurantExtractList) tuples and matches that Record
    with at most one of the restaurant extracts in the RestaurantExtractList.
    '''

    def match_all(self, extract_tuples):
        return filter(None, (self.match(extract_tuple) for extract_tuple in extract_tuples))

    def match(self, extract_tuple):
        NotImplementedError


class ByPhoneExtractMatcher(ExtractMatcher):

    def match(self, extract_tuple):
        '''In the by-phone search, we just filter out the non-responses, otherwise assume its a match.'''

        doh_extract, yelp_extracts = extract_tuple 
        if not yelp_extracts:
            return None

        else:
            return extract_tuple   


class ByAddressExtractMatcher(ExtractMatcher):

    def __init__(self, similarity_threshold = .8):
        self.similarity_threshold = similarity_threshold

    def match(self, extract_tuple):

        doh_extract, yelp_extracts = extract_tuple

        sorted_extracts = self._score_extracts(doh_extract, yelp_extracts)

        if len(sorted_extracts) > 0:

            best_yelp_extract_match, best_score = sorted_extracts[0]

            if best_score > 0:
                return (doh_extract, best_yelp_extract_match)

        return None    

        
    def _score_extracts(self, doh_extract, yelp_extracts):    

        scored_extracts = [ (yelp_extract, self._score_extract(doh_extract, yelp_extract)) for yelp_extract in yelp_extracts]
        return sorted(scored_extracts, key = itemgetter(1), reverse = True)


    def _score_extract(self, doh_extract, yelp_extract):

        target_address = doh_extract.doh_address
        target_name = doh_extract.doh_dba

        candidate_address = yelp_extract.yelp_address
        candidate_name = yelp_extract.yelp_name

        address_score = self._similarity_score(target_address, candidate_address)
        name_score = self._similarity_score(target_name, candidate_name)
        

        if min(address_score, name_score) >= LOWER_SIMILARITY_THRESHOLD and \
            max(address_score, name_score) >= UPPER_SIMILARITY_THRESHOLD:

            return (ADDRESS_SCORE_WEIGHT*address_score + NAME_SCORE_WEIGHT*name_score)

        else:

            return 0

    def _normalize(self, text):

        return text.lower().strip()


    def _similarity_score(self, a, b):
        
        if a is None or b is None:
            return 0

        a, b = self._normalize(a), self._normalize(b)
        return SequenceMatcher(None, a, b).ratio()





