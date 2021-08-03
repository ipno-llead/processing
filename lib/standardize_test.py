from pandas.testing import assert_series_equal
import pandas as pd
import unittest
import sys
from standardize import standardize_from_lookup_table
sys.path.append('./')


class StandardizeFromLookupTableTestCase(unittest.TestCase):
    def test_standardize(self):
        df = pd.DataFrame([
            ['evidence sus - suspended 10 days'],
            ['sus. - handling'],
            ['disorbey - sustained sus 10'],
        ], columns=['disposition'])
        df2 = standardize_from_lookup_table(df, 'disposition', [
            ['sustained', 'sus', 'sus.'],
            ['handling of evidence', 'evidence', 'handling'],
            ['10 days suspension', 'sus 10', 'suspended 10 days'],
            ['disobey direct order', 'disorbey']
        ], True)
        assert_series_equal(df2.disposition, pd.Series([
            'handling of evidence; sustained; 10 days suspension',
            'sustained; handling of evidence',
            'disobey direct order; sustained; 10 days suspension'
        ]), check_names=False)
