# -*- coding: utf-8 -*-

import unittest
from gdoc_spreadsheet_extraction.utility_code import SheetGrabber


sheet_grabber = SheetGrabber( u'test-identifier' )


class SheetGrabberTest(unittest.TestCase):

    def setUp(self):
        self.spreadsheet = sheet_grabber.get_spreadsheet()

    def test_get_spreadsheet(self):
        self.assertEqual( "<class 'gspread.models.Spreadsheet'>", repr(type(self.spreadsheet)) )

    def test_get_worksheet(self):
        worksheet = sheet_grabber.get_worksheet()
        self.assertEqual( "<class 'gspread.models.Worksheet'>", repr(type(worksheet)) )




if __name__ == '__main__':
    unittest.main()
