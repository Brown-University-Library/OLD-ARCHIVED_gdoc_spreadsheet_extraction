# -*- coding: utf-8 -*-

import pprint, unittest
from gdoc_spreadsheet_extraction.utility_code import SheetGrabber


sheet_grabber = SheetGrabber( u'test-identifier' )


class SheetGrabberTest(unittest.TestCase):

    def setUp(self):
        self.spreadsheet = sheet_grabber.get_spreadsheet()

    def test_get_spreadsheet(self):
        self.assertEqual( "<class 'gspread.models.Spreadsheet'>", repr(type(self.spreadsheet)) )

    def test_get_worksheet(self):
        sheet_grabber.get_worksheet()
        self.assertEqual( "<class 'gspread.models.Worksheet'>", repr(type(sheet_grabber.worksheet)) )

    def test_find_ready_row(self):
        sheet_grabber.get_worksheet()
        sheet_grabber.find_ready_row()
        self.assertEqual( 2, sheet_grabber.original_ready_row_idx )
        # self.assertEqual( '2', sheet_grabber.original_ready_row_dct )




if __name__ == '__main__':
    unittest.main()
