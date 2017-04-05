# -*- coding: utf-8 -*-

"""
- Purpose: script manages the detection of items in a google doc spreadsheet
    which are ready to be uploaded to the bdr, and, for each one,
    calls the item-api to ingest the item to the bdr.
- Assumes:
    - virtual environment set up
    - site-packages `requirements.pth` file adds `gdoc_spreadsheet_extraction` enclosing-directory to sys path.
- TODO:
    1) currently script finds single record ready to be ingested;
       switch to detecting bunch and looping through each.
    2) incorporate logic to look for items ready for 'updating' rather than
       items newly-created.
"""

import datetime, logging, os, random, sys
import utility_code
from utility_code import SheetGrabber, Validator, SheetUpdater


## settings
LOG_PATH = os.environ['ASSMNT__LOG_PATH']
LOG_LEVEL = os.environ['ASSMNT__LOG_LEVEL']  # 'DEBUG' or 'INFO'


## log config
log_level = { 'DEBUG': logging.DEBUG, 'INFO': logging.INFO }
logging.basicConfig(
    filename=LOG_PATH, level=log_level[LOG_LEVEL],
    format=u'[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s', datefmt=u'%d/%b/%Y %H:%M:%S' )
logger = logging.getLogger(__name__)
log_identifier = random.randint( 1111, 9999 )  # helps to track log flow
logger.info( u'%s -- log_identifier set' % log_identifier )


## instances
sheet_grabber = SheetGrabber( log_identifier )
validator = Validator( log_identifier )
sheet_updater = SheetUpdater( log_identifier )


## work

## get spreadsheet object
spreadsheet = sheet_grabber.get_spreadsheet()

## get worksheet
sheet_grabber.get_worksheet()

## find ready row
ready_row = sheet_grabber.find_ready_row()
if ready_row == None:
    logger.info( u'%s -- no target row found; ending script' % log_identifier )
    sys.exit()

## prepare data-dct for api
row_data_dict = sheet_grabber.prepare_working_dct()

## validate

# validate -- additional_rights
vresult_additional_rights = validator.validateAdditionalRights( row_data_dict['additional_rights'] )

# validate -- by
vresult_by = validator.validateBy( row_data_dict['by'] )

# validate -- create_date
vresult_create_date = validator.validateCreateDate( row_data_dict['create_date'] )

# validate -- description
vresult_description = validator.validateDescription( row_data_dict['description'] )

# validate -- file_path
vresult_file_path = validator.validateFilePath( row_data_dict['file_path'] )

# validate -- folders
vresult_folders = validator.validateFolders( row_data_dict['folders'] )

# validate -- keywords
vresult_keywords = validator.validateKeywords( row_data_dict['keywords'] )

# validate -- title
vresult_title = validator.validateTitle( row_data_dict['title'] )

# check overall validity
validity_result_list = [
  vresult_additional_rights, vresult_by,
  vresult_create_date, vresult_description,
  vresult_file_path, vresult_folders,
  vresult_keywords, vresult_title
  ]
overall_validity_data = validator.runOverallValidity( validity_result_list )
logger.info( u'%s -- validity_result_list, `%s`' % (log_identifier, validity_result_list) )
logger.info( u'%s -- overall_validity_data, `%s`' % (log_identifier, overall_validity_data) )

# update spreadsheet if necessary
if overall_validity_data['status'] == 'FAILURE':
    logger.info( u'%s -- failure update starting' % log_identifier )
    sheet_updater.update_on_error(
        worksheet=sheet_grabber.worksheet,  # for actually updating the cell
        original_data_dct=sheet_grabber.original_ready_row_dct,
        row_num=sheet_grabber.original_ready_row_num,
        error_data=overall_validity_data )
    logger.info( u'%s -- failure update end -- shouldn\'t get here because called class exits script' % log_identifier )

# gogogo!
logger.info( u'%s -- ready to ingest' % log_identifier )
ingestion_result_data = utility_code.ingestItem( validity_result_list, log_identifier )
logger.info( u'%s -- ingestion_result_data, `%s`' % (log_identifier, ingestion_result_data) )

# update row after ingestion
if ingestion_result_data['status'] == 'success':
    logger.info( u'%s -- updating spreadsheet on success' % log_identifier )
    pid = ingestion_result_data['post_json_dict']['pid']
    logger.debug( u'%s -- pid, `%s`' % (log_identifier, pid) )
    sheet_updater.update_on_success(
        worksheet=sheet_grabber.worksheet,  # for actually updating the cell
        original_data_dct=sheet_grabber.original_ready_row_dct,
        row_num=sheet_grabber.original_ready_row_num,
        pid=pid
        )
else:
    logger.info( u'%s -- updating spreadsheet on ingestion error' % log_identifier )
    sheet_updater.update_on_error(
        worksheet=sheet_grabber.worksheet,  # for actually updating the cell
        original_data_dct=sheet_grabber.original_ready_row_dct,
        row_num=sheet_grabber.original_ready_row_num,
        error_data={ u'message': u'data valid, but problem ingesting item; error logged' }
        )

# quit
logger.info( u'%s -- spreadsheet updated, ending script -- shouldn\'t get here because called class exists script' % log_identifier )
sys.exit()

# [END]
