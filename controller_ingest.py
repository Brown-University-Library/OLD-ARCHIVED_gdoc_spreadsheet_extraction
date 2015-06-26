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
# from gdoc_spreadsheet_extraction import utility_code
from gdoc_spreadsheet_extraction.utility_code import SheetGrabber, Validator


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


## work

## get spreadsheet object
spreadsheet = sheet_grabber.get_spreadsheet()

## get worksheet
worksheet = sheet_grabber.get_worksheet()

## find ready row
ready_row = sheet_grabber.find_ready_row()
if ready_row == None:
    logger.info( u'%s -- no target row found; ending script' % log_identifier )
    sys.exit()

## prepare data-dct for api
row_data_dict = sheet_grabber.prepare_working_dct()

# validate -- additional_rights
vresult_additional_rights = validator.validateAdditionalRights( row_data_dict['additional_rights'] )

# validate -- by
vresult_by = validator.validateBy( row_data_dict['by'] )

# validate -- create_date
vresult_create_date = validator.validateCreateDate( row_data_dict['create_date'] )

# validate -- description
vresult_description = validator.validateDescription( row_data_dict['description'] )

# validate -- file_path
default_filepath_directory = settings.SPREADSHEET_ACCESS_DICT[spreadsheet_name + '_dict']['default_filepath_directory']
vresult_file_path = utility_code.validateFilePath( row_data_dict['file_path'], default_filepath_directory, identifier )





# # find a row that needs processing
# gdata_target_row_data = utility_code.findRowToProcess( gdata_row_feed, identifier )
# utility_code.updateLog( message=u'C: gdata_target_row_data is: %s' % gdata_target_row_data, identifier=identifier )
# if gdata_target_row_data[ 'status' ] == 'no target row found':
#   utility_code.updateLog( message=u'C: no target row found; ending script', identifier=identifier )
#   sys.exit()

# # convert row fields to a dict-list
# row_data_dict = utility_code.makeRowDataDict( gdata_target_row_data['gdata_target_row'], identifier )
# utility_code.updateLog( message=u'C: row_data_dict is: %s' % row_data_dict, identifier=identifier )

# # validate -- additional_rights
# vresult_additional_rights = utility_code.validateAdditionalRights( row_data_dict['additional_rights'], identifier )
# utility_code.updateLog( message=u'C: vresult_additional_rights is: %s' % vresult_additional_rights, identifier=identifier )

# # validate -- by
# vresult_by = utility_code.validateBy( row_data_dict['by'], identifier )
# utility_code.updateLog( message=u'C: vresult_by is: %s' % vresult_by, identifier=identifier )

# # validate -- create_date
# vresult_create_date = utility_code.validateCreateDate( row_data_dict['create_date'], identifier )
# utility_code.updateLog( message=u'C: vresult_create_date is: %s' % vresult_create_date, identifier=identifier )

# # validate -- description
# vresult_description = utility_code.validateDescription( row_data_dict['description'], identifier )
# utility_code.updateLog( message=u'C: vresult_description is: %s' % vresult_description, identifier=identifier )

# # validate -- file_path
# default_filepath_directory = settings.SPREADSHEET_ACCESS_DICT[spreadsheet_name + '_dict']['default_filepath_directory']
# vresult_file_path = utility_code.validateFilePath( row_data_dict['file_path'], default_filepath_directory, identifier )
# utility_code.updateLog( message=u'C: vresult_file_path is: %s' % vresult_file_path, identifier=identifier )

# validate -- folders
spreadsheet_folder_api_identity = settings.SPREADSHEET_ACCESS_DICT[spreadsheet_name + '_dict']['permitted_folder_api_add_items_identity']
vresult_folders = utility_code.validateFolders( row_data_dict['folders'], spreadsheet_folder_api_identity, identifier )
utility_code.updateLog( message=u'C: vresult_folders is: %s' % vresult_folders, identifier=identifier )

# validate -- keywords
vresult_keywords = utility_code.validateKeywords( row_data_dict['keywords'], identifier )
utility_code.updateLog( message=u'C: vresult_keywords is: %s' % vresult_keywords, identifier=identifier )

# validate -- title
vresult_title = utility_code.validateTitle( row_data_dict['title'], identifier )
utility_code.updateLog( message=u'C: vresult_title is: %s' % vresult_title, identifier=identifier )

# check overall validity
validity_result_list = [
  vresult_additional_rights, vresult_by,
  vresult_create_date, vresult_description,
  vresult_file_path, vresult_folders,
  vresult_keywords, vresult_title
  ]
overall_validity_data = utility_code.runOverallValidity( validity_result_list, identifier )
utility_code.updateLog( message=u'C: overall_validity_data is: %s' % overall_validity_data, identifier=identifier )


1/0


# update spreadsheet if necessary
if overall_validity_data['status'] == 'FAILURE':
  # prepare replacement-dict
  row_replacement_data = utility_code.prepareRowReplacementDictOnError(
    gdata_row_object=gdata_target_row_data['gdata_target_row'],
    error_message=overall_validity_data['message'],
    identifier=identifier )
  utility_code.updateLog( message=u'C: row_replacement_data is: %s' % row_replacement_data, identifier=identifier )
  # update spreadsheet
  spreadsheet_update_data = utility_code.updateSpreadsheet(
    gdata_client=gdata_client,
    gdata_row_object=gdata_target_row_data['gdata_target_row'],
    replacement_dict=row_replacement_data['replacement_dict'],
    identifier=identifier )
  utility_code.updateLog( message=u'C: spreadsheet_update_data is: %s' % spreadsheet_update_data, identifier=identifier )
  # quit
  utility_code.updateLog( message=u'C: ending script after spreadsheet error update', identifier=identifier )
  sys.exit()

# gogogo!
utility_code.updateLog( message=u'C: ready to ingest!', identifier=identifier )
ingestion_result_data = utility_code.ingestItem( validity_result_list, identifier )
utility_code.updateLog( message=u'C: ingestion_result_data is: %s' % ingestion_result_data, identifier=identifier )

# update row after ingestion
if ingestion_result_data['status'] == 'success':
  row_replacement_data = utility_code.prepareRowReplacementDictOnSuccess(
    gdata_row_object=gdata_target_row_data['gdata_target_row'],
    pid=ingestion_result_data['post_json_dict']['pid'],
    identifier=identifier )
else:
  row_replacement_data = utility_code.prepareRowReplacementDictOnError(
    gdata_row_object=gdata_target_row_data['gdata_target_row'],
    error_message=u'data valid, but problem ingesting item; error logged',
    identifier=identifier )
utility_code.updateLog( message=u'C: row_replacement_data is: %s' % row_replacement_data, identifier=identifier )
# final update spreadsheet
final_spreadsheet_update_data = utility_code.updateSpreadsheet(
  gdata_client=gdata_client,
  gdata_row_object=gdata_target_row_data['gdata_target_row'],
  replacement_dict=row_replacement_data['replacement_dict'],
  identifier=identifier )
utility_code.updateLog( message=u'C: final_spreadsheet_update_data is: %s' % final_spreadsheet_update_data, identifier=identifier )
# quit
utility_code.updateLog( message=u'C: ending script', identifier=identifier )
sys.exit()

# [END]
