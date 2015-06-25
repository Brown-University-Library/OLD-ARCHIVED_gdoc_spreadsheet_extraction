'''
- Purpose: script manages the detection of items in a google doc spreadsheet 
           which are ready to be uploaded to the bdr, and, for each one, 
           calls the item-api to ingest the item to the bdr.
- TODO: 1) currently script finds single record ready to be ingested;
           switch to detecting bunch and looping through each.
        2) incorporate logic to look for items ready for 'updating' rather than
           items newly-created.
'''



## setup

# activate env
from spreadsheet_extraction_local_settings import settings_local as env_settings
activate_this = '%s/bin/activate_this.py' % env_settings.ENV_PATH
execfile( activate_this, dict(__file__=activate_this) )

# add enclosing directory to path
import os, sys
current_script_name = sys.argv[0] # may or may not include path
directory_path = os.path.dirname( current_script_name )
full_directory_path = os.path.abspath( directory_path )
directory_list = full_directory_path.split('/')
last_element_string = directory_list[-1]
enclosing_directory = full_directory_path.replace( '/' + last_element_string, '' ) # strip off the slash plus the current directory
sys.path.append( enclosing_directory )

# ok, normal imports
from gdoc_spreadsheet_extraction import settings
from gdoc_spreadsheet_extraction import utility_code
# from gdoc_spreadsheet_extraction.libs import atom  # part of gdata download  # 2011-01-24: don't think i need this
# from xml.etree import ElementTree  # 2011-01-24: needed?
import datetime

# gdata imports
sys.path.append( '%s/gdoc_spreadsheet_extraction/libs' % enclosing_directory )  # needed for next line
from gdoc_spreadsheet_extraction.libs import gdata  # gdata.__init__.py runs an import on atom
import gdata.spreadsheet.service
import gdata.service
import atom.service
import gdata.spreadsheet



## work

# make log identifier
identifier = utility_code.makeIdentifier()
utility_code.updateLog( message=u'C: identifier set', identifier=identifier )

# get the spreadsheet name
utility_code.updateLog( message=u'C: sys.argv is: %s' % sys.argv, identifier=identifier )
if len( sys.argv ) < 2:
  sys.exit('Usage: ./controller.py spreadsheet_name')
else:
  spreadsheet_name = sys.argv[1]
  utility_code.updateLog( message=u'C: spreadsheet_name is: %s' % spreadsheet_name, identifier=identifier )

# get a gdata_client
gdata_client_result = utility_code.getGdataClient( spreadsheet_name, identifier )
utility_code.updateLog( message=u'C: gdata_client is: %s' % gdata_client_result, identifier=identifier )

# access spreadsheet
gdata_client = gdata_client_result['gdata_client_object']
spreadsheet_data = utility_code.getSpreadsheetData( gdata_client, spreadsheet_name, identifier )
utility_code.updateLog( message=u'C: spreadsheet_data is: %s' % spreadsheet_data, identifier=identifier )

# get spreadsheet rows
gdata_row_feed = gdata_client.GetListFeed( spreadsheet_data['spreadsheet_key'] )
utility_code.updateLog( message=u'C: gdata_row_feed is: %s' % repr(gdata_row_feed).decode(u'utf-8', u'replace'), identifier=identifier )

# find a row that needs processing
gdata_target_row_data = utility_code.findRowToProcess( gdata_row_feed, identifier )
utility_code.updateLog( message=u'C: gdata_target_row_data is: %s' % gdata_target_row_data, identifier=identifier )
if gdata_target_row_data[ 'status' ] == 'no target row found':
  utility_code.updateLog( message=u'C: no target row found; ending script', identifier=identifier )
  sys.exit()
  
# convert row fields to a dict-list
row_data_dict = utility_code.makeRowDataDict( gdata_target_row_data['gdata_target_row'], identifier )
utility_code.updateLog( message=u'C: row_data_dict is: %s' % row_data_dict, identifier=identifier )

# validate -- additional_rights
vresult_additional_rights = utility_code.validateAdditionalRights( row_data_dict['additional_rights'], identifier )
utility_code.updateLog( message=u'C: vresult_additional_rights is: %s' % vresult_additional_rights, identifier=identifier )

# validate -- by
vresult_by = utility_code.validateBy( row_data_dict['by'], identifier )
utility_code.updateLog( message=u'C: vresult_by is: %s' % vresult_by, identifier=identifier )

# validate -- create_date
vresult_create_date = utility_code.validateCreateDate( row_data_dict['create_date'], identifier )
utility_code.updateLog( message=u'C: vresult_create_date is: %s' % vresult_create_date, identifier=identifier )

# validate -- description
vresult_description = utility_code.validateDescription( row_data_dict['description'], identifier )
utility_code.updateLog( message=u'C: vresult_description is: %s' % vresult_description, identifier=identifier )

# validate -- file_path
default_filepath_directory = settings.SPREADSHEET_ACCESS_DICT[spreadsheet_name + '_dict']['default_filepath_directory']
vresult_file_path = utility_code.validateFilePath( row_data_dict['file_path'], default_filepath_directory, identifier )
utility_code.updateLog( message=u'C: vresult_file_path is: %s' % vresult_file_path, identifier=identifier )

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