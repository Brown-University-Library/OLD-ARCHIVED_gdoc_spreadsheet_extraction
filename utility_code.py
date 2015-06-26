# -*- coding: utf-8 -*-

import datetime, json, logging, os, pprint, sys
import gspread
import requests
from oauth2client.client import SignedJwtAssertionCredentials


log = logging.getLogger(__name__)


class Validator( object ):
    """ Manages validation. """

    def __init__( self, log_identifier ):
        self.log_identifier = log_identifier
        self.DEFAULT_FILEPATH_DIRECTORY = os.environ['ASSMNT__DEFAULT_FILEPATH_DIRECTORY']  # should contain trailing slash
        self.PERMITTED_FOLDER_API_ADD_ITEMS_IDENTITY = os.environ['ASSMNT__PERMITTED_FOLDER_API_ADD_ITEMS_IDENTITY']
        self.FOLDER_API_URL = os.environ['ASSMNT__FOLDER_API_URL']

    def validateAdditionalRights( self, cell_data ):
        try:
            # make identity list
            identity_list = []
            delete_identities = cell_data['delete'].split( ' | ' )
            update_identities = cell_data['update'].split( ' | ' )
            view_identities = cell_data['view'].split( ' | ' )
            merged_identities = delete_identities[:]
            merged_identities.extend( update_identities )
            merged_identities.extend( view_identities )
            merged_identities.sort()
            for identity in merged_identities:
              if not identity in identity_list:
                identity_list.append( identity )
            identity_list.sort( key=str.lower )  # sort helps with testing and logging
            log.debug( u'%s -- identity_list, `%s`' % (self.log_identifier, identity_list) )

            # make string
            return_string = ''
            for identity in identity_list:
              segment = '%s#' % identity
              if identity in view_identities:
                segment = segment + 'discover,display'
              if identity in update_identities:
                segment = segment + ',modify'
              if identity in delete_identities:
                segment = segment + ',delete'
              log.debug( u'%s -- segment before comma-check, `%s`' % (self.log_identifier, segment) )
              # remove possible preceding comma
              segment = segment.replace( '#,', '#' )
              log.debug( u'%s -- segment, `%s`' % (self.log_identifier, segment) )
              return_string = '%s+%s' % ( return_string, segment )

            # return
            return_string = return_string[1:]  # remove initial '+'
            return_dict = { 'status': 'valid', 'normalized_cell_data': return_string, 'parameter_label': 'additional_rights' }
            log.info( u'%s -- return_dict, `%s`' % (self.log_identifier, return_dict) )
            return return_dict

        except Exception, e:
            log.error( u'%s -- exception, `%s`' % (self.log_identifier, unicode(repr(e))) )
            return { 'status': 'FAILURE', 'message': 'problem with "additional-rights" entry' }

        # end validateAdditionalRights()

    def validateBy( self, cell_data ):
          '''
          - Purpose: a) validate 'by' data; b) create a postable string for the item-api.
          - Called by: controller.py
          - TODO: make more robust by stripping unnecessary whitespace.
          '''
          try:
            # if filled out, ensure both name and role
            if len( cell_data ) > 0:
              by_list = cell_data.split( '#' )
              if not ( len(by_list[0]) > 0 and len(by_list[1]) > 0 ):
                return_dict = { 'status': 'FAILURE', 'message': 'problem with "by" entry' }
              else:
                return_dict = { 'status': 'valid', 'normalized_cell_data': cell_data, 'parameter_label': 'by' }
            else:
              return_dict = { 'status': 'valid-empty', 'normalized_cell_data': '', 'parameter_label': 'by' }
            log.info( u'%s -- return_dict, `%s`' % (self.log_identifier, return_dict) )
            return return_dict
          except Exception, e:
            log.error( u'%s -- exception, `%s`' % (self.log_identifier, unicode(repr(e))) )
            return { 'status': 'FAILURE', 'message': 'problem with "by" entry' }

          # end validateBy()

    def validateCreateDate( self, cell_data ):
          '''
          - Purpose: a) validate 'create_date' data;
                     b) create a postable string for the item-api by converting 2/15/2007 to 2007-02-15
          - Called by: controller.py
          '''
          try:
            # optional field
            if len( cell_data ) > 0:
              date_parts = cell_data.split( '/' )
              log.debug( u'%s -- date_parts, `%s`' % (self.log_identifier, date_parts) )
              datetime_object = datetime.datetime( year=int(date_parts[2]), month=int(date_parts[0]), day=int(date_parts[1]) )
              new_date_string = datetime_object.strftime('%Y-%m-%d')
              return_dict = { 'status': 'valid', 'normalized_cell_data': new_date_string, 'parameter_label': 'create_date' }
            else:
              return_dict = { 'status': 'valid-empty', 'normalized_cell_data': '', 'parameter_label': 'create_date' }
            log.info( u'%s -- return_dict, `%s`' % (self.log_identifier, return_dict) )
            return return_dict
          except Exception, e:
            log.error( u'%s -- exception, `%s`' % (self.log_identifier, unicode(repr(e))) )
            return { 'status': 'FAILURE', 'message': 'problem with "create_date" entry' }

          # end validateCreateDate()

    def validateDescription( self, cell_data ):
          '''
          - Purpose: a) validate 'description' data; b) create a postable string for the item-api
          - Called by: controller.py
          - TODO: add test for unicode issues
          '''
          try:
            # optional field
            if len( cell_data ) > 0:
              return_dict = { 'status': 'valid', 'normalized_cell_data': cell_data, 'parameter_label': 'description' }
            else:
              return_dict = { 'status': 'valid-empty', 'normalized_cell_data': '', 'parameter_label': 'description' }
            log.info( u'%s -- return_dict, `%s`' % (self.log_identifier, return_dict) )
            return return_dict
          except Exception, e:
            log.error( u'%s -- exception, `%s`' % (self.log_identifier, unicode(repr(e))) )
            return_dict =  { 'status': 'FAILURE', 'message': 'problem with "description" entry' }
            return return_dict

        # end validateDescription()

    def validateFilePath( self, cell_data ):
          '''
          - Purpose: a) validate 'file_path' data; b) create a postable string for the item-api
          - Called by: controller.py
          - TODO: add test for file-does-not-exist
          '''
          try:
            log.debug( u'%s -- cell_data, `%s`' % (self.log_identifier, cell_data) )
            log.debug( u'%s -- default_filepath_directory, `%s`' % (self.log_identifier, self.DEFAULT_FILEPATH_DIRECTORY) )
            # make path
            if '/' in cell_data:
              file_path = cell_data
            else:
              file_path = '%s%s' % ( self.DEFAULT_FILEPATH_DIRECTORY, cell_data )  # default_filepath_directory contains trailing slash
            log.debug( u'%s -- file_path, `%s`' % (self.log_identifier, file_path) )
            # see if file exists
            return_dict = 'init'
            if not os.path.exists( file_path ):
              return_dict = { 'status': 'FAILURE', 'message': 'file not found' }
            elif not os.path.isfile( file_path ):
              return_dict = { 'status': 'FAILURE', 'message': 'path valid but not a file' }
            else:
              return_dict = { 'status': 'valid', 'normalized_cell_data': file_path, 'parameter_label': 'file_path' }
            # return
            log.info( u'%s -- return_dict, `%s`' % (self.log_identifier, return_dict) )
            return return_dict
          except Exception, e:
            log.error( u'%s -- exception, `%s`' % (self.log_identifier, unicode(repr(e))) )
            return_dict =  { 'status': 'FAILURE', 'message': 'problem with "file_path" entry' }
            return return_dict

      # end validateFilePath()

    def validateFolders( self, cell_data ):
          '''
          - Purpose: a) validate 'folders' data; b) create a postable string for the item-api
          - Called by: controller.py
          - TODO: add test for multiple folders / make more robust by stripping unnecessary white-space
          '''
          try:
            log.debug( u'%s -- cell_data, `%s`' % (self.log_identifier, cell_data) )
            log.debug( u'%s -- spreadsheet_folder_api_identity, `%s`' % (self.log_identifier, self.PERMITTED_FOLDER_API_ADD_ITEMS_IDENTITY) )

            return_dict = 'init'
            # see if the there's folder info
            cell_data = cell_data.strip()
            if len( cell_data ) == 0:
              return_dict =  { 'status': 'FAILURE', 'message': 'no folder specified' }
            # get a list of folders (might only be one)
            folder_list = cell_data.split( ' | ' )
            cleaned_folder_list = []
            for entry in folder_list:
              cleaned_entry = entry.strip()
              cleaned_folder_list.append( cleaned_entry )

            # process folders
            log.debug( u'%s -- cleaned_folder_list, `%s`' % (self.log_identifier, cleaned_folder_list) )
            normalized_cell_string = ''
            for cleaned_entry in cleaned_folder_list:

              # see if it's formatted properly
              if return_dict == 'init' and not cleaned_entry.count('[') == 1:
                return_dict =  { 'status': 'FAILURE', 'message': 'folder data formatted incorrectly' }
                break
              # see if it exists
              if return_dict == 'init':
                folder_parts = cleaned_entry.split( '[' )
                folder_name = folder_parts[0]
                folder_id = folder_parts[1][0:-1]
                # folder_api_full_url = '%s%s/?identities=%s' % (self.FOLDER_API_URL, folder_id, json.dumps([self.PERMITTED_FOLDER_API_ADD_ITEMS_IDENTITY]))
                # log.debug( u'%s -- folder_api_full_url, `%s`' % (self.log_identifier, folder_api_full_url) )
                # r = requests.get(folder_api_full_url, verify=False)
                folder_api_url_root = u'%s%s/' % ( self.FOLDER_API_URL, folder_id )
                log.debug( u'%s -- folder_api_url_root, `%s`' % (self.log_identifier, folder_api_url_root) )
                params = { 'identities': json.dumps([self.PERMITTED_FOLDER_API_ADD_ITEMS_IDENTITY]) }
                r = requests.get( folder_api_url_root, params )
                log.debug( u'%s -- requests url, `%s`' % (self.log_identifier, r.url) )
                if not r.ok:  # forbidden or not found
                  log.debug( u'%s -- error from collection api, `%s - %s`' % (self.log_identifier, r.status_code, r.text) )
                  # updateLog(message=u'uc.validateFolders() error from collection api: %s - %s' % (r.status_code, r.text))
                  return_dict =  {'status': 'FAILURE', 'message': u'folder not found'}
                  break
              # folder-id found, confirm name is correct
              if return_dict == 'init':
                folder_info = json.loads(r.text)
                if not folder_info['name'] == folder_name:
                  return_dict = { 'status': 'FAILURE', 'message': 'folder name/id mismatch' }
                  break
              # folder found, check if spreadsheet user is permitted to add items
              if return_dict == 'init':
                if spreadsheet_folder_api_identity in folder_info['add_items']:
                  normalized_cell_string = '%s+%s#%s' % ( normalized_cell_string, folder_name, folder_id )
                else:
                  return_dict = { 'status': 'FAILURE', 'message': 'not permitted to add items to specified folder' }
                  break

            # return
            if return_dict == 'init':
              normalized_cell_string = normalized_cell_string[1:]  # to get rid of initial '+'
              return_dict = { 'status': 'valid', 'normalized_cell_data': normalized_cell_string, 'parameter_label': 'folders' }
            log.info( u'%s -- return_dict, `%s`' % (self.log_identifier, return_dict) )
            # updateLog( message=u'validateFolders() return_dict is: %s' % return_dict, identifier=identifier )
            return return_dict
          except Exception as e:
            log.error( u'%s -- exception, `%s`' % (self.log_identifier, unicode(repr(e))) )
            return_dict =  { 'status': 'FAILURE', 'message': 'problem with "folders" entry' }
            return return_dict
          # end validateFolders()

    # end class Validator


class SheetGrabber( object ):
    """ Uses gspread to access spreadsheet. """

    def __init__( self,log_identifier ):
        self.CREDENTIALS_FILEPATH = os.environ['ASSMNT__CREDENTIALS_JSON_PATH']  # file produced by <http://gspread.readthedocs.org/en/latest/oauth2.html>
        self.SPREADSHEET_KEY = os.environ['ASSMNT__SPREADSHEET_KEY']
        self.scope = ['https://spreadsheets.google.com/feeds']
        self.log_identifier = log_identifier
        self.spreadsheet = None
        self.worksheet = None
        self.row_dcts = None
        self.original_ready_row_dct = None
        self.original_ready_row_num = None

    def get_spreadsheet( self ):
        """ Accesses googledoc spreadsheet. """
        try:
            json_key = json.load( open(self.CREDENTIALS_FILEPATH) )
            credentials = SignedJwtAssertionCredentials(
                json_key['client_email'], json_key['private_key'], self.scope )
            gc = gspread.authorize( credentials )
            self.spreadsheet = gc.open_by_key( self.SPREADSHEET_KEY )
            log.debug( u'%s -- spreadsheet grabbed, `%s`' % (self.log_identifier, self.spreadsheet) )
            return self.spreadsheet
        except Exception as e:
            message = u'Problem grabbing spreadsheet; exception, `%s`' % unicode(repr(e))
            log.error( message )
            raise Exception( message )

    def get_worksheet( self ):
        """ Accesses correct worksheet. """
        self.worksheet = self.spreadsheet.get_worksheet(0)
        log.debug( u'%s -- worksheet grabbed, `%s`' % (self.log_identifier, self.worksheet) )
        return self.worksheet

    def find_ready_row( self ):
        """ Searches worksheet for row ready for ingestion. """
        self.row_dcts = self.worksheet.get_all_records( empty2zero=False, head=1 )
        for (i, row_dct) in enumerate( self.row_dcts ):
            if row_dct['Ready'].strip() == 'Y':
                self.original_ready_row_dct = row_dct
                displayed_row_num = i + 2
                self.original_ready_row_num = displayed_row_num
                break
        log.debug( u'%s -- find-ready-row() complete; `%s`' % (self.log_identifier, pprint.pformat(self.original_ready_row_dct)) )
        return self.original_ready_row_dct

    def prepare_working_dct( self ):
        """ Converts default row dct to expected dct format for api call. """
        rights_view = self.original_ready_row_dct['Rights-View'].strip()
        rights_update = self.original_ready_row_dct['Rights-Update'].strip()
        rights_delete = self.original_ready_row_dct['Rights-Delete'].strip()
        working_dct = {
            'additional_rights': { 'view': rights_view, 'update': rights_update, 'delete': rights_delete },
            'by': self.original_ready_row_dct['Creator'].strip(),
            'create_date': self.original_ready_row_dct['DateCreated'].strip(),
            'description': self.original_ready_row_dct['Description'].strip(),
            'file_path': self.original_ready_row_dct['Location'].strip(),
            'folders': self.original_ready_row_dct['Folders'].strip(),
            'keywords': self.original_ready_row_dct['Keywords'].strip(),
            'title': self.original_ready_row_dct['Title'].strip(),
            'ready': self.original_ready_row_dct['Ready'].strip(),
            'pid': self.original_ready_row_dct['PID'].strip(), }
        log.debug( u'%s -- working_dct, `%s`' % (self.log_identifier, pprint.pformat(working_dct)) )
        return working_dct

    # end class SheetGrabber


def ingestItem( validity_result_list, identifier ):
  '''
  - Purpose: hits the item-api with a POST
  - Called by: controller.py
  '''
  try:

    # POST setup
    from gdoc_spreadsheet_extraction.libs.holcomb_schneider_file_post import MultipartPostHandler
    import cookielib, datetime, urllib2
    result = 'init'
    cookies = cookielib.CookieJar()
    opener = urllib2.build_opener(
      urllib2.HTTPCookieProcessor(cookies),
      MultipartPostHandler,
      )

    # build POST parameters
    params = {}
    params[ 'identity' ] = settings.ITEM_API_IDENTITY
    params[ 'authorization_code' ] = settings.ITEM_API_KEY
    for entry in validity_result_list:
      if entry[ 'parameter_label' ] == 'file_path':
        params['actual_file'] = open( entry['normalized_cell_data'] )
      else:
        params[ entry['parameter_label'] ] = entry['normalized_cell_data']
    updateLog ( message=u'ingestItem(); params is: %s' % params, identifier=identifier )

    # POST
    post_json_string = opener.open( settings.ITEM_API_URL, params ).read()
    updateLog ( message=u'ingestItem(); post_json_string is: %s' % post_json_string, identifier=identifier )
    post_json_dict = json.loads( post_json_string )
    if post_json_dict['post_result'] == 'SUCCESS':
      return_dict = { 'status': 'success', 'post_json_dict': post_json_dict }
    else:
      return_dict = { 'status': 'FAILURE', 'message': 'ingestion problem; error logged' }
  except Exception, e:
    updateLog( message=u'ingestItem(); exception detail is: %s' % makeErrorString(sys.exc_info()), identifier=identifier, message_importance='high' )
    return_dict = { 'status': 'FAILURE', 'message': 'ingest failed; error logged' }

  # return
  updateLog ( message=u'ingestItem(); return_dict is: %s' % return_dict, identifier=identifier )
  return return_dict

  # end def ingestItem()



def makeErrorDict():
  '''
  - Called by: could be any exception block.
  - Purpose: to return detailed error information. To replace makeErrorString().
  '''
  import sys
  return {
    u'error_type': sys.exc_info()[0],
    u'error_message': sys.exc_info()[1],
    u'line_number': sys.exc_info()[2].tb_lineno }
  # end def makeErrorDict()



def makeErrorString( error_info ):
  '''
  - Called by: could be any exception block.
  - Purpose: to return detailed error information.
  '''
  return u'error-type - %s; error-message - %s; line-number - %s' % ( sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2].tb_lineno, )
  # end def makeErrorString()



# def makeOriginalRowDictData( gdata_row_object, identifier ):
#   '''
#   - Purpose: creates a simple dict of raw-row-values in preparation for an update.
#   - Called by: utility_code.prepareRowReplacementDictOnError() and utility_code.prepareRowReplacementDictOnSuccess()
#   '''
#   try:
#     from types import NoneType
#     assert type(gdata_row_object) == gdata.spreadsheet.SpreadsheetsList, type(gdata_row_object)
#     assert type(identifier) == unicode, type(identifier)
#     original_row_dict = {}
#     assert type(gdata_row_object.custom) == dict, type(gdata_row_object.custom)
#     assert type(gdata_row_object.custom[u'creator']) == gdata.spreadsheet.Custom, type(gdata_row_object.custom[u'creator'])
#     assert type(gdata_row_object.custom[u'creator'].text) == str, type(gdata_row_object.custom[u'creator'].text)
#     custom_keys = gdata_row_object.custom.keys()
#     for key in ['creator', 'datecreated', 'description', 'folders', 'ingestionstatus', 'keywords', 'location', 'notes-nonbdr', 'orangerequired', 'pid', 'ready', 'rights-delete', 'rights-update', 'rights-view', 'tempurl', 'title']:
#         assert key in custom_keys, u'%s not in custom_keys %s' % (key, custom_keys)
#     for k, v in gdata_row_object.custom.items():
#       if type(v.text) != NoneType:
#         original_row_dict[k.decode(u'utf-8', u'replace')] = v.text.decode(u'utf-8', u'replace')
#       else:
#         original_row_dict[k.decode(u'utf-8', u'replace')] = None
#     updateLog( message=u'- in makeOriginalRowDictData(); original_row_dict is: %s' % original_row_dict, identifier=identifier )
#     return original_row_dict
#   except Exception, e:
#     message = message=u'makeOriginalRowDictData(); exception detail is: %s' % makeErrorString(sys.exc_info())
#     updateLog( message=message, identifier=identifier, message_importance=u'high' )
#     return_dict = { u'status': u'FAILURE', u'error_message': message }
#   # end def makeOriginalRowDictData()



# def makeRowDataDict( gdata_row_object, identifier ):
#   '''
#   - Purpose: converts gdata_row_object into regular dict.
#   - Called by: controller.py
#   '''
#   updateLog( message=u'gdata_row_object is: %s' % gdata_row_object, identifier=identifier )

#   rights_view = gdata_row_object.custom['rights-view'].text
#   rights_update = gdata_row_object.custom['rights-update'].text
#   rights_delete = gdata_row_object.custom['rights-delete'].text

#   row_dict = {
#     'additional_rights': { 'view': rights_view, 'update': rights_update, 'delete': rights_delete },
#     'by': gdata_row_object.custom['creator'].text,
#     'create_date': gdata_row_object.custom['datecreated'].text,
#     'description': gdata_row_object.custom['description'].text,
#     'file_path': gdata_row_object.custom['location'].text,
#     'folders': gdata_row_object.custom['folders'].text,
#     'keywords': gdata_row_object.custom['keywords'].text,
#     'title': gdata_row_object.custom['title'].text,
#     # new as of 2012-05-25
#     'ready': gdata_row_object.custom['ready'].text,
#     'pid': gdata_row_object.custom['pid'].text,
#     # 'delete': gdata_row_object.custom['delete'].text,  # disabled 2013-04-18; was causing error; production spreadsheet does not have this column.
#   }

#   updateLog( message=u'row_dict is: %s' % row_dict, identifier=identifier )
#   return row_dict

#   # end def makeRowDataDict()



def prepareMessageString( now_string, previous_message, new_message, identifier ):
  '''
  - Purpose: preps the new message string; breaking it out makes it easier for testing
  - Called by: utility_code.prepareRowReplacementDictOnError() & utility_code.prepareRowReplacementDictOnSuccess()
  '''
  try:
    updateLog( message=u'prepareMessageString(); previous_message is: %s' % previous_message, identifier=identifier )
    if previous_message == None or len( previous_message.strip() ) == 0:
      return_string = '%s -- %s\n----' % ( now_string, new_message )
    else:
      return_string = '%s -- %s\n----\n\n%s' % ( now_string, new_message, previous_message )
    return_dict =  { 'status': 'success', 'formatted_new_message': return_string }
  except Exception, e:
    updateLog( message=u'prepareMessageString(); exception detail is: %s' % makeErrorString(sys.exc_info()), identifier=identifier, message_importance='high' )
    return_dict = { 'status': 'FAILURE', 'message': 'problem preparing new message string' }
  updateLog( message=u'prepareMessageString(); return_dict is: %s' % return_dict, identifier=identifier )
  return return_dict
  # end def prepareMessageString()



def prepareRowReplacementDictOnError( gdata_row_object, error_message, identifier ):
  '''
  - Purpose: to prepare the row replacement dict on a validation error (all columns required).
  - Called by: controller.py
  - TODO: merge with prepareRowReplacementDictOnSuccess()
  '''
  import datetime

  try:
    original_row_dict = makeOriginalRowDictData( gdata_row_object, identifier )
    updateLog( message=u'prepareRowReplacementDictOnError(); original_row_dict is: %s' % original_row_dict, identifier=identifier )
    new_row_dict = original_row_dict.copy()
    if new_row_dict['ingestionstatus'] == None:
      new_row_dict['ingestionstatus'] = ''
    new_row_dict[ 'ready' ] = 'Error'
    # prepare error message
    updateLog( message=u'prepareRowReplacementDictOnError(); error_message is: %s' % error_message, identifier=identifier )
    now_string = str(datetime.datetime.now())[0:19]
    full_message = prepareMessageString(
      now_string=str(datetime.datetime.now())[0:19],
      previous_message=original_row_dict['ingestionstatus'],
      new_message=error_message,
      identifier=identifier )
    new_row_dict[ 'ingestionstatus' ] = full_message['formatted_new_message']
    updateLog( message=u'prepareRowReplacementDictOnError(); new_row_dict is: %s' % new_row_dict, identifier=identifier )
    # return
    return_dict = { 'status': 'success', 'replacement_dict': new_row_dict }

  except Exception, e:
    updateLog( message=u'prepareRowReplacementDictOnError(); exception detail is: %s' % makeErrorString(sys.exc_info()), message_importance='high' )
    return_dict = { 'status': 'FAILURE', 'message': 'problem preparing error-replacement values' }

  updateLog( message=u'prepareRowReplacementDictOnError(); return_dict is: %s' % return_dict, identifier=identifier )
  return return_dict

  # end def prepareRowReplacementDictOnError()



def prepareRowReplacementDictOnSuccess( gdata_row_object, pid, identifier ):
  '''
  - Purpose: prepares the row replacement dict on validation success (all columns required).
  - Called by: controller.py
  - TODO: merge with prepareRowReplacementDictOnError()
  '''
  import datetime

  try:
    original_row_dict = makeOriginalRowDictData( gdata_row_object, identifier )
    updateLog( message=u'prepareRowReplacementDictOnSuccess(); original_row_dict is: %s' % original_row_dict, identifier=identifier )
    new_row_dict = original_row_dict.copy()
    if new_row_dict['ingestionstatus'] == None:
      new_row_dict['ingestionstatus'] = ''
    new_row_dict[ 'ready' ] = 'Ingested'  # ingested
    # prepare ingestion message
    message_string = 'Item ingested, and is accessable at http://%s/studio/item/%s/' % ( settings.HOST_DOMAIN_NAME, pid )
    updateLog( message=u'prepareRowReplacementDictOnSuccess(); message is: %s' % message_string, identifier=identifier )
    now_string = str(datetime.datetime.now())[0:19]
    full_message = prepareMessageString(
      now_string=str(datetime.datetime.now())[0:19],
      previous_message=original_row_dict['ingestionstatus'],
      new_message=message_string,
      identifier=identifier )
    new_row_dict[ 'ingestionstatus' ] = full_message['formatted_new_message']
    updateLog( message=u'prepareRowReplacementDictOnSuccess(); new_row_dict is: %s' % new_row_dict, identifier=identifier )
    # return
    return_dict = { 'status': 'success', 'replacement_dict': new_row_dict }

  except Exception, e:
    updateLog( message=u'prepareRowReplacementDictOnSuccess(); exception detail is: %s' % makeErrorString(sys.exc_info()), message_importance='high' )
    return_dict = { 'status': 'FAILURE', 'message': 'problem preparing success-replacement values' }

  updateLog( message=u'prepareRowReplacementDictOnSuccess(); return_dict is: %s' % return_dict, identifier=identifier )
  return return_dict

  # end def prepareRowReplacementDictOnSuccess()



def prepareRowReplacementDictOnDeletionSuccess( gdata_row_object, pid, identifier ):
  '''
  - Purpose: prepares the row replacement dict on deletion success.
  - Called by: controller.py
  - TODO: merge with prepareRowReplacementDictOnError()
  '''
  import datetime
  assert type(gdata_row_object) == gdata.spreadsheet.SpreadsheetsList, type(gdata_row_object)
  assert type(pid) == unicode, type(pid)
  assert type(identifier) == unicode, type(identifier)
  try:
    original_row_dict = makeOriginalRowDictData( gdata_row_object, identifier )
    updateLog( message=u'- in prepareRowReplacementDictOnDeletionSuccess(); original_row_dict is: %s' % original_row_dict, identifier=identifier )
    new_row_dict = original_row_dict.copy()
    new_row_dict[ u'delete' ] = u'Deleted'
    updateLog( message=u'- in prepareRowReplacementDictOnDeletionSuccess(); new_row_dict is: %s' % new_row_dict, identifier=identifier )
    return_dict = { u'status': u'success', u'replacement_dict': new_row_dict }
  except Exception, e:
    message=u'- in prepareRowReplacementDictOnDeletionSuccess(); exception detail is: %s' % makeErrorString(sys.exc_info())
    updateLog( message, message_importance=u'high' )
    return_dict = { u'status': u'FAILURE', u'message': message }
  updateLog( message=u'- in prepareRowReplacementDictOnDeletionSuccess(); return_dict is: %s' % return_dict, identifier=identifier )
  return return_dict
  # end def prepareRowReplacementDictOnDeletionSuccess()



def runOverallValidity( validity_result_list, identifier ):
  '''
  - Purpose: to assess the results of all of the individual validity tests.
  - Called by: controller.py
  '''
  # run through each validity-check
  updateLog( message=u'runOverallValidity(); validity_result_list is: %s' % validity_result_list, identifier=identifier )
  problem_message_list = []
  for entry in validity_result_list:
    if not 'valid' in entry['status']:
      problem_message_list.append( entry['message'] )
  updateLog( message=u'runOverallValidity(); problem_message_list is: %s' % problem_message_list, identifier=identifier )
  # build problem-list if necessary
  if len( problem_message_list ) > 0:
    return_dict = {
      'status': 'FAILURE',
      'message': 'File not ingested; errors: %s' % ', '.join(problem_message_list)
      }
  else:
    return_dict = { 'status': 'valid' }
  updateLog( message=u'runOverallValidity(); return_dict is: %s' % return_dict, identifier=identifier )
  # return
  return return_dict
  # end def runOverallValidity()


def updateSpreadsheet( gdata_client, gdata_row_object, replacement_dict, identifier ):
  '''
  - Purpose: updates the spreadsheet row with prepared info.
  - Called by controller.py
  '''
  try:
    assert type(gdata_client) == gdata.spreadsheet.service.SpreadsheetsService, type(gdata_client)
    assert type(gdata_row_object) == gdata.spreadsheet.SpreadsheetsList, type(gdata_row_object)
    assert type(replacement_dict) == dict, type(replacement_dict)
    assert type(identifier) == unicode, type(identifier)
    updated_row_object = u'init'
    updated_row_object = gdata_client.UpdateRow( gdata_row_object, replacement_dict )
    updateLog( message=u'updateSpreadsheet(); updated_row_object is: %s' % updated_row_object, identifier=identifier )
    return_dict = { u'status': u'success', u'updated_row_object': updated_row_object }
  except Exception, e:
    updateLog( message=u'updateSpreadsheet(); exception detail is: %s' % makeErrorString(sys.exc_info()), identifier=identifier, message_importance='high' )
    return_dict = { u'status': u'FAILURE', u'message': u'problem updating spreadsheet' }
  return return_dict
  # end def updateSpreadsheet()


def validateDeletionDict( deletion_dict, log_id ):
  '''
  - Purpose: sanity check on title, filename, and deletion permissions.
  - Called by: controller_delete_new.py
  '''
  try:
    import json
    import requests
    assert type(deletion_dict) == dict, type(deletion_dict)
    assert sorted(deletion_dict.keys()) == [u'file_path', u'permitted_deleters', u'pid', u'title'], sorted(deletion_dict.keys())
    assert type(log_id) == unicode, type(log_id)
    assert type(settings.ITEM_API_URL) == unicode, type(settings.ITEM_API_URL)
    assert type(settings.ITEM_API_IDENTITY) == unicode, type(settings.ITEM_API_IDENTITY)
    assert type(settings.ITEM_API_KEY) == unicode, type(settings.ITEM_API_KEY)
    ## get item-api data
    url = u'%s?pid=%s' % ( settings.ITEM_API_URL, deletion_dict[u'pid'] )
    username = settings.ITEM_API_IDENTITY
    password = settings.ITEM_API_KEY
    r = requests.get( url, auth=(username, password), verify=False )
    # print u'- r.content:'; print r.content
    assert r.status_code == 200, r.status_code  # int
    updateLog( message=u'- in uc.validateDeletionDict(); r text is: %s' % r.content.decode(u'utf-8', u'replace'), identifier=log_id )
    bdr_info = json.loads( r.content.decode(u'utf-8', u'replace') )
    assert sorted(bdr_info.keys()) == [u'request', u'response'], sorted(bdr_info.keys())
    assert sorted(bdr_info['response'].keys()) == [u'additional_data', u'standard_data'], sorted(bdr_info['response'].keys())
    assert sorted(bdr_info['response'][u'standard_data'].keys()) == [u'additional_rights', u'by', u'create_date', u'description', u'filename', u'folders', u'keywords', u'title'], sorted(bdr_info['response'][u'standard_data'].keys())
    validity_errors = []
    ## check title
    if not deletion_dict[u'title'] == bdr_info[u'response'][u'standard_data'][u'title']:
      validity_errors.append( u'title_mismatch' )
    ## check filename
    deletion_filename = deletion_dict[u'file_path'].split(u'/')[-1]
    if not deletion_filename == bdr_info[u'response'][u'standard_data'][u'filename']:
      validity_errors.append( u'filename_mismatch' )
    ## check deletion rights
    bdr_rights_holders = bdr_info[u'response'][u'standard_data'][u'additional_rights']
    match_flag = u'init'
    for pd in deletion_dict[u'permitted_deleters']:
      for brh in bdr_rights_holders:
        if brh[u'identity'] == pd:  # an identity match, but what about permissions?
          if u'delete' in brh[u'permissions']:  # good!
            match_flag = u'match_found'
            break
    if not match_flag == u'match_found':
      validity_errors.append( u'invalid deletion permissions' )
    ## return
    if len(validity_errors) == 0:
      return_dict = { u'status': u'valid' }
    else:
      return_dict = { u'status': u'FAILURE', u'error_list': validity_errors }
    updateLog( message=u'- in uc.validateDeletionDict(); return_dict is: %s' % return_dict, identifier=log_id )
    return return_dict
  except:
    error_dict = makeErrorDict()
    print u'validateDeletionDict() exception -'; pprint.pprint( error_dict )
    updateLog( message=u'- in uc.validateDeletionDict(); exception detail is: %s' % error_dict, message_importance='high', identifier=log_id )
    return { u'status': u'FAILURE', u'data': error_dict }


def validateKeywords( cell_data, identifier ):
  '''
  - Purpose: a) validate 'keywords' data; b) create a postable string for the item-api
  - Called by: controller.py
  '''
  try:
    updateLog( message=u'validateKeywords() cell_data is: %s' % cell_data, identifier=identifier )
    # ensure not empty
    if len( cell_data.strip() ) == 0:
      return_dict = { 'status': 'FAILURE', 'message': 'at least one keyword is required' }
    else:
      # split on space-pipe-space
      keyword_list = cell_data.split( ' | ' )
      updateLog( message=u'validateKeywords() keyword_list is: %s' % keyword_list, identifier=identifier )
      cleaned_list = []
      for entry in keyword_list:
        cleaned_entry = entry.strip()
        cleaned_list.append( cleaned_entry )
      updateLog( message=u'validateKeywords() cleaned_list is: %s' % cleaned_list, identifier=identifier )
      cleaned_list.sort()
      updateLog( message=u'validateKeywords() cleaned_list b is: %s' % cleaned_list, identifier=identifier )
      return_string = ''
      for keyword in cleaned_list:
        return_string = '%s+%s' % ( return_string, keyword )
      return_string = return_string[1:]  # get rid of initial '+'
      return_dict = { 'status': 'valid', 'normalized_cell_data': return_string, 'parameter_label': 'keywords' }
    updateLog( message=u'validateKeywords() return_dict is: %s' % return_dict, identifier=identifier )
    return return_dict
  except Exception, e:
    return_dict =  { 'status': 'FAILURE', 'message': 'problem with "keywords" entry' }
    updateLog( message=u'validateKeywords() return_dict is: %s' % return_dict, identifier=identifier )
    updateLog( message=u'validateKeywords() exception detail is: %s' % makeErrorString(sys.exc_info()), identifier=identifier, message_importance='high' )
    return return_dict
  # end validateKeywords()



def validateTitle( cell_data, identifier ):
  '''
  - Purpose: a) validate 'description' data; b) create a postable string for the item-api
  - Called by: controller.py
  - TODO: add test for unicode issues
  '''
  try:
    if len( cell_data ) == 0:
      return_dict = { 'status': 'FAILURE', 'message': '"title" required' }
    else:
      return_dict = { 'status': 'valid', 'normalized_cell_data': cell_data, 'parameter_label': 'title' }
    updateLog( message=u'validateTitle() return_dict is: %s' % return_dict, identifier=identifier )
    return return_dict
  except Exception, e:
    return_dict =  { 'status': 'FAILURE', 'message': 'problem with "title" entry' }
    updateLog( message=u'validateTitle() return_dict is: %s' % return_dict, identifier=identifier )
    updateLog( message=u'validateTitle(); exception detail is: %s' % makeErrorString(sys.exc_info()), identifier=identifier, message_importance='high' )
    return return_dict

  # end def validateTitle()

