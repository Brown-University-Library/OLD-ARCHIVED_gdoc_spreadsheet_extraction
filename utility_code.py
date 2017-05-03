# -*- coding: utf-8 -*-

import datetime, json, logging, os, pprint, sys
import gspread,requests
from oauth2client.client import SignedJwtAssertionCredentials


log = logging.getLogger(__name__)


class SheetUpdater( object ):
    """ Manages updates to spreadsheet on error and success.
        TODO: consider refactoring make-new-message defs, and update defs. """

    def __init__( self, log_identifier ):
        self.log_identifier = log_identifier
        self.HOST_DOMAIN_NAME = os.environ['ASSMNT__HOST_DOMAIN_NAME']
        self.ingestion_ready_column_name = u'Ready'
        self.ingestion_status_column_name = u'IngestionStatus'
        self.ready_column_int = None
        self.ingestion_status_column_int = None

    def update_on_success( self, worksheet, original_data_dct, row_num, pid ):
        """ Updates ready-column and message column.
            Called by controller. """
        log.info( u'%s -- starting update_on_success()' % self.log_identifier )
        self.ready_column_int = self.get_column_int( worksheet, self.ingestion_ready_column_name )
        self.ingestion_status_column_int = self.get_column_int( worksheet, self.ingestion_status_column_name )
        worksheet.update_cell(
            row_num, self.ready_column_int, u'Ingested' )
        new_message = self.make_new_success_message( original_data_dct, pid )
        worksheet.update_cell(
            row_num, self.ingestion_status_column_int, new_message )
        log.info( u'%s -- ending script' % self.log_identifier )
        sys.exit()

    def update_on_error( self, worksheet, original_data_dct, row_num, error_data ):
        """ Pulls error message from error_data & updates worksheet cell.
            Called by controller. """
        log.info( u'%s -- starting update_on_error()' % self.log_identifier )
        self.ready_column_int = self.get_column_int( worksheet, self.ingestion_ready_column_name )
        self.ingestion_status_column_int = self.get_column_int( worksheet, self.ingestion_status_column_name )
        worksheet.update_cell(
            row_num, self.ready_column_int, u'Error' )
        new_message = self.make_new_error_message( original_data_dct, error_data )
        worksheet.update_cell(
            row_num, self.ingestion_status_column_int, new_message )
        log.info( u'%s -- raising exception' % self.log_identifier )
        raise Exception('exception - see assessments log')

    def get_column_int( self, worksheet, column_name ):
        """ Returns integer for given column_name.
            Called by update_on_error() """
        ( column_int, error_message ) = ( None, u'Unable to determine column integer for column name, `%s`.' % column_name )
        for i in range( 1, 20 ):
            column_title = worksheet.cell( 1, i ).value  # cell( row, column )
            if column_name in column_title:  # column_title may contain a colon
                column_int = i
                break
        log.debug( u'%s -- column_int, `%s`' % (self.log_identifier, column_int) )
        if not column_int:
            log.error( u'%s -- raising exception, `%s`' % (self.log_identifier, error_message) )
            raise Exception( error_message )
        return column_int

    def make_new_success_message( self, original_data_dct, pid ):
        """ Adds date-stamped new success message containing bdr-link to beginning of old message & returns it.
            Called by update_on_success() """
        previous_message = original_data_dct['IngestionStatus']
        now = unicode( datetime.datetime.now() )[0:19]
        message = u'Item ingested, and is accessable at https://%s/studio/item/%s/' % ( self.HOST_DOMAIN_NAME, pid )
        if previous_message == None or len( previous_message.strip() ) == 0:
            new_message = u'%s -- %s\n----' % ( now, message )
        else:
            new_message = u'%s -- %s\n----\n\n%s' % ( now, message, previous_message )
        log.debug( u'%s -- new_message, `%s`' % (self.log_identifier, new_message) )
        return new_message

    def make_new_error_message( self, original_data_dct, error_data ):
        """ Adds date-stamped new error message to beginning of old message & returns it.
            Called by update_on_error() """
        previous_message = original_data_dct['IngestionStatus']
        now = unicode( datetime.datetime.now() )[0:19]
        if previous_message == None or len( previous_message.strip() ) == 0:
            new_message = u'%s -- %s\n----' % ( now, error_data['message'] )
        else:
            new_message = u'%s -- %s\n----\n\n%s' % ( now, error_data['message'], previous_message )
        log.debug( u'%s -- new_message, `%s`' % (self.log_identifier, new_message) )
        return new_message

    # end class SheetUpdater


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
                r = requests.get( folder_api_url_root, params, verify=False )
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
                if self.PERMITTED_FOLDER_API_ADD_ITEMS_IDENTITY in folder_info['add_items']:
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

    def validateKeywords( self, cell_data ):
          '''
          - Purpose: a) validate 'keywords' data; b) create a postable string for the item-api
          - Called by: controller.py
          '''
          try:
            log.debug( u'%s -- cell_data, `%s`' % (self.log_identifier, cell_data) )
            # ensure not empty
            if len( cell_data.strip() ) == 0:
              return_dict = { 'status': 'FAILURE', 'message': 'at least one keyword is required' }
            else:
              # split on space-pipe-space
              keyword_list = cell_data.split( ' | ' )
              log.debug( u'%s -- keyword_list, `%s`' % (self.log_identifier, keyword_list) )
              cleaned_list = []
              for entry in keyword_list:
                cleaned_entry = entry.strip()
                cleaned_list.append( cleaned_entry )
              log.debug( u'%s -- cleaned_list, `%s`' % (self.log_identifier, cleaned_list) )
              cleaned_list.sort()
              log.debug( u'%s -- cleaned_list b, `%s`' % (self.log_identifier, cleaned_list) )
              return_string = ''
              for keyword in cleaned_list:
                return_string = '%s+%s' % ( return_string, keyword )
              return_string = return_string[1:]  # get rid of initial '+'
              return_dict = { 'status': 'valid', 'normalized_cell_data': return_string, 'parameter_label': 'keywords' }
            log.info( u'%s -- return_dict, `%s`' % (self.log_identifier, return_dict) )
            return return_dict
          except Exception, e:
            log.error( u'%s -- exception, `%s`' % (self.log_identifier, unicode(repr(e))) )
            return_dict =  { 'status': 'FAILURE', 'message': 'problem with "keywords" entry' }
            return return_dict
          # end validateKeywords()

    def validateTitle( self, cell_data ):
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
            log.info( u'%s -- return_dict, `%s`' % (self.log_identifier, return_dict) )
            return return_dict
          except Exception, e:
            log.error( u'%s -- exception, `%s`' % (self.log_identifier, unicode(repr(e))) )
            return_dict =  { 'status': 'FAILURE', 'message': 'problem with "title" entry' }
            return return_dict

          # end def validateTitle()

    def runOverallValidity( self, validity_result_list ):
          '''
          - Purpose: to assess the results of all of the individual validity tests.
          - Called by: controller.py
          '''
          # run through each validity-check
          log.debug( u'%s -- validity_result_list, `%s`' % (self.log_identifier, pprint.pformat(validity_result_list)) )
          problem_message_list = []
          for entry in validity_result_list:
            if not 'valid' in entry['status']:
              problem_message_list.append( entry['message'] )
          log.debug( u'%s -- problem_message_list, `%s`' % (self.log_identifier, problem_message_list) )
          # build problem-list if necessary
          if len( problem_message_list ) > 0:
            return_dict = {
              'status': 'FAILURE',
              'message': 'File not ingested; errors: %s' % ', '.join(problem_message_list)
              }
          else:
            return_dict = { 'status': 'valid' }
          log.info( u'%s -- return_dict, `%s`' % (self.log_identifier, return_dict) )
          # return
          return return_dict
          # end def runOverallValidity()

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
            import traceback
            message = 'Problem grabbing spreadsheet; exception, `%s`' % traceback.format_exc()
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
                log.debug( u'%s -- self.original_ready_row_num, `%s`' % (self.log_identifier, self.original_ready_row_num) )
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


def ingestItem(validity_result_list):
    """ Posts data to item-api
        Called by controller. """
    URL = os.environ['ASSMNT__ITEM_API_URL']
    IDENTITY = os.environ['ASSMNT__ITEM_API_IDENTITY']
    KEY = os.environ['ASSMNT__ITEM_API_KEY']
    try:
        ## params
        params = {}
        params[u'identity'] = IDENTITY
        params[u'authorization_code'] = KEY
        filepath = u''
        for entry in validity_result_list:
            if entry[u'parameter_label'] == u'file_path':
                filepath = entry[u'normalized_cell_data']
            else:
                params[ entry[u'parameter_label'] ] = entry[u'normalized_cell_data']
        ## post
        files = { 'actual_file': open(filepath, 'rb') }
        r = requests.post( URL, data=params, files=files, verify=True )
        if r.ok:
            result_dct = r.json()
            if result_dct['post_result'] == u'SUCCESS':
                return_dict = { u'status': u'success', u'post_json_dict': result_dct }
            else:
                return_dict = { u'status': 'FAILURE', u'message': u'ingestion problem; error logged' }
        else:
            raise Exception('error posting to BDR: %s - %s' % (r.status_code, r.content))
    except Exception as e:
        import traceback
        msg = traceback.format_exc()
        log.error(u'Exception on ingest: `%s`' % msg.decode('utf8'))
        return_dict = {u'status': u'FAILURE', u'message': u'ingest failed; error logged'}
    return return_dict
