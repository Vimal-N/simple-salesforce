""" Classes for interacting with Salesforce Bulk API """

import json
import csv
from collections import OrderedDict
from time import sleep

import requests

from .util import call_salesforce


class SFBulkHandler2:
    """ Bulk API request handler
    Intermediate class which allows us to use commands,
     such as 'sf.bulk.Contacts.create(...)'
    This is really just a middle layer, whose sole purpose is
    to allow the above syntax
    """

    def __init__(self, session_id, bulk_url, proxies=None, session=None):
        """Initialize the instance with the given parameters.

        Arguments:

        * session_id -- the session ID for authenticating to Salesforce
        * bulk_url -- API endpoint set in Salesforce instance
        * proxies -- the optional map of scheme to proxy server
        * session -- Custom requests session, created in calling code. This
                     enables the use of requests Session features not otherwise
                     exposed by simple_salesforce.
        """
        self.session_id = session_id
        self.session = session or requests.Session()
        self.bulk_url = bulk_url
        print("Bulk API 2.0 URL: " + bulk_url)
        # don't wipe out original proxies with None
        if not session and proxies is not None:
            self.session.proxies = proxies

        # Define these headers separate from Salesforce class,
        # as bulk uses a slightly different format
        self.headers = {
            'Content-Type': 'application/json',
            'X-SFDC-Session': self.session_id,
            'X-PrettyPrint': '1'
        }
        # Different headers for job creation and setting Job status
        self.jobinfoheaders = {
            'Content-Type': 'application/json; charset=UTF-8',
            'Accept': 'application/json',
            'X-PrettyPrint': '1',
            'X-SFDC-Session': self.session_id,

        }
        # Different header for uploading data from a csv file
        self.uploaddataheaders = {
            'Content - Type': 'text/csv',
            'Accept': 'application/json',
            'X-SFDC-Session': self.session_id,
        }

    def __getattr__(self, name):
        return SFBulkType2(object_name=name, bulk_url=self.bulk_url,
                           headers=self.headers, jobinfoheaders=self.jobinfoheaders,
                           uploaddataheaders=self.uploaddataheaders, session=self.session)


class SFBulkType2:
    """ Interface to Bulk2.0 Async API functions"""

    def __init__(self, object_name, bulk_url, headers, jobinfoheaders, uploaddataheaders, session):
        """Initialize the instance with the given parameters.

        Arguments:

        * object_name -- the name of the type of SObject this represents,
                         e.g. `Lead` or `Contact`
        * bulk2_url -- API endpoint set in Salesforce instance
        * headers -- bulk API headers. separate header for Create Job, Data Upload, Set Job Status
        * session -- Custom requests session, created in calling code. This
                     enables the use of requests Session features not otherwise
                     exposed by simple_salesforce.
        """
        self.object_name = object_name
        self.bulk_url = bulk_url
        self.session = session
        self.headers = headers
        self.jobinfoheaders = jobinfoheaders
        self.uploaddataheaders = uploaddataheaders

    def _create_job(self, operation, object_name, external_id_field=None):
        """ Create a bulk job

        Arguments:

        * operation -- Bulk operation to be performed by job
        * object_name -- SF object
        * external_id_field -- unique identifier field for upsert operations
        """

        payload = {
            'operation': operation,
            'object': object_name,
            "contentType": "CSV"
        }

        if operation == 'upsert':
            payload['externalIdFieldName'] = external_id_field

        url = "{}{}".format(self.bulk_url, 'jobs/ingest/')

        result = call_salesforce(url=url, method='POST', session=self.session,
                                 headers=self.jobinfoheaders,
                                 data=json.dumps(payload))
        return result.json(object_pairs_hook=OrderedDict)

    def _close_job(self, job_id):
        """ Close a bulk job """
        payload = {
            'state': 'Closed'
        }

        url = "{}{}{}".format(self.bulk_url, 'job/', job_id)

        result = call_salesforce(url=url, method='POST', session=self.session,
                                 headers=self.headers,
                                 data=json.dumps(payload))
        return result.json(object_pairs_hook=OrderedDict)

    def _get_job(self, job_id):
        """ Get an existing job to check the status """
        url = "{}{}{}".format(self.bulk_url, 'job/', job_id)

        result = call_salesforce(url=url, method='GET', session=self.session,
                                 headers=self.headers)
        return result.json(object_pairs_hook=OrderedDict)

    def _get_csvdata(self, data):
        """ Process Comma Separated file
        return as List
        """
        rows = []
        with open(data, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            for row in csvreader:
                rows.append(row)
        return rows

    def _add_batch(self, job_id, data, operation):
        """ Add a set of data as a batch to an existing job
        Separating this out in case of later
        implementations involving multiple batches
        """

        url = "{}{}{}{}".format(self.bulk_url, 'jobs/ingest/', job_id, '/batches/')

        if operation != 'query':
            data = self._get_csvdata(self, data)

        result = call_salesforce(url=url, method='PUT', session=self.session,
                                 headers=self.uploaddataheaders, data=data)
        return result.json(object_pairs_hook=OrderedDict)

    def _set_job_status(self, job_id, set_status):
        """ Set the job status
        UploadComplete - data upload completed and Job is ready for Salesforce Process
        Aborted - Abort the Job in its current state
        """
        url = "{}{}{}".format(self.bulk_url, 'jobs/ingest/', job_id)

        # construct payload
        payload = {}
        if set_status == 'UploadComplete':
            payload['state'] = 'UploadComplete'
        if set_status == 'Aborted':
            payload['state'] = 'Aborted'

        # call Salesforce and send the request
        result = call_salesforce(url=url, method='PATCH', session=self.session,
                                 headers=self.jobinfoheaders, data=json.dumps(payload))

        # return the result back to the call
        return result.json(object_pairs_hook=OrderedDict)

    def _get_batch(self, job_id, batch_id):
        """ Get an existing batch to check the status """

        url = "{}{}{}{}{}".format(self.bulk_url, 'job/',
                                  job_id, '/batch/', batch_id)

        result = call_salesforce(url=url, method='GET', session=self.session,
                                 headers=self.headers)
        return result.json(object_pairs_hook=OrderedDict)

    def _get_batch_results(self, job_id, batch_id, operation):
        """ retrieve a set of results from a completed job """

        url = "{}{}{}{}{}{}".format(self.bulk_url, 'job/', job_id, '/batch/',
                                    batch_id, '/result')

        result = call_salesforce(url=url, method='GET', session=self.session,
                                 headers=self.headers)

        if operation == 'query':
            url_query_results = "{}{}{}".format(url, '/', result.json()[0])
            query_result = call_salesforce(url=url_query_results, method='GET',
                                           session=self.session,
                                           headers=self.headers)
            return query_result.json()

        return result.json()

    # pylint: disable=R0913
    def _bulk_operation(self, object_name, operation, data,
                        external_id_field=None, wait=5):
        """ String together helper functions to create a complete
        end-to-end bulk API request

        Arguments:

        * object_name -- SF object
        * operation -- Bulk operation to be performed by job
        * data -- list of dict to be passed as a batch
        * external_id_field -- unique identifier field for upsert operations
        * wait -- seconds to sleep between checking batch status
        """

        job = self._create_job(object_name=object_name, operation=operation,
                               external_id_field=external_id_field)

        batch = self._add_batch(job_id=job['id'], data=data,
                                operation=operation)

        self._close_job(job_id=job['id'])

        batch_status = self._get_batch(job_id=batch['jobId'],
                                       batch_id=batch['id'])['state']

        while batch_status not in ['Completed', 'Failed', 'Not Processed']:
            sleep(wait)
            batch_status = self._get_batch(job_id=batch['jobId'],
                                           batch_id=batch['id'])['state']

        results = self._get_batch_results(job_id=batch['jobId'],
                                          batch_id=batch['id'],
                                          operation=operation)
        return results

    # _bulk_operation wrappers to expose supported Salesforce bulk operations
    def delete(self, data):
        """ soft delete records """
        results = self._bulk_operation(object_name=self.object_name,
                                       operation='delete', data=data)
        return results

    def insert(self, data):
        """ insert records """
        results = self._bulk_operation(object_name=self.object_name,
                                       operation='insert', data=data)
        return results

    def upsert(self, data, external_id_field):
        """ upsert records based on a unique identifier """
        results = self._bulk_operation(object_name=self.object_name,
                                       operation='upsert',
                                       external_id_field=external_id_field,
                                       data=data)
        return results

    def update(self, data):
        """ update records """
        results = self._bulk_operation(object_name=self.object_name,
                                       operation='update', data=data)
        return results

    def hard_delete(self, data):
        """ hard delete records """
        results = self._bulk_operation(object_name=self.object_name,
                                       operation='hardDelete', data=data)
        return results

    def query(self, data):
        """ bulk query """
        results = self._bulk_operation(object_name=self.object_name,
                                       operation='query', data=data)
        return results
