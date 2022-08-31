#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
import urllib
import json
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""




# Basic full refresh stream
class AlnStream(HttpStream, ABC):
    """
    TODO remove this comment

    This class represents a stream output by the connector.
    This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
    parsing responses etc..

    Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.

    Typically for REST APIs each stream corresponds to a resource in the API. For example if the API
    contains the endpoints
        - GET v1/customers
        - GET v1/employees

    then you should have three classes:
    `class AlnStream(HttpStream, ABC)` which is the current class
    `class Customers(AlnStream)` contains behavior to pull data for customers using v1/customers
    `class Employees(AlnStream)` contains behavior to pull data for employees using v1/employees

    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalAlnStream((AlnStream), ABC)` then have concrete stream implementations extend it. An example
    is provided below.

    See the reference docs for the full list of configurable options.
    """

    # TODO: Fill in the url base. Required.
    url_base = "https://odata4.alndata.com/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()
        try:
            next = decoded_response.get("@odata.nextLink")
        except:
            return None
        next_url = urllib.parse.urlparse(next)
        #return None
        return {str(k): str(v) for (k, v) in urllib.parse.parse_qsl(next_url.query)}
        
    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        print(f"This is the stream_state: {stream_state}")
        params = super().request_params(stream_state, stream_slice, next_page_token)
        params['apikey'] = self.apikey
        #params['$filter'] = "RowVersion%20gt%20" + str(100000)
        return {**params, **next_page_token} if next_page_token else params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        json_response = response.json()
        records = json_response.get("value", [])
        yield from records
        #yield response.json()

    def read_records(self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_slice: Mapping[str, Any] = None,
                     stream_state: Mapping[str, Any] = None) -> Iterable[Mapping[str, Any]]:
        print(cursor_field)
        active_row_version = ActiveRowVersion(self.apikey)
        #stream_state['RowVersion'] = active_row_version
        yield from super().read_records(
                sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state
            )



# Basic incremental stream
class IncrementalAlnStream(AlnStream, ABC):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = 1

    @property
    def cursor_field(self) -> List[str]:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return ["data", "RowVersion"]

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        #print(latest_record)
        #return {self.cursor_field: latest_record.get('RowVersion')}
        if current_stream_state.get('RowVersion') != None:
            latest_row_version = max(current_stream_state.get('RowVersion'), latest_record.get('RowVersion'))
        else:
            latest_row_version = latest_record.get('RowVersion')
        return {self.cursor_field[-1]: latest_row_version}


class ActiveRowVersion(AlnStream):

    primary_key = "HighestRowVersion"

    def __init__(self, apikey: str):
        super().__init__()
        self.apikey = apikey

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return "ActiveRowVersion"



class Apartments(IncrementalAlnStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the cursor_field. Required.
    #cursor_field = "RowVersion"

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "ApartmentId"

    def __init__(self, apikey: str):
        super().__init__()
        self.apikey = apikey


    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/employees then this should
        return "single". Required.
        """
        print(stream_state)
        if stream_state != {}:
            return f"FlatApartments{stream_state}"
        else:
            return "FlatApartments"

    @property
    def supports_incremental(self) -> bool: 
         """ 
         :return: True if this stream supports incrementally reading data 
         """ 
         return True

    def source_defined_cursor(self) -> bool:
        return True 

    


# Source
class SourceAln(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        return [Apartments(config['apikey']),
                ActiveRowVersion(config['apikey'])]
