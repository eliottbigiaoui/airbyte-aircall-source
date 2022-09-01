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


# Basic full refresh stream
class AlnStream(HttpStream, ABC):
    url_base = "https://odata4.alndata.com/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()
        try:
            next = decoded_response.get("@odata.nextLink")
        except:
            return None
        next_url = urllib.parse.urlparse(next)
        return {str(k): str(v) for (k, v) in urllib.parse.parse_qsl(next_url.query)}
        
    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice, next_page_token)
        params['apikey'] = self.apikey
        if stream_state not in [None, {}]:
            params['$filter'] = f"RowVersion gt {stream_state.get('RowVersion')}"
        else:
            params['$filter'] = f"RowVersion gt 433218766"
        return {**params, **next_page_token} if next_page_token else params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        json_response = response.json()
        records = json_response.get("value", [])
        yield from records

    def read_records(self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_slice: Mapping[str, Any] = None,
                     stream_state: Mapping[str, Any] = None) -> Iterable[Mapping[str, Any]]:        
        yield from super().read_records(
                sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state
            )


# Basic incremental stream
class IncrementalAlnStream(AlnStream, ABC):
    state_checkpoint_interval = 100

    @property
    def cursor_field(self) -> List[str]:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return ['data', 'RowVersion']

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        if current_stream_state.get('RowVersion') != None:
            latest_row_version = max(current_stream_state.get('RowVersion'), latest_record.get('RowVersion'))
        else:
            latest_row_version = latest_record.get('RowVersion')
        return {self.cursor_field[-1]: latest_row_version}

"""
class ActiveRowVersion(AlnStream):

    primary_key = "HighestRowVersion"

    def __init__(self, apikey: str):
        super().__init__()
        self.apikey = apikey

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return 'ActiveRowVersion'
"""


class Apartments(IncrementalAlnStream):
    primary_key = "ApartmentId"

    def __init__(self, apikey: str):
        super().__init__()
        self.apikey = apikey

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/employees then this should
        return "single". Required.
        """
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
                #ActiveRowVersion(config['apikey']),
                #Contacts(config['apikey'])
                #Amenities(config['apikey']),
                #JobCategories(config['apikey']),
                #ManagementCompanies(config['apikey']),
                #Markets(config['apikey']),
                #NewConstructions(config['apikey']),
                #Owners(config['apikey']),
                #PurgedEntities(config['apikey']),
                #InactiveEntities(config['apikey']),
                #Schools(config['apikey']),
                #SchoolDistricts(config['apikey']),
                #StatusCodes(config['apikey']),
                #Submarkets(config['apikey'])
                ]
