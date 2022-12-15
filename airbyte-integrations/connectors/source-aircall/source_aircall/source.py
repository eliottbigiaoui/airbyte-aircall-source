#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from base64 import b64encode
import urllib
from datetime import datetime

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator


# Basic full refresh stream
class AircallStream(HttpStream, ABC):
    url_base = "https://api.aircall.io/v1/"
        
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()
        meta = decoded_response.get("meta")
        if not meta:
            return None

        next = meta.get("next_page_link")
        if not next:
            return None

        next_url = urllib.parse.urlparse(next)
        return {str(k): str(v) for (k, v) in urllib.parse.parse_qsl(next_url.query)}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice, next_page_token)
        if stream_state not in [None, {}]:
            if stream_state.get('ended_at') == None:
                params['from'] = 1671091566 #int(datetime.utcnow().timestamp()) - 260000
            else:
                params['from'] = 1671091566 #stream_state.get('ended_at')
        else:
            params['from'] = 1671091566 #int(datetime.utcnow().timestamp()) - 260000
        return {**params, **next_page_token} if next_page_token else params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        records = response_json.get("calls", [])
        for record in records:
            record["user"] = str(record.get("user"))
            record["number"] = str(record.get("number"))
        yield from records
        #yield response_json


# Basic incremental stream
class IncrementalAircallStream(AircallStream, ABC):
    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = 100

    @property
    def cursor_field(self) -> List[str]:
        """
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return ["ended_at"]

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        print(current_stream_state)
        print(self.state)
        print(latest_record)
        try:
            latest_call = latest_record['ended_at']
            if current_stream_state not in [None, {}]:
                if latest_call != None:
                    return {self.cursor_field[-1]: max(current_stream_state['ended_at'], latest_call)}
                else:
                    {self.cursor_field[-1]: current_stream_state['ended_at']}
            else:
                if latest_call != None:
                    return {self.cursor_field[-1]: latest_call}
                else:
                    return {}
        except Exception as e:
            print(e)
            return {}



class Calls(IncrementalAircallStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the cursor_field. Required.
    cursor_field = ["ended_at"]

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "id"

    def path(self, **kwargs) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/employees then this should
        return "single". Required.
        """
        return "calls"



# Source
class SourceAircall(AbstractSource):
    @staticmethod
    def get_authenticator(config: Mapping[str, Any]):
        token = b64encode(bytes(str(config["api_id"] + ":" + config["api_token"]), "utf-8")).decode("ascii")
        authenticator = TokenAuthenticator(token, auth_method="Basic")
        return authenticator

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            authenticator = self.get_authenticator(config)
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # TODO remove the authenticator if not required.
        authenticator = self.get_authenticator(config)
        return [Calls(authenticator=authenticator)]
