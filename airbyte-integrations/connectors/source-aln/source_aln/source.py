#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
import urllib
import json
import dateutil.parser
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
            next_link = decoded_response.get("@odata.nextLink")
        except:
            return None
        next_url = urllib.parse.urlparse(next_link)
        return {str(k): str(v) for (k, v) in urllib.parse.parse_qsl(next_url.query)}
        
    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice, next_page_token)
        params['apikey'] = self.apikey
        if self.name in ["apartments", "apt_amenities" "contacts", "markets", "submarkets", "new_constructions", "owners", "management_companies"]:
            if stream_state not in [None, {}]:
                params['$filter'] = f"RowVersion gt {stream_state.get(self.cursor_field[-1])}"
            else:
                params['$filter'] = f"RowVersion gt 463750000"
            if self.name == "apartments":
                params['$expand'] = "PhoneNumbers,Addresses"
            elif self.name == "contacts":
                params['$expand'] = "JobCategories"
            return {**params, **next_page_token} if next_page_token else params
        elif self.name == "inactive_entities":
            if stream_state not in [None, {}]:
                params['$filter'] = f"EntityRowVersion gt {stream_state.get(self.cursor_field[-1])}"
            else:
                params['$filter'] = f"EntityRowVersion gt 463750000"
        elif self.name == "purged_entities":
            if stream_state not in [None, {}]:
                params['$filter'] = f"PurgedRowVersion gt {stream_state.get(self.cursor_field[-1])}"
            else:
                params['$filter'] = f"PurgedRowVersion gt 463750000"
        elif self.name == "schools":
            if stream_state not in [None, {}]:
                params['$filter'] = f"SchoolLastDateChanged gt {stream_state.get(self.cursor_field[-1])}"
            else:
                params['$filter'] = f"SchoolLastDateChanged gt 2022-02-02T16:15:23.693Z"
        elif self.name == "school_districts":
            if stream_state not in [None, {}]:
                params['$filter'] = f"SchoolDistrictLastDateChanged gt {stream_state.get(self.cursor_field[-1])}"
            else:
                params['$filter'] = f"SchoolDistrictLastDateChanged gt 2022-02-02T16:15:23.693Z"
        return {**params, **next_page_token} if next_page_token else params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        json_response = response.json()
        records = json_response.get("value", [])
        for record in records:
            if self.name == "apartments":
                record["PhoneNumbers"] = str(record.get("PhoneNumbers"))
                record["Addresses"] = str(record.get("Addresses"))
            elif self.name == "contacts":
                record["JobCategories"] = str(record.get("JobCategories"))
        yield from records


# Basic incremental stream
class IncrementalAlnStream(AlnStream, ABC):
    state_checkpoint_interval = 1

    @property
    def cursor_field(self) -> List[str]:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return ['RowVersion']

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        cursor = self.cursor_field[-1]
        if self.name in ["schools", "school_districts"]:
            if current_stream_state.get(cursor) != None:
                if dateutil.parser.parse(latest_record.get(cursor)) > dateutil.parser.parse(current_stream_state.get(cursor)):
                    latest_row_version = latest_record.get(cursor)
                else:
                    latest_row_version = current_stream_state.get(cursor)
            else:
                latest_row_version = latest_record.get(cursor)
            return {cursor: latest_row_version}
            
        else:
            if current_stream_state.get(cursor) != None:
                latest_row_version = max(current_stream_state.get(cursor), latest_record.get(cursor))
            else:
                latest_row_version = latest_record.get(cursor)
            return {cursor: latest_row_version}


### Basic streams

class ActiveRowVersion(AlnStream):
    primary_key = "HighestRowVersion"

    def __init__(self, apikey: str):
        super().__init__()
        self.apikey = apikey

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return 'ActiveRowVersion'


class Amenities(AlnStream):
    primary_key = "AmenityId"

    def __init__(self, apikey: str):
        super().__init__()
        self.apikey = apikey

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return "Amenities"


class AmenityGroups(AlnStream):
    primary_key = "AmenityGroupId"

    def __init__(self, apikey: str):
        super().__init__()
        self.apikey = apikey

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return "AmenityGroups"


class JobCategories(AlnStream):
    primary_key = "JobCategoryId"

    def __init__(self, apikey: str):
        super().__init__()
        self.apikey = apikey

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return "JobCategories"


class StatusCodes(AlnStream):
    primary_key = "Status"

    def __init__(self, apikey: str):
        super().__init__()
        self.apikey = apikey

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return "StatusCodes"


### Incremental streams

class Apartments(IncrementalAlnStream):
    primary_key = "ApartmentId"

    def __init__(self, apikey: str):
        super().__init__()
        self.apikey = apikey

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return "FlatApartments"


class AptAmenities(IncrementalAlnStream):
    primary_key = ["AmenityId", "ApartmentId"]

    def __init__(self, apikey: str):
        super().__init__()
        self.apikey = apikey

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return "AptAmenities"


class Contacts(IncrementalAlnStream):
    primary_key = "ContactId"

    def __init__(self, apikey: str):
        super().__init__()
        self.apikey = apikey

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return "Contacts"


class ManagementCompanies(IncrementalAlnStream):
    primary_key = "ManagementCompanyEntityId"

    def __init__(self, apikey: str):
        super().__init__()
        self.apikey = apikey

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return "ManagementCompanies"


class Markets(IncrementalAlnStream):
    primary_key = "MarketId"

    def __init__(self, apikey: str):
        super().__init__()
        self.apikey = apikey

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return "Markets"


class Submarkets(IncrementalAlnStream):
    primary_key = "SubmarketId"

    def __init__(self, apikey: str):
        super().__init__()
        self.apikey = apikey

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return "Submarkets"


class NewConstructions(IncrementalAlnStream):
    primary_key = "NewConstructionId"

    def __init__(self, apikey: str):
        super().__init__()
        self.apikey = apikey

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return "NewConstructions"


class Owners(IncrementalAlnStream):
    primary_key = "OwnerId"

    def __init__(self, apikey: str):
        super().__init__()
        self.apikey = apikey

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return "Owners"


class PurgedEntities(IncrementalAlnStream):
    primary_key = "EntityId"

    cursor_field = ["PurgedRowVersion"]

    def __init__(self, apikey: str):
        super().__init__()
        self.apikey = apikey

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return "PurgedEntities"


class InactiveEntities(IncrementalAlnStream):
    primary_key = "InactiveEntityId"

    cursor_field = ["EntityRowVersion"]

    def __init__(self, apikey: str):
        super().__init__()
        self.apikey = apikey

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return "InactiveEntities"

class Schools(IncrementalAlnStream):
    primary_key = "SchoolEntityId"

    cursor_field = ["SchoolLastDateChanged"]

    def __init__(self, apikey: str):
        super().__init__()
        self.apikey = apikey

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return "Schools"


class SchoolDistricts(IncrementalAlnStream):
    primary_key = "SchoolDistrictEntityId"

    cursor_field = ["SchoolDistrictLastDateChanged"]

    def __init__(self, apikey: str):
        super().__init__()
        self.apikey = apikey

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return "SchoolDistricts"



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
        return [ActiveRowVersion(config['apikey']),
                Amenities(config['apikey']),
                AmenityGroups(config['apikey']),
                JobCategories(config['apikey']),
                Schools(config['apikey']),
                SchoolDistricts(config['apikey']),
                StatusCodes(config['apikey']),
                Apartments(config['apikey']),
                AptAmenities(config['apikey']),
                Contacts(config['apikey']),
                ManagementCompanies(config['apikey']),
                Markets(config['apikey']),
                Submarkets(config['apikey']),
                NewConstructions(config['apikey']),
                Owners(config['apikey']),
                PurgedEntities(config['apikey']),
                InactiveEntities(config['apikey']),
                ]
