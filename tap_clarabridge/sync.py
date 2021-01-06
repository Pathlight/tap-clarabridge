import singer
from .client import ClarabridgeAPI
from datetime import datetime, timedelta
from singer.utils import strftime as singer_strftime
import math
import pytz


LOGGER = singer.get_logger()


def transform_date(value):
    return singer_strftime(datetime.utcfromtimestamp(value).replace(tzinfo=pytz.UTC))


def transform_value(key, value):
    date_fields = set(['date'])

    if key in date_fields:
        if type(value) == dict:
            value = {k: transform_date(v) for (k, v) in value.items()}
        elif type(value) == int:
            value = transform_date(value)

    return value


# TODO: Finish modifying this
def sync(config, state, catalog):
    """ Sync data from tap source """

    client = ClarabridgeAPI(config)

    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info('Syncing stream:' + stream.tap_stream_id)

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )

        if state:
            bookmark = state[stream.tap_stream_id]
        else:
            bookmark = config['start_date']

        url = 'inbox/mentions'
        bookmark_date = datetime.fromtimestamp(bookmark)
        last_action_minutes_ago = int(math.ceil((datetime.now() - bookmark_date).seconds / 60))
        params = {
            'date_from': bookmark_date - timedelta(days=90),
            'sort': 'timestamps.action_last_date:asc',
            'filter': f'status:alldone AND action_date_age:[0 TO {last_action_minutes_ago}]',
            'limit': client.MAX_PAGE_SIZE,
        }

        for record in client.paging_get(url, params):
            transformed_record = {k: transform_value(k, v) for (k, v) in record.items()}
            singer.write_record(stream.tap_stream_id, transformed_record)
            # Assuming actions are sorted in desc chron order
            new_bookmark = record['actions'][0]['date']['added']
            singer.write_state({stream.tap_stream_id: new_bookmark})

    return
