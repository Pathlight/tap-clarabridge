import requests
import singer
import time


LOGGER = singer.get_logger()


class ClarabridgeAPI:
    URL_TEMPLATE = 'https://api.engagor.com'
    MAX_RETRIES = 5
    RETRY_TIMEOUT = 10
    MAX_PAGE_SIZE = 100

    def __init__(self, config):
        self.access_token = config['access_token']
        self.account_id = config['account_id']

        self.base_url = self.URL_TEMPLATE

    def get_params(self, extra_params):
        params = {'access_token': self.access_token}

        if extra_params:
            params.update(extra_params)

        return params

    def get(self, url, params=None):
        """
        Gets data from the requested url (ensuring the base url is always
        Clarabridge's api). Handles rate limit errors and returns the response
        body as json.
        """

        if not url.startswith('https://'):
            if not url.startswith('me'):
                if not self.account_id:
                    raise Exception(f'Clarabridge query error: Missing account_id for API call to {url}.')
                url = f'{self.account_id}/{url}'
            url = f'{self.base_url}/{url}'

        url = set_query_parameters(url, **self.get_params(params))
        LOGGER.info(f'Clarabridge GET {url}')

        for num_retries in range(self.MAX_RETRIES + 1):
            resp = requests.get(url)
            try:
                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                # Rate limit error, which is currently a 400
                # but may later adopt the 429 convention
                if resp.status_code == 429 or (resp.status_code == 400 and resp.json().get('error') == 'rate_limit_reached') and num_retries < self.MAX_RETRIES:
                    # Hmm I don't think there's any point in retrying if we got rate-limited,
                    # as the limit will only reset in an hour (assuming it was just crossed)
                    limit_remaining = resp.headers['x-ratelimit-remaining']
                    until_reset = resp.headers['x-ratelimit-reset'] - time.time()
                    LOGGER.info('api query clarabridge rate limit', extra={
                        'limit_remaining': limit_remaining,
                        'until_reset': until_reset,
                        'account_id': self.account_id
                    })
                    if (until_reset <= self.RETRY_TIMEOUT):
                        time.sleep(self.RETRY_TIMEOUT)
                    else:
                        raise Exception(f'Clarabridge query error: Rate limit reached, resetting in {int(until_reset / 60)} minutes.')
                elif resp.status_code >= 500 and num_retries < self.MAX_RETRIES:
                    LOGGER.info('api query clarabridge service errors', extra={
                        'code': resp.content['error'],
                        'account_id': self.account_id
                    })
                else:
                    raise Exception('Clarabridge query error: Max retries exceeded.')

        return resp.json()['response']

    def paging_get(self, url, results_key='data'):
        """
        Iterates through all pages and yields results, one object at a time.
        """
        urls = set()
        total_returned = 0

        while url and url not in urls:
            urls.add(url)
            data = self.get(url, params={'limit': self.MAX_PAGE_SIZE})

            LOGGER.info('clarabridge paging request', extra={
                'total_size': data['count'],
                'page': len(urls),
                'results_key': results_key,
                'url': url,
                'total_returned': total_returned
            })

            for record in data[results_key]:
                total_returned += 1
                yield record

            url = data['paging']['next_url']

    def get_users(self):
        return self.paging_get('settings/users')
