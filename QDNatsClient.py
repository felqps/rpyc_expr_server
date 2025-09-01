import asyncio
import pickle
import sys
from optparse import OptionParser
import nats.aio.errors

from nats.aio.client import Client
import logging

#from QDNatsServer import QD_SUBJECT

from QDSettings import SHARE_CODE_FOLDER


QD_SUBJECT = 'qd.request'


class QDNatsClient:

    def __init__(self, *, nats_url: str, timeout=120):
        self._loop = asyncio.get_event_loop()
        self._timeout = timeout

        async def error_cb(e):
            qps_print(f'Error e {e}')

        self._nats = Client()
        self._loop.run_until_complete(self._nats.connect(nats_url, loop=self._loop, error_cb=error_cb))

    def request(self, payload):

        result = self._loop.run_until_complete(self._request(payload))
        return result

    async def _request(self, payload):
        logging.info(f'Send Request, subject: {QD_SUBJECT}  payload: {payload}')
        return await self._nats.request(QD_SUBJECT,  pickle.dumps(payload), timeout=self._timeout)


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.DEBUG)
    parser = OptionParser(description="QDNatsServer")

    parser.add_option("--nats_url",
                      dest="nats_url",
                      help="nats_url (default: %default)",
                      metavar="nats_url",
                      default="nats://192.168.20.141:4222")
    parser.add_option("--payload",
                      dest="payload",
                      help="payload",
                      default="test payload")
    options, _ = parser.parse_args()
    client = QDNatsClient(nats_url=options.nats_url)

    result = client.request(options.payload)
    qps_print(result.data.decode())


