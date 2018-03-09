from __future__ import absolute_import, unicode_literals

from . import virtual, base
from kombu.five import Empty

import proton
from proton.utils import BlockingConnection


#
# Required dependencies 
#$ apt-get install gcc cmake cmake-curses-gui uuid-dev

# dependencies needed for ssl support
#$ apt-get install libssl-dev

# dependencies needed for Cyrus SASL support
#$ apt-get install libsasl2-2 libsasl2-dev

# dependencies needed for bindings
#$ apt-get install swig python-dev ruby-dev libperl-dev

# dependencies needed for python docs
#$ apt-get install python-epydoc

# pip install python-qpid-proton==0.20.0



class Message(base.Message):

    def __init__(self, message, channel=None, **kwargs):
        super(Message, self).__init__(
            body=message.body['body'],
            channel=channel,
            delivery_tag=message.delivery_count,
            content_type=message.body['content-type'],
            delivery_info=message.body['properties']['delivery_info'],
            headers=message.body['headers'],
            properties=message.body['properties'],
            content_encoding=message.body['content-encoding'],
            **kwargs)


class Channel(virtual.Channel):
    _client = None
    Message = Message

    @property
    def client(self):
        if self._client is None:
            conninfo = self.connection.client
            self._client = BlockingConnection(conninfo.hostname,timeout=None)
        return self._client

    def _get(self, queue):
        self.receiver = self.client.create_receiver(queue)
        try:
            message = self.receiver.receive(timeout=0.01)
        except:
            self.receiver.close()
            raise Empty()
        else:
            self.receiver.accept()
            self.receiver.close()
        return message

    def _put(self, queue, message, **kwargs):
        self.sender = self.client.create_sender(queue)
        self.sender.send(proton.Message(body=message))
        self.sender.close()

    def basic_publish(self, message, exchange, routing_key, **kwargs):
        if exchange:
            return self.typeof(exchange).deliver(
                message, exchange, routing_key, **kwargs
            )
        return self._put(routing_key, message, **kwargs)


class Transport(virtual.Transport):

    Channel = Channel

    driver_type = 'pqpid'
    driver_name = 'pqpid'


