import random
import socket
import time
from multiprocessing import Process


class ServerHelper(object):
    '''A helper object for managing a separate python process with a
    LiveStatus server running in it
    '''

    conn = None
    host = None
    port = None
    def __init__(self, cls):
        self.server = cls()

    def start(self):
        self.host, self.port = self.server.get_sock()
        self.proc = Process(target=self.server.run)
        self.proc.start()
        return self.host, self.port

    def stop(self):
        self.proc.terminate()
        self.proc.join(1)

    def get_last_recv(self):
        s = socket.create_connection((self.host, self.port), 5)
        s.send('GET-LAST-RECV')
        s.shutdown(socket.SHUT_WR)
        msg = s.recv(1024)
        s.close()
        return msg

    def get_last_send(self):
        s = socket.create_connection((self.host, self.port), 5)
        s.send('GET-LAST-SEND')
        s.shutdown(socket.SHUT_WR)
        msg = s.recv(1024)
        s.close()
        return msg


class MockLivestatusServer(object):
    '''A helper class for setting up a fake Livestatus endpoint,
    primarily for use with testing the LivestatusClient
    '''

    host = '127.0.0.1' # Only bind to localhost
    port = None
    socket = None
    def get_sock(self):
        '''Helper method that tries to pick a random port to bind to'''
        for attempt in xrange(10):
            rand_port = random.randint(2000, 9999)
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.bind((self.host, rand_port))
                self.socket = s
                self.socket.listen(5)
                self.port = rand_port
                break
            except Exception as e:
                print e
                continue
        if self.socket is None or self.port is None:
            raise RuntimeError('Could not create a socket')
        else:
            return self.host, self.port

    def run(self):
        last_recv = []
        last_send = []
        while True:
            client, address = self.socket.accept()
            data = client.recv(4096)
            if data is not None:
                if data == 'GET-LAST-RECV':
                    client.send(last_recv.pop())
                elif data == 'GET-LAST-SEND':
                    client.send(last_send.pop())
                else:
                    last_recv.append(data)
                    response = self.make_response(data)
                    client.send(response)
                    last_send.append(response)
            client.close()

    def make_response(self, data):
        '''Method for generating response data to be seint back to the
        client. Subclasses of MockLivestatusServer should override this
        method to adjust the behavior of the endpoint
        '''
        header = self.make_header(data)
        return header + data

    def make_header(self, response, status=200, length=None):
        '''Given data as a string, returns a header conforming to the
        Livestatus API spec:

        http://mathias-kettner.de/checkmk_livestatus.html#H1:Response Header

        Keyword args are made available in order to simulate a
        misbehaving endpoint.

        Args:
            response (str): the generated response data
        Kwargs:
            status (int): the status code to be returned
            length (int, None): if None, the length will be set to the
                actual length of the response string
        '''
        if length is None:
            length = len(response)
        header = '{status} {length: ^11}\n'.format(status=status,
                                                     length=length)
        return header


class WellBehavedServer(MockLivestatusServer):
    '''Returns a proper, well-formed response'''
    def make_response(self, data):
        if data.startswith('GET columns\n'):
            dummy_data = 'col1;string\ncol2;int\ncol3;time\ncol4;list\n'
        else:
            dummy_data = 'string1;1;1418675988;1,2,3\n' + \
                         'string2;2;1418675987;a,b,c\n'
        header = self.make_header(dummy_data)
        return header + dummy_data


class EmptyResponseServer(MockLivestatusServer):
    '''Returns a well-formed but empty response'''
    def make_response(self, data):
        dummy_data = ''
        header = self.make_header(dummy_data)
        return header + dummy_data


class NoDataServer(MockLivestatusServer):
    '''Returns a completely empty response (no headers)'''
    def make_response(self, data):
        return ''


class TimeoutServer(MockLivestatusServer):
    '''Accepts connections but never returns a response'''
    def make_response(self, data):
        while True:
            time.sleep(60)


class DeadServer(MockLivestatusServer):
    '''This server will shut the socket before you can try to query it'''
    def run(self):
        self.socket.close()

class RudeServer(MockLivestatusServer):
    '''This server will shut down the socket after sending some data'''
    def run(self):
        client, address = self.socket.accept()
        data = client.recv(4096)
        client.send('something!')
        self.socket.close()
