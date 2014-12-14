import random
import socket


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
                self.socket.listen(1)
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
        while True:
            client, address = self.socket.accept()
            data = client.recv(2048)
            if data is not None:
                response = self.make_response(data)
                client.send(response)
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
    def make_resonse(data):
        dummy_data = 'col1;col2;col3;\n1;2;3\n'
        header = self.make_header(dummy_data)
        return header + dummy_data
