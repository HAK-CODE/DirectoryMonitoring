import socket
import time

class TCPSERVER:
    TCP_IP = ''
    TCP_PORT = 0
    BUFFER_SIZE = 0  # Normally 1024, but we want fast response
    conn = None
    addr = None

    def __init__(self, TCP_IP, TCP_PORT, BUFFER_SIZE):
        self.TCP_IP      = TCP_IP
        self.TCP_PORT    = TCP_PORT
        self.BUFFER_SIZE = BUFFER_SIZE
        self.connect()

    def connect(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self.TCP_IP, self.TCP_PORT))
        s.listen(1)
        self.conn, self.addr = s.accept()
        print('Connection address:', self.addr)

    def sendData(self, datagram):
        data = datagram.encode()
        if not data:
            self.conn.close()
        #print("received data:", data)
        self.conn.send(data)