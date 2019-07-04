import os
from multiprocessing import Process
import time
import threading
import socket
import signal
import logging

logger = logging.getLogger()

class Tunnel(object):
    bind_address = '127.0.0.1'
    remote_socket = None
    local_socket = None
    server_socket = None

    def __init__(self, src_port, dst_port, keep_alive=False):
        self.src_port = src_port
        self.dst_port = dst_port
        self.keep_alive = keep_alive

        signal.signal(signal.SIGTERM, self.signal_handler)

        self.server()

    def signal_handler(self, signum, frame):
        logger.info('Terminating tunnel from %d to %d' % (self.src_port, self.dst_port))

        if self.remote_socket:
            self.remote_socket.close()

        if self.local_socket:
            self.local_socket.close()

        if self.server_socket:
            self.server_socket.close()

        os._exit(0)

    def server(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.server_socket.bind((self.bind_address, self.src_port))
        self.server_socket.listen(10)

        while True:
            self.local_socket, local_address = self.server_socket.accept()
            self.remote_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.remote_socket.connect((self.bind_address, self.dst_port))

            s = threading.Thread(target=self.handle, args=(self.remote_socket, self.local_socket))
            r = threading.Thread(target=self.handle, args=(self.local_socket, self.remote_socket))

            s.start()
            r.start()
        
    def handle(self, src, dst):
        try:
            while True:
                buffer = src.recv(1024)
                if len(buffer) == 0:
                    break

                dst.send(buffer)
        except:
            pass

        try:
            src.shutdown(socket.SHUT_RDWR)
            src.close()
            dst.shutdown(socket.SHUT_RDWR)
            dst.close()

            if self.keep_alive == False:
                self.signal_handler(None, None)
        except:
            pass

class TunnelProcess(object):
    proc = None

    def __init__(self, src_port, dst_port, timeout=None, keep_alive=False):
        self.proc = Process(target=Tunnel, args=(src_port, dst_port, keep_alive))
        self.proc.daemon = True
        self.proc.start()

        if timeout != None:
            t = threading.Thread(target=self.wait_and_kill, args=(self.proc, timeout))
            t.start()

    def wait_and_kill(self, proc, timeout):
        time.sleep(timeout)
        proc.terminate()
        proc.join(1)
