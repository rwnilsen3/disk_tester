from multiprocessing.connection import Listener
from threading import Thread

def handle_client(c):
    while True:
        try:
            msg = c.recv()
        except EOFError:
            return
        print msg


def echo_server(address, authkey):
    server_c = Listener(address, authkey=authkey)
    while True:
        client_c = server_c.accept()
        t = Thread(target=handle_client, args=(client_c,))
        t.daemon = True
        t.start()


if __name__ == '__main__':
    echo_server(('',16000), "peekaboo")