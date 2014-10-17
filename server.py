import logging
import threading
import time

from multiprocessing.connection import Listener



def client_listener(address, authkey):
    server_c = Listener(address, authkey=authkey)
    while True:
        client_c = server_c.accept()
        client_has_connected.set()
        t = threading.Thread(target=handle_client, args=(client_c,))
        t.daemon = True
        t.start()

def start_client_listener(address, authkey):
    t = threading.Thread(target=client_listener, args=(address, authkey))
    t.daemon = True
    t.start()
    return t

def handle_client(c):
    while True:
        try:
            msg = c.recv()
            master_logger.debug(msg)
        except EOFError:
            return

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('master.log')
    #fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    #ch.setLevel(logging.ERROR)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def clients_are_not_finished():
    return not client_has_connected.isSet() or threading.active_count() > 2


master_logger = setup_logging()

client_has_connected = threading.Event()

if __name__ == '__main__':
    authentication_key = 'sM45ubOwRfm2'
    hostname = ''
    port = 16000
    #client_listener(('', port), authentication_key)
    listener_handle = start_client_listener((hostname, port), authentication_key)

    while clients_are_not_finished():
        master_logger.debug('threads: %s' % threading.active_count())
        time.sleep(5)

    master_logger.debug('Main Thread Exiting')

