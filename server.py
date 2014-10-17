import logging
from multiprocessing.connection import Listener
from threading import Thread




def results_server(address, authkey):
    server_c = Listener(address, authkey=authkey)
    while True:
        client_c = server_c.accept()
        t = Thread(target=handle_client, args=(client_c,))
        t.daemon = True
        t.start()


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

master_logger = setup_logging()



if __name__ == '__main__':
    authentication_key = 'sM45ubOwRfm2'
    port = 16000
    results_server(('', port), authentication_key)