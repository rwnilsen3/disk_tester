import threading
import time
import logging
import Queue
import shelve


from multiprocessing.connection import Listener


def setup_logging(filename):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(filename)
    #fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    #ch.setLevel(logging.ERROR)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def client_listener(address, authkey, performance_results_queue):
    server_c = Listener(address, authkey=authkey)
    while True:
        client_c = server_c.accept()
        client_has_connected.set()
        t = threading.Thread(target=handle_client, args=(client_c, performance_results_queue))
        t.daemon = True
        t.start()


def start_client_listener(address, authkey, performance_results_queue):
    t = threading.Thread(target=client_listener, args=(address, authkey, performance_results_queue))
    t.daemon = True
    t.start()
    return t


def handle_client(client_connection, performance_results_queue):
    while True:
        try:
            msg = client_connection.recv()
            master_logger.debug('Client %s: Test run %s: Type %s: Message %s' % (msg['hostname'], msg['test_id'], msg['type'], msg['message']))
            if msg['type'] == 'chunk_sequential_write':
                performance_results_queue.put(msg)
        except EOFError:
            return



def clients_are_finished():
    return client_has_connected.isSet() and threading.active_count() <= 2


master_logger = setup_logging('master.log')

client_has_connected = threading.Event()

performance_results_queue = Queue.Queue()

if __name__ == '__main__':
    authentication_key = 'sM45ubOwRfm2'
    hostname = ''
    port = 16000
    listener_handle = start_client_listener((hostname, port), authentication_key, performance_results_queue)

    database_file = 'test_results.dat'
    test_results_db = shelve.open(database_file)

    while True:
        try:
            msg = performance_results_queue.get(block=True, timeout=5)
            if msg:
                # store to db
                test_id = msg['test_id']
                if test_results_db.has_key(test_id):
                    result_list = test_results_db[test_id]
                else:
                    result_list = []
                result_list.append(msg)
                test_results_db[test_id] = result_list

        except Queue.Empty:
            if clients_are_finished():
                break

    test_results_db.close()

    master_logger.debug('Main Thread Exiting')

