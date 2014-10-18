"""A server process that listens for disk tester clients to report in test results,
and records those results to a central database.
"""

import threading
import logging
import Queue
import shelve

from multiprocessing.connection import Listener


def setup_logging(filename):
    """Set up logging configuration
    :param filename: Name of the file to write log statements to
    :return: Logger -- handle to a Logger object for writing logs to
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(filename)
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def client_listener(address, authkey, performance_results_queue):
    """Listen to incoming multiprocessing connection requests, meant to run in a thread.
        It will spawn new threads for each incoming
    :param address: (hostname, port) to bind to
    :param authkey: Shared secret string to authenticate server to clients
    :param performance_results_queue: Queue to which results can be published
    :return: None
    """
    server_c = Listener(address, authkey=authkey)
    while True:
        client_c = server_c.accept()
        logger.info('Client Connected')
        client_has_connected.set()
        t = threading.Thread(target=handle_client, args=(client_c, performance_results_queue))
        t.daemon = True
        t.start()


def start_client_listener(address, authkey, performance_results_queue):
    """Start client listener threads
    :param address: (hostname, port) to bind to
    :param authkey: Shared secret string to authenticate server to clients
    :param performance_results_queue: Queue to which results can be published
    :return: Thread
    """
    t = threading.Thread(target=client_listener, args=(address, authkey, performance_results_queue))
    t.daemon = True
    t.start()
    return t


def handle_client(client_connection, performance_results_queue):
    """Handle messages coming from client processes, logging and forwarding to the database based on message type
    :param client_connection: Connection from client process
    :param performance_results_queue: Queue to which results can be published
    :return: None
    """
    while True:
        try:
            msg = client_connection.recv()
            logger.debug('Client %s: Test run %s: Type %s: Message %s' % (msg['hostname'], msg['test_id'], msg['type'], msg['message']))
            if msg['type'] == 'chunk_sequential_write':
                performance_results_queue.put(msg)
        except EOFError:
            return



def clients_are_finished():
    """Determine if all clients processing has been completed.  A hint we can shut down the server.
    :return: bool
    """
    return client_has_connected.isSet() and threading.active_count() <= 2


def print_report(test_id_list, results_database):
    """Print report of test results from all clients
    :param test_id_list: list of test run ids for this session
    :results_database: database to pull results from
    """
    for id in test_id_list:
        if id in results_database:
            test_stats = results_database[id]
            minWrite = min(float(s['message']) for s in test_stats)
            maxWrite = max(float(s['message']) for s in test_stats)
            avgWrite = sum(float(s['message']) for s in test_stats) / len(test_stats)
            logger.info("Hostname: %s Test ID: %s Write Throughput ( min %.2f MiB/s / avg %.2f MiB/s / max %.2f MiB/s )" %
                        (test_stats[0]['hostname'], id, minWrite, avgWrite, maxWrite))


"""
Main
"""
if __name__ == '__main__':
    logger = setup_logging('server.log')
    client_has_connected = threading.Event()
    performance_results_queue = Queue.Queue()

    authentication_key = 'sM45ubOwRfm2'
    hostname = ''
    port = 16000
    listener_handle = start_client_listener((hostname, port), authentication_key, performance_results_queue)

    test_id_list = []

    database_file = 'test_results.dat'
    test_results_db = shelve.open(database_file)

    try:
        logger.info('Listening for test clients to connect')

        while not clients_are_finished():
            try:
                msg = performance_results_queue.get(block=True, timeout=1)
                if msg:
                    # store to db
                    test_id = msg['test_id']
                    if test_id not in test_id_list:
                        test_id_list.append(test_id)
                    if test_id in test_results_db:
                        result_list = test_results_db[test_id]
                    else:
                        result_list = []
                    result_list.append(msg)
                    test_results_db[test_id] = result_list

            except Queue.Empty:
                pass

        print_report(test_id_list, test_results_db)

    finally:
        test_results_db.close()

    logger.info('All tests competed, exiting')

