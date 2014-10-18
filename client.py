"""A client process that tests disk performance and reports results to a server process/database
"""

import argparse
import logging
import multiprocessing
import time
import platform
from multiprocessing.connection import Client

from subprocesses.disktester import disk_tester
from subprocesses.heartbeat import heartbeat
from subprocesses.utilization_monitor import utilization_monitor
from utilities.random_string_generator import id_generator



def process_command_line_arguments():
    """Setup argparse to handle command line options
    :return: arguments from command line
    """
    parser = argparse.ArgumentParser(description='A filesystem and disk write performance measurement utility')
    parser.add_argument('working-directory',
                        help='Location on filesystem where file write performance should be tested')
    parser.add_argument('--results-server', default='localhost',
                        help='Hostname of the server to which the client should send status and results')
    parser.add_argument('--test-duration', type=int, default=20,
                        help='Length of time in seconds for the test to run')
    parser.add_argument('--write-chunk-size', type=int, default=10,
                        help='Size of chunks to be written to the file, in MiB')
    parser.add_argument('--max-file-size', type=int, default=100,
                        help='Maximum size in of test output files, in MiB')
    args = parser.parse_args()

    if args.write_chunk_size > args.max_file_size:
        logger.error('File chunk size can not be larger than file size')
        exit(1)

    return args


def send_master_message(message_dict):
    """Send message to server, adding in extra details about hostname and test run id
    :param message_dict: Contents of message
    :return: None
    """
    message_dict['test_id'] = test_id
    message_dict['hostname'] = platform.node()
    c.send(message_dict)


def setup_logging(filename):
    """Configure Logger
    :param filename: File to write log statements to
    :return: Logger
    """

    logger = multiprocessing.get_logger()
    DEFAULT_LOGGING_FORMAT = '[%(levelname)s/%(processName)s] %(message)s'
    formatter = logging.Formatter(DEFAULT_LOGGING_FORMAT)
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(filename)
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.WARNING)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


"""
Main
"""
if __name__ == '__main__':
    test_id = id_generator()
    logger = setup_logging('client_' + test_id + '.log')
    LOG_ALWAYS = 60
    logger.log(LOG_ALWAYS, 'Starting up')

    test_config_arguments = process_command_line_arguments()

    # connect to test results server
    c = Client((test_config_arguments.results_server,16000), authkey='sM45ubOwRfm2')

    logger.log(LOG_ALWAYS, 'Connected to server')

    send_master_message(dict(type='event', message='starting'))

    # create thread to do disk testing
    tester_connection, tester_child_side = multiprocessing.Pipe()
    dt = multiprocessing.Process(target=disk_tester, args=(tester_child_side, logger, test_config_arguments))
    dt.start()

    # create thread to monitor utilization of disk testing thread
    monitor_connection, monitor_child_side = multiprocessing.Pipe()
    um = multiprocessing.Process(target=utilization_monitor, args=(monitor_child_side, logger, dt.pid))
    um.start()

    # create heartbeat sender thread
    heartbeat_connection, heartbeat_child_side = multiprocessing.Pipe()
    hb = multiprocessing.Process(target=heartbeat, args=(heartbeat_child_side, logger))
    hb.start()

    while True:

        # Receive messages from disk tester thread, routing as appropriate
        if tester_connection.poll():
            msg = tester_connection.recv()
            logger.debug(msg)
            if msg:
                send_master_message(msg)
                if msg['type'] == 'event' and msg['message'] == 'test completed':
                    tester_connection.send(dict(type='stop', message=''))
                    monitor_connection.send(dict(type='stop', message=''))
                    heartbeat_connection.send(dict(type='stop', message=''))
                    break

        # Route messages from monitor to server
        if monitor_connection.poll():
            msg = monitor_connection.recv()
            if msg:
                send_master_message(msg)

        # Route heartbeats to server
        if heartbeat_connection.poll():
            msg = heartbeat_connection.recv()
            if msg:
                send_master_message(msg)

        time.sleep(0.25)

    dt.join()
    um.join()
    hb.join()

    send_master_message(dict(type='event', message='exiting'))

    logger.log(LOG_ALWAYS, 'All done, exiting')


