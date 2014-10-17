import argparse
import logging
import multiprocessing
from multiprocessing.connection import Client



from subprocesses.disktester import disk_tester
from subprocesses.heartbeat import heartbeat
from subprocesses.utilization_monitor import utilization_monitor
from utilities.random_string_generator import id_generator


logger = multiprocessing.log_to_stderr()
logger.setLevel(logging.DEBUG)


#Todo: Define all messages into a more strict protocol listing here


def process_command_line_arguments():
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
        print 'File chunk size can not be larger than file size'
        exit(1)

    return args





if __name__ == '__main__':

    test_config_arguments = process_command_line_arguments()



    c = Client((test_config_arguments.results_server,16000), authkey='sM45ubOwRfm2')

    client_id = id_generator()
    c.send('Client %s starting' % (client_id))

    tester_connection, tester_child_side = multiprocessing.Pipe()
    dt = multiprocessing.Process(target=disk_tester, args=(tester_child_side, logger, test_config_arguments))
    dt.start()

    monitor_connection, monitor_child_side = multiprocessing.Pipe()
    um = multiprocessing.Process(target=utilization_monitor, args=(monitor_child_side, logger, dt.pid))
    um.start()

    heartbeat_connection, heartbeat_child_side = multiprocessing.Pipe()
    hb = multiprocessing.Process(target=heartbeat, args=(heartbeat_child_side, logger))
    hb.start()

    while True:
        if tester_connection.poll(1):
            msg = tester_connection.recv()
            print msg
            if msg:
                c.send(msg)
                if msg[0] == 'test_completed':
                    tester_connection.send(('stop',))
                    monitor_connection.send(('stop',))
                    heartbeat_connection.send(('stop',))
                    break
        else:
            if monitor_connection.poll():
                msg = monitor_connection.recv()
                if msg:
                    c.send(msg)
            else:
                if heartbeat_connection.poll():
                    msg = heartbeat_connection.recv()
                    if msg:
                        c.send(msg)


    dt.join()
    um.join()
    hb.join()

    c.send('Client %s is done' % (client_id))


