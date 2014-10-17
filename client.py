import argparse
import time
import sys
import multiprocessing
from multiprocessing.connection import Client
import logging


try:
    import psutil
except ImportError:
    print "This python module requires the 'psutil' module be installed"
    sys.exit(1)



logger = multiprocessing.log_to_stderr()
logger.setLevel(logging.DEBUG)


#Todo: Define all messages into a more strict protocol listing here


def disk_tester(msgs, arguments):
    t = time.time() + arguments.test_duration
    while True:
        logger.debug('testing')
        if msgs.poll() is True:
            msg = msgs.recv()
            if msg and msg[0] == 'stop':
                break

        if t < time.time():
            break

        x = 1
        for i in range(1000000):
            x = i * i
        msgs.send(('results', 'calculated %s' % (x)))
        time.sleep(0.75)

    msgs.send(('test_completed', 'yay'))
    msg = msgs.recv()



def heartbeat(msgs):
    while True:
        logger.debug('beating')
        if msgs.poll() is True:
            msg = msgs.recv()
            if msg and msg[0] == 'stop':
                logging.debug('I need to stop now')
                break

        msgs.send(('heartbeat', 'beat'))
        time.sleep(5)


def utilization_monitor(msgs, pid_to_watch):
    p = psutil.Process(pid_to_watch)
    while True:
        logger.debug('monitoring')
        if msgs.poll() is True:
            msg = msgs.recv()
            if msg and msg[0] == 'stop':
                break

        cpu = p.cpu_percent(interval=1.0)
        mem = p.memory_info()
        msgs.send(('utilization', 'cpu: %s, memory: %s' % (cpu, mem[0])))
        time.sleep(10)



def process_command_line_arguments():
    parser = argparse.ArgumentParser(description='A filesystem and disk write performance measurement utility')
    parser.add_argument('working-directory',
                        help='Location on filesystem where file write performance should be tested')
    parser.add_argument('--test-duration', type=int, default=120,
                        help='Length of time in seconds for the test to run')
    parser.add_argument('--write-chunk-size', type=int, default=10,
                        help='Size of chunks to be written to the file, in MiB')
    parser.add_argument('--max-file-size', type=int, default=100,
                        help='Maximum size in of test output files, in MiB')
    return parser.parse_args()




if __name__ == '__main__':

    test_config_arguments = process_command_line_arguments()

    c = Client(('localhost',16000), authkey='sM45ubOwRfm2')

    c.send('Hello')

    tester_connection, tester_child_side = multiprocessing.Pipe()
    dt = multiprocessing.Process(target=disk_tester, args=(tester_child_side, test_config_arguments))
    dt.start()

    monitor_connection, monitor_child_side = multiprocessing.Pipe()
    um = multiprocessing.Process(target=utilization_monitor, args=(monitor_child_side, dt.pid))
    um.start()

    heartbeat_connection, heartbeat_child_side = multiprocessing.Pipe()
    hb = multiprocessing.Process(target=heartbeat, args=(heartbeat_child_side,))
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

    multiprocessing.join(dt)
    multiprocessing.join(um)
    multiprocessing.join(hb)