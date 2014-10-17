import argparse
import time
import multiprocessing
from multiprocessing.connection import Client
import logging

try:
    import psutil
except ImportError:
    print "This python module requires the 'psutil' module be installed"
    sys.exit(1)

def utilization_monitor(msgs, logger, pid_to_watch):
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
