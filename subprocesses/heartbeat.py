import multiprocessing
import logging
import time


def heartbeat(msgs, logger):
    while True:
        logger.debug('beating')
        if msgs.poll() is True:
            msg = msgs.recv()
            if msg and msg[0] == 'stop':
                break

        msgs.send(('heartbeat', 'beat'))
        time.sleep(5)
