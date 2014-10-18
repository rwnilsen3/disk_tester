"""A subprocess function that will emit heartbeat messages every 5 seconds
"""

import time


def heartbeat(msgs, logger):
    """Send heartbeat messages every 5 seconds
    :param msgs: Connection from parent process
    :param logger: Logger to write logs to
    :return: None
    """
    while True:
        logger.debug('beating')

        # Check for message from parent to shutdown
        if msgs.poll() is True:
            msg = msgs.recv()
            if msg and msg['type'] == 'stop':
                break

        msgs.send(dict(type='event', message='heartbeat'))
        time.sleep(5)

