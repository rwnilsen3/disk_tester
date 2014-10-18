import time


def heartbeat(msgs, logger):
    while True:
        logger.debug('beating')
        if msgs.poll() is True:
            msg = msgs.recv()
            if msg and msg['type'] == 'stop':
                break

        msgs.send(dict(type='event', message='heartbeat'))
        time.sleep(5)

