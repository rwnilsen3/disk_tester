"""A subprocess function that will monitor the utilization of the disk tester process
"""

import time

try:
    import psutil
except ImportError:
    print "This python module requires the 'psutil' module be installed"
    sys.exit(1)



def utilization_monitor(msgs, logger, pid_to_watch):
    """Monitor utilization of disk tester process
    :param msgs: Connection from parent process
    :param logger: Logger to write to
    :param pid_to_watch: process id of disk tester process
    :return: None
    """
    p = psutil.Process(pid_to_watch)
    while True:
        logger.debug('monitoring')

        # Check for message from parent to shutdown
        if msgs.poll() is True:
            msg = msgs.recv()
            if msg and msg['type'] == 'stop':
                break

        try:
            cpu = p.cpu_percent(interval=1.0)
            mem = p.memory_info()
            msgs.send(dict(type='utilization', message='cpu: %s, memory: %s' % (cpu, mem[0])))
        except:
            pass

        time.sleep(10)

