import time

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
            if msg and msg['type'] == 'stop':
                break

        try:
            cpu = p.cpu_percent(interval=1.0)
            mem = p.memory_info()
            msgs.send(dict(type='utilization', message='cpu: %s, memory: %s' % (cpu, mem[0])))
        except:
            pass

        time.sleep(10)

