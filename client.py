from multiprocessing.connection import Client
from multiprocessing import Process, Queue
from Queue import Empty
import time
import psutil


def disk_tester(q, timeout):
    t = time.time() + timeout
    while t > time.time():
        x = 1
        for i in range(1000000):
            x = i * i
        q.put('calculated %s' % (x))
        time.sleep(0.5)




def heartbeat(q):
    while True:
        q.put(('heartbeat', 'beat'))
        time.sleep(5)


def utilization_monitor(q, pid_to_watch):
    p = psutil.Process(pid_to_watch)
    while True:
        cpu = p.cpu_percent(interval=1.0)
        mem = p.memory_info()
        q.put(('utilization', 'cpu: %s, memory: %s' % (cpu, mem[0])))
        time.sleep(10)

if __name__ == '__main__':

    c = Client(('localhost',16000), authkey='peekaboo')

    c.send('Hello')

    q = Queue()
    processes = []
    dt = Process(target=disk_tester, args=(q,15))
    dt.start()
    print dt.pid
    um = Process(target=utilization_monitor, args=(q,dt.pid))
    um.start()
    hb = Process(target=heartbeat, args=(q,))
    hb.start()
    processes.append(dt)
    processes.append(um)
    processes.append(hb)

    while True:
        try:
            msg = q.get(True, 1)
            if msg:
                c.send(msg)
        except Empty:
            time_to_die = False
            for proc in processes:
                if not proc.is_alive():
                    time_to_die = True
            if time_to_die:
                break


    for proc in processes:
        proc.terminate()