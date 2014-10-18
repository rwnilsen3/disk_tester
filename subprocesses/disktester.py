"""A subprocess that will test sequential write speed of a disk
"""

import time
import os
import mmap
from utilities.random_string_generator import id_generator



def open_file_with_random_name(files_created_during_test):
    """ Opens a file for writing, using a random name to avoid overwriting an existing file.
    :param files_created_during_test:
    :return: file handle
    """
    # Open in O_DIRECT and O_SYNC modes to try to more accurately assess the
    # performance of the disk rather than OS buffering
    filename = id_generator(8)
    f = os.open(filename, os.O_CREAT | os.O_DIRECT | os.O_SYNC | os.O_WRONLY)
    files_created_during_test.append(filename)
    return f



def disk_tester(msgs, logger, arguments):
    """Write random bytes to a file in specified chunks, up to a specified file size, then rotating into another file
       until sufficient bytes have been written or time has past.
    :param msgs: Connection from parent processs
    :param logger: Logger to use
    :param arguments: Collection of test parameters, collected from command line arguments
    :return: None
    """

    one_mebibyte = 2 ** 20

    # Create a 1 MiB 512k-aligned memory buffer with random bytes to be used to write to storage
    # (alignment is requirement for doing O_DIRECT)
    m = mmap.mmap(-1, one_mebibyte)
    m.write(os.urandom(one_mebibyte))

    file_rollovers = 0
    chunk_size = arguments.write_chunk_size * one_mebibyte
    max_file_size = arguments.max_file_size * one_mebibyte

    files_created_during_test = []
    f = open_file_with_random_name(files_created_during_test)
    bytes_remaining_in_file = max_file_size

    t = time.time() + arguments.test_duration

    try:
        # Write chunks of bytes to files until test is complete
        while True:
            logger.debug('testing')

            # Check for message from parent to shutdown
            if msgs.poll() is True:
                msg = msgs.recv()
                if msg and msg['type'] == 'stop':
                    break

            if time.time() > t and file_rollovers > 2:
                break

            size_of_this_chunk = min(chunk_size, bytes_remaining_in_file)

            bytes_remaining_in_chunk = size_of_this_chunk
            start_time = time.time()

            while bytes_remaining_in_chunk:
                num = os.write(f, m)
                bytes_remaining_in_chunk -= num
                bytes_remaining_in_file -= num

            elapsed_time = time.time() - start_time
            write_speed = ((size_of_this_chunk / one_mebibyte) / elapsed_time)
            msgs.send(dict(type='chunk_sequential_write', message=str(write_speed), timestamp=time.time()))

            # Rotate into a new output file if the max size has been reached
            if bytes_remaining_in_file <= 0:
                os.close(f)
                f = open_file_with_random_name(files_created_during_test)
                file_rollovers += 1
                bytes_remaining_in_file = max_file_size
                msgs.send(dict(type='event', message='output file rollover'))
    finally:
        os.close(f)

    for testfile in files_created_during_test:
        os.remove(testfile)

    msgs.send(dict(type='event', message='test completed'))

    # wait to get stop message back from parent
    msg = msgs.recv()