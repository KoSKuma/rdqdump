#!/usr/bin/python
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# Copyright (c) 2014 Mozilla Corporation
#
# Contributors:
# Jeff Bryner jbryner@mozilla.com/jeff@jeffbryner.com
# Lerit Nuksawn lerit.ns@gmail.com
#
# Parser for rabbitmq .rdq files that attempts
# to find record delimeters, record length and
# output json of the record
# Assumes your rabbitmq events are json.
# example run: ./rdqdump.py -f 383506.rdq -c0 | less
#
#

import os
import sys
import io
from argparse import ArgumentParser
import json


def read_chunk(data, start, end):
    data.seek(int(start))
    readdata = data.read(end)
    return readdata


# Hex dump code from:
# Author: Boris Mazic
# Date: 04.06.2012
# package rfid.libnfc.hexdump
def hexbytes(xs, group_size=1, byte_separator=' ', group_separator=' '):
    def ordc(c):
        return ord(c) if isinstance(c,str) else c

    if len(xs) <= group_size:
        s = byte_separator.join('%02X' % (ordc(x)) for x in xs)
    else:
        r = len(xs) % group_size
        s = group_separator.join(
            [byte_separator.join('%02X' % (ordc(x)) for x in group) for group in zip(*[iter(xs)] * group_size)]
        )
        if r > 0:
            s += group_separator + byte_separator.join(['%02X' % (ordc(x)) for x in xs[-r:]])
    return s.lower()


def hexprint(xs):
    def chrc(c):
        return c if isinstance(c, str) else chr(c)

    def ordc(c):
        return ord(c) if isinstance(c, str) else c

    def isprint(c):
        return ordc(c) in range(32,127) if isinstance(c, str) else c > 31

    return ''.join([chrc(x) if isprint(x) else '.' for x in xs])


def hexdump(xs, group_size=4, byte_separator=' ', group_separator='-', printable_separator='  ', address=0, address_format='%04X', line_size=16):
    if address is None:
        s = hexbytes(xs, group_size, byte_separator, group_separator)
        if printable_separator:
            s += printable_separator + hexprint(xs)
    else:
        r = len(xs) % line_size
        s = ''
        bytes_len = 0
        for offset in range(0, len(xs) - r, line_size):
            chunk = xs[offset:offset + line_size]
            bytes = hexbytes(chunk, group_size, byte_separator, group_separator)
            s += (address_format + ': %s%s\n') % (address + offset, bytes, printable_separator + hexprint(chunk) if printable_separator else '')
            bytes_len = len(bytes)

        if r > 0:
            offset = len(xs) - r
            chunk = xs[offset:offset + r]
            bytes = hexbytes(chunk, group_size, byte_separator, group_separator)
            bytes = bytes + ' ' * (bytes_len - len(bytes))
            s += (address_format + ': %s%s\n') % (address + offset, bytes, printable_separator + hexprint(chunk) if printable_separator else '')
    return s


def convert_hex(string):
    return ''.join([hex(character)[2:].upper().zfill(2) \
                    for character in string])


def save_data(output_file, queue_name, queue_message, file_location='output'):
    try:
        queue_message = json.loads(queue_message)
    except:
        pass

    line = {
        'queue_name': queue_name,
        'queue_message': queue_message
    }

    file_location = file_location.rstrip('/')
    with open(f'{file_location}/{output_file}', 'a') as out_file:
        out_file.write(json.dumps(line) + '\n')


def extract_unescaped_content(src, data, hex_bytes, chunk_size, data_length_byte_size, zero=False):
    # where is the string in this chunk of data
    hexdata = convert_hex(data)
    data_pos = hexdata.find(hex_bytes.upper()) / 2
    # where is the string in the file
    data_address = (max(0, (src.tell() - chunk_size)) + data_pos)
    # what do we print in the hexoutput
    print_address = data_address
    if zero:
        # used to carve out a portion of a stream and save it via xxd -r
        print_address = 0

    # Find the length of the data
    record_size = read_chunk(src, data_address + len(hex_bytes) / 2, data_length_byte_size)
    rdq_entry_length = int(convert_hex(record_size), 16)
    data = read_chunk(src, data_address + (len(hex_bytes) / 2) + data_length_byte_size, rdq_entry_length)
    unescaped_data = data.decode('ascii', 'ignore')

    if options.debug:
        print(hexdump(data, byte_separator='', group_size=2, group_separator=' ', printable_separator='  ', address=print_address, line_size=16, address_format='%f'))

    return (data, unescaped_data)

if __name__ == '__main__':
    # search a rabbit mq rdq file for
    # potential amqp records:
    # by finding: 395f316c000000016d0000 , parsing the next 2 bytes as
    # record length and outputing the record to stdout
    #
    parser = ArgumentParser(description="Process RabbitMQ persistent file storage (.rdq) and output to file or another RabbitMQ")
    parser.add_argument("-b", dest='bytes', default=16, type=int, help="number of bytes to show per line")
    parser.add_argument("-s", dest='start', default=0, type=int, help="starting byte")
    parser.add_argument("-l", dest='length', default=16, type=int, help="length in bytes to dump")
    parser.add_argument("-r", dest='chunk', default=1024, type=int, help="length in bytes to read at a time")
    parser.add_argument("-f", dest='input', default='', help="input: filename")
    parser.add_argument("-t", dest='text', default='', help="text string to search for")

    # output related arguments
    parser.add_argument("-o", dest='output', help="output: filename")
    parser.add_argument("-q", dest='queue', help="config containing output queue credential")

    # this hex value worked for me, might work for you
    # to delimit the entries in a rabbitmq .rdq file
    parser.add_argument("-x", "--msg-hex", dest='hex', default="395f316c000000016d0000", help="hex string to search for queue message")
    parser.add_argument("--hex_queue", dest='hex_queue', default="65786368616E67656D000000006C000000016D000000,000865786368616E67656D000000", help="hex string to search for queue name")
    parser.add_argument("-c", dest='count', default=1, type=int, help="count of hits to find before stopping (0 for don't stop)")

    # debug related arguments
    parser.add_argument("--dev", action='store_true', dest='dev_debug', default=False, help="Print debug message related to development process")
    parser.add_argument("-d", "--debug", action='store_true', dest='debug', default=False, help="turn on debugging output")
    parser.add_argument("-z", "--zero", action='store_true', dest='zero', default=False, help="when printing output, count from zero rather than position hit was found")

    options = parser.parse_args()
    dev_debug = options.dev_debug

    if os.path.exists(options.input):
        src = open(options.input, 'rb')
        # if the file is smaller than our chunksize, reset.
        chunk_size = min(os.path.getsize(options.input), options.chunk)
    else:
        sys.stderr.write(options.input)
        sys.stderr.write("No input file specified\n")
        sys.exit()

    if options.output is not None:
        output_file = options.output
        output = True
    else:
        output = False

    if options.queue is not None:
        queue_config_file = options.queue
        queue = True
    else:
        queue = False

    data = read_chunk(src, options.start, chunk_size)
    if options.debug:
        print("[*] position: %d" % (src.tell()))
    count = 0

    queue_name = None
    queue_message = None

    queue_name_indicator_hex = options.hex_queue.split(',')
    queue_message_indicator_hex = options.hex

    while data:
        if dev_debug:
            print("")
            print(data)
            print(convert_hex(data))
            print("")
        
        for hex_bytes in queue_name_indicator_hex:
            if len(hex_bytes) > 0 and hex_bytes.upper() in convert_hex(data):
                if queue_name is not None:
                    raise Exception(f"ERROR [1], QUEUE MESSAGE NOT FOUND FOR QUEUE NAME ({hex_bytes})")
                (data, queue_name) = extract_unescaped_content(
                    src=src,
                    data=data,
                    hex_bytes=hex_bytes,
                    chunk_size=chunk_size,
                    data_length_byte_size=1,
                    zero=options.zero
                )

        if len(queue_message_indicator_hex) > 0 and queue_message_indicator_hex.upper() in convert_hex(data):
            if queue_message is not None:
                raise Exception("ERROR [2], QUEUE NAME NOT FOUND FOR QUEUE MESSAGE")
            (data, queue_message) = extract_unescaped_content(
                src=src,
                data=data,
                hex_bytes=queue_message_indicator_hex,
                chunk_size=chunk_size,
                data_length_byte_size=2,
                zero=options.zero
            )
            count += 1

        if queue_message is not None:
            if queue_name is None:
                raise Exception("ERROR [3], QUEUE NAME NOT FOUND FOR QUEUE MESSAGE")
            
            if (not queue) and (not output):
                print(f"queue_name = {queue_name}")
                print(f"queue_message = {queue_message}")
            else:
                if queue:
                    pass
                if output:
                    save_data(output_file, queue_name, queue_message)
            queue_name = None
            queue_message = None

        if options.count != 0 and options.count <= count:
            sys.exit()
        else:
            data = read_chunk(src,src.tell(), chunk_size)
            if options.debug:
                print("[*] position: %d" % (src.tell()))
