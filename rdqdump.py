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
from optparse import OptionParser
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


# def save_data():
#     try:
#         if output:
#             with open(output_file, "a") as out_file:
#                 out_file.write(json.loads(unescaped_data) + '\n')
#         else:
#             # sys.stdout.write(json.loads(unescaped_data) + '\n')
#             pass

#     except Exception as e:
#         if output:
#             with open(output_file, "a") as out_file:
#                 out_file.write(unescaped_data + '\n')
#         else:
#             sys.stderr.write(unescaped_data + '\n')
#         pass


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
    parser = OptionParser()
    parser.add_option("-b", dest='bytes', default=16, type="int", help="number of bytes to show per line")
    parser.add_option("-s", dest='start', default=0, type="int", help="starting byte")
    parser.add_option("-l", dest='length', default=16, type="int", help="length in bytes to dump")
    parser.add_option("-r", dest='chunk', default=1024, type="int", help="length in bytes to read at a time")
    parser.add_option("-f", dest='input', default="", help="input: filename")
    parser.add_option("-t", dest='text', default="", help="text string to search for")
    parser.add_option("-o", dest='output', help="output: filename")

    # this hex value worked for me, might work for you
    # to delimit the entries in a rabbitmq .rdq file
    parser.add_option("-x", dest='hex', default="395f316c000000016d0000", help="hex string to search for queue message")
    parser.add_option("-q", "--hex_queue", dest='hex_queue', default="000000006C000000016D000000", help="hex string to search for queue name")
    parser.add_option("-c", dest='count', default=1, type="int", help="count of hits to find before stopping (0 for don't stop)")
    parser.add_option("-d", "--debug", action="store_true", dest="debug", default=False, help="turn on debugging output")
    parser.add_option("-z", "--zero", action="store_true", dest="zero", default=False, help="when printing output, count from zero rather than position hit was found")

    (options, args) = parser.parse_args()

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

    data = read_chunk(src, options.start, chunk_size)
    if options.debug:
        print("[*] position: %d" % (src.tell()))
    count = 0

    queue_name = None
    queue_message = None

    queue_name_indicator_hex = options.hex_queue
    queue_message_indicator_hex = options.hex

    while data:
        # print("")
        # print("")
        # print(data)
        # print(convert_hex(data))
        
        if len(queue_name_indicator_hex) > 0 and queue_name_indicator_hex.upper() in convert_hex(data):
            (data, queue_name) = extract_unescaped_content(
                src=src,
                data=data,
                hex_bytes=queue_name_indicator_hex,
                chunk_size=chunk_size,
                data_length_byte_size=1,
                zero=options.zero
            )

        if len(queue_message_indicator_hex) > 0 and queue_message_indicator_hex.upper() in convert_hex(data):
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
                raise Exception("ERROR, QUEUE NAME NOT FOUND FOR QUEUE MESSAGE")
            print("")
            print("==============================================================================")
            print("")
            print(queue_name, "||", queue_message)
            queue_name = None
            queue_message = None

        if options.count != 0 and options.count <= count:
            sys.exit()
        else:
            data = read_chunk(src,src.tell(), chunk_size)
            if options.debug:
                print("[*] position: %d"%(src.tell()))
