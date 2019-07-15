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
import yaml
from helpers.rabbitmq import RabbitMQ
from helpers.hex import hexdump, convert_hex


def read_chunk(data, start, end):
    data.seek(int(start))
    readdata = data.read(end)
    return readdata


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


def restore_rdq_data(
        file_path,
        queue_name_indicator_hex,
        queue_message_indicator_hex,
        chunk,
        message_limit,
        output_file,
        queue,
        start_bytes=0,
        delete_file=False,
        debug=False,
        dev_debug=False,
        zero=False
        ):
    print(f"Start processing '{file_path}'")

    src, chunk_size = read_file(file_path, chunk)

    if debug:
        print("[*] position: %d" % (src.tell()))

    count = 0
    queue_name = None
    queue_message = None
    data = read_chunk(src, start_bytes, chunk_size)

    while data:
        if dev_debug:
            print("Data:")
            print(data)
            print("")
            print("Data (HEX):")
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
                    zero=zero
                )

        for hex_bytes in queue_message_indicator_hex:
            if len(hex_bytes) > 0 and hex_bytes.upper() in convert_hex(data):
                if queue_message is not None:
                    raise Exception("ERROR [2], QUEUE NAME NOT FOUND FOR QUEUE MESSAGE")
                (data, queue_message) = extract_unescaped_content(
                    src=src,
                    data=data,
                    hex_bytes=hex_bytes,
                    chunk_size=chunk_size,
                    data_length_byte_size=2,
                    zero=zero
                )
                count += 1

                if count % 1000 == 0:
                    print(f"- Processed {count} messages.")

        if queue_message is not None:
            if queue_name is None:
                raise Exception("ERROR [3], QUEUE NAME NOT FOUND FOR QUEUE MESSAGE")
            
            if (not queue) and (not output_file):
                print(f"queue_name:\n{queue_name}\n")
                print(f"queue_message:\n{queue_message}")
                print("\n\n=====================================================================================\n\n")
            else:
                if queue:
                    queue.create_queue(queue_name)
                    queue.publish_to_queue(queue_name, queue_message)
                if output_file:
                    save_data(output_file, queue_name, queue_message)
            queue_name = None
            queue_message = None

        if message_limit != 0 and message_limit <= count:
            sys.exit()
        else:
            data = read_chunk(src,src.tell(), chunk_size)
            if debug:
                print("[*] position: %d" % (src.tell()))

    print(f"- Processed {count} messages.")
    print(f"Finish processing {count} messages from {file_path}.\n")

    if delete_file:
        print(f"...Delete file '{file_path}'...\n")
        os.remove(file_path)


def read_file(file_path, chunk):
    if os.path.exists(file_path):
        src = open(file_path, 'rb')
        # if the file is smaller than our chunksize, reset.
        chunk_size = min(os.path.getsize(file_path), chunk)
    else:
        sys.stderr.write(f"Cannot find the specified file '{file_path}'\n")
        sys.exit()
    return src, chunk_size


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
    parser.add_argument("-i", dest='input', default=None, help="input: filename")
    parser.add_argument("-f", "--folder", dest='folder', default=None, help="input: folder path")
    parser.add_argument("-t", dest='text', default='', help="text string to search for")

    # output related arguments
    parser.add_argument("-o", dest='output', help="output: filename")
    parser.add_argument("-q", dest='queue', help="config containing output queue credential")
    parser.add_argument("--delete", action='store_true', dest='delete', default=False, help="input: folder path")

    # this hex value worked for me, might work for you
    # to delimit the entries in a rabbitmq .rdq file
    parser.add_argument("-x", "--msg-hex", dest='hex', default="395f316c000000016d0000,395f316c000000016d0001,00046E6F6E656400046E6F6E656C000000016D0000", help="hex string to search for queue message")
    parser.add_argument("--hex_queue", dest='hex_queue', default="65786368616E67656D000000006C000000016D000000,00046E6F6E656400046E6F6E656C000000016D0001,000865786368616E67656D000000", help="hex string to search for queue name")
    parser.add_argument("-c", dest='count', default=1, type=int, help="count of hits to find before stopping (0 for don't stop)")

    # debug related arguments
    parser.add_argument("--dev", action='store_true', dest='dev_debug', default=False, help="Print debug message related to development process")
    parser.add_argument("-d", "--debug", action='store_true', dest='debug', default=False, help="turn on debugging output")
    parser.add_argument("-z", "--zero", action='store_true', dest='zero', default=False, help="when printing output, count from zero rather than position hit was found")

    options = parser.parse_args()

    if options.input is None and options.folder is None:
        sys.stderr.write("No input file (-i) or folder (-f) specified\n")
        sys.exit()

    if options.input is not None and options.folder is not None:
        sys.stderr.write("Please specify only either input file (-i) or folder (-f)\n")
        sys.exit()

    output_file = None
    if options.output is not None:
        output_file = options.output

    queue = None
    if options.queue is not None:
        with open(f'config/{options.queue}') as stream:
            queue_config = yaml.safe_load(stream)['queue']
        queue = RabbitMQ(queue_config['user'], queue_config['password'], queue_config['host'], queue_config['port'])
        queue.connect_to_queue()

    queue_name_indicator_hex = options.hex_queue.split(',')
    queue_message_indicator_hex = options.hex.split(',')

    if options.input:
        file_path = options.input
        restore_rdq_data(
            file_path=file_path,
            queue_name_indicator_hex=queue_name_indicator_hex,
            queue_message_indicator_hex=queue_message_indicator_hex,
            chunk=options.chunk,
            message_limit=options.count,
            output_file=output_file,
            queue=queue,
            start_bytes=options.start,
            delete_file=options.delete,
            debug=options.debug,
            dev_debug=options.dev_debug
        )
    elif options.folder:
        folder_path = options.folder.rstrip('/')
        if os.path.exists(folder_path):
            rdq_list = list(filter(lambda file_name: file_name.endswith('.rdq'), os.listdir(folder_path)))
            total_file_count = len(rdq_list)
            processed_file_count = 0
            for rdq_file in rdq_list:
                full_path = f"{folder_path}/{rdq_file}"
                restore_rdq_data(
                    file_path=full_path,
                    queue_name_indicator_hex=queue_name_indicator_hex,
                    queue_message_indicator_hex=queue_message_indicator_hex,
                    chunk=options.chunk,
                    message_limit=options.count,
                    output_file=output_file,
                    queue=queue,
                    start_bytes=0,
                    delete_file=options.delete,
                    debug=options.debug,
                    dev_debug=options.dev_debug
                )
                processed_file_count += 1
                print(f"Processed {processed_file_count}/{total_file_count} files.\n")
        else:
            sys.stderr.write(f"Cannot find the specified folder '{folder_path}'\n")
