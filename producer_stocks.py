import random


template = {
    'trans_id', 'sender_id', 'sender_info', 'recv_id', 'recv_info', 'amount'
}

def generate_trans_id():
    return random.randint(1000, 100000)

def generate_sender_id():
    return random.getrandbits(128)

def generate_sender_info(trans_id):
    return "Some sender values for trans_id={}".format(trans_id)

def generate_recv_id():
    return random.getrandbits(128)

def generate_recv_info(trans_id):
    return "Some recv values for trans_id={}".format(trans_id)

def generate_amount(trans_id):
    return str(trans_id)[:3]


def generate_event():
    trans_id = generate_trans_id()
    event = (
        ('trans_id', trans_id),
        ('sender_id', generate_sender_id()),
        ('sender_info', generate_sender_info(trans_id)),
        ('recv_id', generate_recv_id()),
        ('recv_info', generate_recv_info(trans_id)),
        ('amount', generate_amount(trans_id)),
    )
    return event

import pprint
pprint.pprint(generate_event())


import socket
import time
import random
import csv
from collections.abc import Callable


HOST = '127.0.0.1'
PORT = 9999
WORDS_IN_SENTENCE = 10


class Source():
    def __init__(self, filename):
        self.pos = 0
        self.data = []

        with open(filename, 'r') as rf:
            reader = csv.reader(rf)
            self.data = sorted([row for row in reader], key=lambda row: row[0]) # sort by date

    def read(self, strings_amount):
            new_pos = self.pos + strings_amount
            data = self.data[self.pos:new_pos]
            print(f'{data=}')
            self.pos = new_pos
            print(f'{self.pos=}')
            return '\n'.join([','.join(row) for row in data])

import csv

def produce_file(source, dir, number):
    with open(dir + '/source' + str(number) + '.csv', 'w', newline='') as wf:
        data = source.read(random.randint(5, 15)).strip('\n')
        # data = source.read(1).strip('\n')
        print(f'{len(data)=}')
        print(f'{data=}')
        wf.write(data)

def run_files():
    idx = 0
    source = Source("all_stocks_5yr.csv")
    while True:
        produce_file(source, 'datasource', idx)
        idx += 1
        # reduce time
        wait_time = 1
        time.sleep(wait_time)

if __name__ == '__main__':
    run_files()
