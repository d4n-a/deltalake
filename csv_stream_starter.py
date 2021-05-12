import csv
import time
import random

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
            self.pos = new_pos
            return '\n'.join([','.join(row) for row in data])


def produce_file(source, dir, number):
    with open(dir + '/source' + str(number) + '.csv', 'w', newline='') as wf:
        data = source.read(random.randint(5, 15)).strip('\n')
        # data = source.read(1).strip('\n')
        print(f'{len(data)=}')
        # print(f'{data=}')
        wf.write(data)

def start_csv_stream():
    idx = 0
    source = Source("datasource/all_stocks_5yr.csv")
    while True:
        produce_file(source, 'datastream', idx)
        idx += 1
        # reduce time
        wait_time = 1
        time.sleep(wait_time)

if __name__ == '__main__':
    start_csv_stream()
