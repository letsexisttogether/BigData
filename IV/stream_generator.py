import json
import random
import time
import argparse
import threading
from datetime import datetime

from names import JSON_DATASETS


class StreamDataGenerator:
    def __init__(self, start_id: int, file_name: str):
        self.__id_counter = start_id
        self.__file = file_name

    def write_loop(self, delay: int):
        while True:
            self.write_next()
            time.sleep(delay)

    def write_next(self):
        data = self.__generate()
        with open(self.__file, 'a') as f:
            f.write(json.dumps(data) + '\n')

    def __generate(self):
        data = {
            'id': self.__id_counter,
            'name': random.choice(['Alex', 'Vlad', 'Mike', 'Nastya']),
            'university': random.choice(['KPI', 'KNU', 'Karazin', 'KMA']),
            'top': random.randint(1, self.__id_counter),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        self.__id_counter += 1

        return data


def start_generator(id: int, file_name: str, delay: int):
    generator = StreamDataGenerator(id, file_name)
    generator.write_loop(delay)


def default():
    print('Default task is running')

    start_generator(1, JSON_DATASETS + 'UTop.json', 2)


def two_streams():
    print('Two streams task is running')

    file_1 = JSON_DATASETS + 'First/File.json'
    file_2 = JSON_DATASETS + 'Second/File.json'

    thread1 = threading.Thread(target=start_generator, args=(1, file_1, 2))
    thread2 = threading.Thread(target=start_generator, args=(1, file_2, 2))

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Stream task selector')
    parser.add_argument('--task', required=True,
                        help='Task to execute: default, two_streams')

    args = parser.parse_args()

    if args.task == 'default':
        default()
    elif args.task == 'two_streams':
        two_streams()
    else:
        print('There\'s no such a task option')
