from abc import ABC, abstractmethod
import time
import requests
from Lib.exceptions import WebsiteError
from datetime import datetime
import json
import logging



class Index(ABC):
    def __init__(self, event_handler, actionfiles, config):
        self.date_update = None
        self.last_period = None
        self.eventhandler = event_handler
        self.actionfiles = actionfiles
        self.config = config

    @abstractmethod
    def process(self, mode, new_messages=None, task=None, queue=None):
        pass

    @abstractmethod
    def handler_data(self, data, period, result):
        pass

    @abstractmethod
    def get_start_index(self, periods, search_period):
        pass

    def work_period(self, index, mode, period=''):
        """
        Возвращает или записывает период в файл period.json
        param:index - IPC или ICP
        mode:'read_index' - получение периода из файла,
        'write_index' - запись нового перида в файл
        """
        with open(self.config.period, mode='r', encoding='utf-8') as file:
            output = json.load(file)
            if mode == 'read_period':
                period = output[index]
                return period
            elif mode == 'write_period':
                output[index] = period
                json.dump(output, open(self.config.period, mode='w', encoding='utf-8'), indent=4, ensure_ascii=False,
                          default=str)
                logging.info(f'Период {period} записан в {self.config.period}')
            else:
                logging.error(f'Неправильно указан mode {mode}')
                raise

    def wait(self, value, type, queue, task, index):
        logging.info(f'Засыпаю до {value} {type}')
        self.eventhandler.handler(queue=queue, task_id=task, event='send_error',
                                  text=f'Сайт не отвечает. Робот осуществит повторную попытку в {value}{type}',
                                  index=index)
        if type in ['hours', 'h', 'H', 'hour']:
            while True:
                hour = datetime.now().hour
                if hour == value:
                    logging.info(f'Проснулся')
                    return
                else:
                    time.sleep(60)
                    continue
        elif type in ['seconds', 'second', 's']:
            time.sleep(value)
            logging.info(f'Проснулся')
            return
        else:
            logging.error('Указан неправильный формат type')
            raise

    def html_content(self, url, index, task, queue):
        """Получение данных с сайта"""
        if queue is None:
            queue = self.config.queue_response
        max_tries = 12
        time_sleep = 1
        while max_tries > 0:
            if max_tries == 1:
                logging.error(f'Сайт не открылся')
                # засыпаем до 21-00
                self.wait(21, 'h', queue, task, index)
            max_tries -= 1
            try:
                response = requests.get(url)
                if response.status_code != 200:
                    logging.info(f'Сайт не доступен. Код ответа {response.status_code}. '
                                 f'Пробую открыть повторно через {time_sleep} сек')
                    time.sleep(time_sleep)
                else:
                    logging.info(f'Сайт доступен. Начинаю загрузку данных')
                    return response.content
            except Exception as err:
                logging.info(f'Сайт не доступен. {err}. Пробую повторно')

        logging.info('Исчерпаны все попытки открыть ссылку')
        raise WebsiteError('сайт не отвечает')



