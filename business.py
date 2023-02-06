import requests
import json
from datetime import datetime
import time
import zipfile
import base64
import logging.config
import re
import shutil
import logging
from bs4 import BeautifulSoup
import os
import jsonschema.exceptions
from jsonschema import validate
from Templates.valid_shema import schema
from Lib.rabbit import Rabbit
from config import Config as cfg
from b_excel import BusinessExcel
from Lib.exceptions import WebsiteError, DataProcessError
from Lib import log


class Business:
    def __init__(self):
        self.date_update = ''
        self.period = ''

    def validator(self, task):
        try:
            validate(task, schema)
            return True
        except jsonschema.exceptions.ValidationError as error:
            logging.info(f'Не валидный запрос {error}')
            return False

    def get_data(self, url, queue='', task=''):
        """Получение данных с сайта"""
        max_tries = 12
        time_sleep = 1
        while max_tries > 0:
            if max_tries == 1:
                logging.error(f'Сайт не открылся')
                self.wait(13, 'h', queue, task)
            max_tries -= 1
            response = requests.get(url)
            if response.status_code != 200:
                logging.info(f'Сайт не доступен. Код ответа {response.status_code}. '
                             f'Пробую открыть повторно через {time_sleep} сек')
                time.sleep(time_sleep)
            else:
                logging.info(f'Сайт доступен. Начинаю загрузку данных')
                return response.content

        logging.info('Исчерпаны все попытки открыть ссылку')
        raise WebsiteError('сайт не отвечает')

    def wait(self, value, type, queue, task):
        logging.info(f'Засыпаю до {value} {type}')
        self.event_handler(queue=queue, task_id=task, event='send_error',
                           text=f'Сайт не отвечает. Робот осуществит повторную попытку в {value}{type}')
        if type in ['hours', 'h', 'H', 'hour']:
            while True:
                now = datetime.now()
                hours = now.hour
                if hours == value:
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

    def get_start_period(self, data, index):
        """
        Возвращаем значение = (конечный период - 5 лет)
        param:data:данные
        param:index:наименование периодв для обновления его значения в файле period.json.
        Может быть ICP или IPC
        return: период формата '2022-10'
        """
        soup = BeautifulSoup(data, 'html.parser')
        dataset = soup.find_all('series')
        period = []
        if dataset:
            for indicator in dataset:
                if indicator == '\n':
                    continue
                else:
                    if 'IX' in indicator['indicator']:
                        try:
                            # последний период
                            last_period = indicator.contents[len(indicator) - 1]['time_period']
                            #обновляем период в файле period.json
                            self.work_period(index, 'write_period', last_period)
                            logging.info(f'Последний период {last_period}')
                            start_year = re.findall(r'^\d{4}', last_period)[0]
                            # вычитаем пять лет, получаем стартовую дату
                            start_period = re.sub(r'^\d{4}', str(int(start_year) - 5), last_period)
                            return start_period
                        except Exception as err:
                            logging.error(f'Проверьте формат периода на сайте {err}')
                            raise
        logging.info(f'Список периодов за 5 лет получен')
        return period

    def first_start_business(self):
        """
        Процесс по передаче данных за 5 лет. Выполняется только при первом запуске робота!!
        """
        logging.info('Начинаю миграция данных за 5 лет')
        for type_index in ['ICP', 'IPC']:
            self.clean_dir(cfg.in_process)
            url = cfg.url[type_index]
            data = self.get_data(url)
            with open(os.path.join(cfg.in_process, 'data.xml'), mode='wb') as file:
                file.write(data)
            #получаем спивок перидов
            period = self.get_start_period(data, type_index)
            # получаем данные за период
            result = self.handler_data(data, period, False)
            file_data = os.path.join(cfg.in_process, 'file_data.json')
            json.dump(result, open(file_data, mode='w', encoding='utf-8'),
                      indent=4, ensure_ascii=False, default=str)
            self.file_compression(cfg.in_process)
            self.event_handler(queue='goodbay', task_id='', event='send_data',
                               text='first_run', period=f'{period[0]}-{period[len(period)-1]}')
            self.move_to_processed()
            logging.info(f'Данные по индексу {type_index} в шину переданы')
        logging.info('Миграция данных за 5 лет завершена')

    def add_in_result(self, indicator, month):
        """
        Запись данных в словарик
        param:indicator - тег нужного периода
        param:month - период
        param:result
        """
        self.period = month
        value = {
            'name_period': indicator['freq'],
            'period': month,
            'type_index': indicator['data_domain'],
            'okved2': indicator['indicator'],
            'country': 'Бангладеш',
            'index': indicator.contents[len(indicator) - 1]['obs_value']
        }
        return value

    def get_start_index(self, periods: list, search_period):
        """Возвращает search_period из списка  periods:"""
        high = len(periods) - 1
        low = 0
        while low <= high:
            index = (low + high) // 2
            if periods[index].get('time_period') == search_period:
                return index
            elif periods[index].get('time_period') > search_period:
                high = index - 1
            else:
                low = index + 1

    def handler_data(self, data, period, new_data=True):
        """
        Сбор данных в заданном периоде
        param:data - данные для поиска
        param:period - период поиска
        param:new_data=True для поиска нового периода,
        new_data=False для поиска данных за 5 лет
        return: данные формата
        {'data':[{'name_period':'', 'period':'', 'type_index':'','okved2':'', 'country':'', 'index':''}, ..]}
        """
        result = {"data": []}
        soup = BeautifulSoup(data, 'html.parser')
        dataset = soup.find_all('series')
        self.date_update = soup.find('message:prepared').text
        try:
            if dataset:
                for indicator in dataset:
                    if indicator == '\n':
                        continue
                    else:
                        if 'IX' in indicator['indicator']:
                            start_index = self.get_start_index(indicator.contents, period)
                            # Если не последний период в списке, записываем новые
                            if start_index != len(indicator.contents) - 1:
                                # если ищим данные не за 5 лет
                                if new_data:
                                    start_index += 1
                                for month in indicator.contents[start_index:]:
                                    value = self.add_in_result(indicator, month['time_period'])
                                    result["data"].append(value)
            else:
                logging.info('В загруженных данных не найден тег series')
            return result
        except Exception as err:
            logging.info(f'Ошибка при парсинге данных {err}')
            raise DataProcessError('Ошибка при обработке данных на сайте')

    def create_message(self, type_response, task_id, period, text=''):
        """
        Создание сообщения для ответа в шину данных
        param:type_response - send_data: для отправки найденных данных в шину,
        send_error для отправки в шину сообщения об ошибке
        param:task_id - id запроса
        param:period - период за который были найдены данные
        param:text - текст для записи в ответ
        """
        with open(cfg.response, mode='r', encoding='utf-8') as file:
            response = json.load(file)
        response['header']['requestID'] = task_id
        response["header"]["timestamp"] = datetime.timestamp(datetime.now())
        if type_response == 'send_data':
            file = self.encode_base64(os.path.join(cfg.in_process, 'data.zip'))
            file_data = self.encode_base64(os.path.join(cfg.in_process, 'file_data.zip'))
            response["body"]["date_update"] = self.date_update
            response["body"]["files"].append({'name': 'data.xml', 'base64': file})
            response["body"]["file_data"] = file_data
            path_response = os.path.join(cfg.in_process, 'response.json')
            response["body"]["period"] = period
            # сохраняю файл в директории in_process
            json.dump(response, open(path_response, mode='w', encoding='utf-8'),
                      indent=4, ensure_ascii=False, default=str)
        elif type_response == 'send_error':
            response["body"]["date_update"] = self.date_update
            response['ErrorText'] = text
            response["body"]["period"] = period
        # возвращаю файл в формате json
        message = json.dumps(response, indent=4, ensure_ascii=False, default=str)
        return message

    def encode_base64(self, file):
        """
        Кодирование файла в base64
        param:file - путь до файла
        """
        with open(file, 'rb') as f:
            doc64 = base64.b64encode(f.read())
            logging.info(f'Закодировал {file} в base64')
            doc_b64 = doc64.decode('utf-8')
            return doc_b64

    def file_compression(self, dir):
        """
        Архивирования файлов
        param:dir - директория с файлами для архивирования
        """
        os.chdir(dir)
        for file in os.listdir(dir):
            if '.xml' in file:
                new_name = file.replace('.xml', '.zip')
            elif '.json' in file:
                new_name = file.replace('.json', '.zip')
            else:
                logging.error('Ошибка обработки файла. Проверьте расширение файлов')
                raise
            with zipfile.ZipFile(new_name, 'w') as zip:
                zip.write(file, compress_type=zipfile.ZIP_DEFLATED)
            os.remove(file)
            logging.info(f'Архив {new_name} создан. {file} удален')
        os.chdir(cfg.folder_root)

    def clean_dir(self, dir):
        """
        Удаление файлов из директории
        """
        for file in os.listdir(dir):
            if os.path.isfile(os.path.join(dir, file)):
                os.remove(os.path.join(dir, file))

    def move_to_processed(self):
        """
        Перенос файлов из in_process в processed
        """
        for file in os.listdir(cfg.in_process):
            shutil.move(os.path.join(cfg.in_process, file), os.path.join(cfg.processed, file))

    def event_handler(self, queue, task_id, event, text, period=''):
        """
        Подготовка данных для ответа в шину данных
        param:queue - очередь для ответа
        param:task_id  - id ответа
        param:event  - send_data: для отправки найденных данных в шину,
        send_error для отправки в шину сообщения об ошибке
        param:text - текст для записи в ответ
        param:period - период за который были найдены данные
        """
        #TODO:Дописать отправку почты на целевом сервере
        message = self.create_message(type_response=event, task_id=task_id, period=period, text=text)
        Rabbit().send_data_queue(queue, message)
        BusinessExcel().write_task_log(message, cfg.task_log, text, period)

    def work_period(self, index, mode, period=''):
        """
        Возвращает или записывает период в файл period.json
        param:index - IPC или ICP
        mode:'read_index' - получение периода из файла,
        'write_index' - запись нового перида в файл
        """
        with open(cfg.period, mode='r', encoding='utf-8') as file:
            output = json.load(file)
            if mode == 'read_period':
                period = output[index]
                return period
            elif mode == 'write_period':
                output[index] = period
                json.dump(output, open(cfg.period, mode='w', encoding='utf-8'), indent=4, ensure_ascii=False,
                          default=str)
                logging.info(f'Период {period} записан в {cfg.period}')
            else:
                logging.error(f'Неправильно указан mode {mode}')
                raise

    def process_task(self):
        """
        Процесс обработки запроса на получение данных
        """
        self.clean_dir(cfg.in_process)
        new_messages = {}
        self.date_update = ''
        try:
            new_messages = Rabbit().check_queue(cfg.queue_request)
        except json.JSONDecodeError as err:
            self.event_handler(queue=cfg.queue_error, task_id=' ', event='send_error',
                               text='Запрос не валиден. Ожидал JSON')
            logging.error(f'Проверьте кодировку запроса {err}')
        if new_messages:
            task_id = new_messages['header']['requestID']
            queue = new_messages['header']['replayRoutingKey']
            try:
                if self.validator(new_messages):
                    type_index = new_messages['header']['subject']
                    period = self.work_period(type_index,'read_period')
                    BusinessExcel().write_task_log(new_messages, cfg.task_log, 'new_task', period)
                    url = cfg.url[type_index]
                    data = self.get_data(url)
                    with open(os.path.join(cfg.in_process, 'data.xml'), mode='wb') as file:
                        file.write(data)
                    logging.info('Данные сохранены в файл')
                    result = self.handler_data(data, period)
                    # для записи в response делаем из формата 2022-10 в 2022-10-01T00:00:00
                    period = f'{period}-01T00:00:00'
                    if result['data']:
                        #записываю новый период в файл
                        self.work_period(type_index, 'write_period', self.period)
                        # создаем file_data.json
                        file_data = os.path.join(cfg.in_process, 'file_data.json')
                        json.dump(result, open(file_data, mode='w', encoding='utf-8'),
                                  indent=4, ensure_ascii=False, default=str)
                        self.file_compression(cfg.in_process)
                        self.event_handler(queue=queue, task_id=task_id, event='send_data',
                                           text='Выполнено', period=period)
                        self.move_to_processed()
                    else:
                        self.event_handler(queue=queue, task_id=task_id, event='send_error',
                                           text='Нет новых данных', period=period)
                        logging.info('Нет новых данных')
                else:
                    self.event_handler(queue=queue, task_id=task_id, event='send_error',
                                       text='Запрос не валиден')
                    logging.info('Запрос не валидный')
                self.clean_dir(cfg.in_process)
                logging.info('Обработка запроса завершена')
            except WebsiteError as err:
                logging.info(err)
                self.event_handler(queue=queue, task_id=task_id, event='send_error',
                                   text='Не удалось перейти на сайт')
            except DataProcessError as err:
                logging.info(err)
                self.event_handler(queue=queue, task_id=task_id, event='send_error',
                                   text='Ошибка при обработке данных на сайте')
            except Exception as err:
                logging.info(err)
                self.event_handler(queue=queue, task_id=task_id, event='send_error',
                                   text='Не предвиденная ошибка при обработке запроса')


if __name__ == '__main__':
    log.set_1(cfg)
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': True,
    })
    logging.info('\n\n=== Start ===\n\n')
    logging.info(f'Режим запуска {cfg.mode}')
    b = Business()
    b.get_data(cfg.bed_url['ICP'], 'goodbay', '666')
