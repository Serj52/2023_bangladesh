from bs4 import BeautifulSoup
import re
import pandas as pd
import json
from datetime import datetime
import os
from typing import Union
import logging
from index import Index
from Lib.exceptions import DataProcessError


class IPC(Index):

    def __init__(self, event_handler, actionfiles, config):
        super().__init__(event_handler, actionfiles, config)
        self.name_index = 'IPC'
        self.loaded_file = os.path.join(self.config.in_process, 'cpi_bgd.xlsx')

    def process(self, mode: str, new_messages=None, task=None, queue=None) -> None:
        """
        Процесс обработки запроса
        """
        result = {'data': []}
        logging.info(f'{self.name_index} {mode} данных начал')
        self.actionfiles.clean_dir(self.config.in_process)
        url = f"{self.config.url}/{self.config.url_tails[self.name_index]}"
        data = self.load_file(url=url, queue=queue, index=self.name_index, task=task)
        self.actionfiles.save_file(self.loaded_file, data)
        df = pd.read_excel(self.loaded_file, sheet_name='Dataset', index_col=False)
        if mode == 'update':
            currperiod = self.work_period(self.name_index, 'read_period')
            periods = self.get_period(df, currperiod)
            if periods:
                result = self.handler_data(data=df, periods=periods, result=result)
        elif mode == 'migration':
            periods = self.get_period(df)
            result = self.handler_data(data=df, periods=periods, result=result)
        else:
            logging.error("Не корректно указан mode")
            raise

        if result['data']:
            file_data = os.path.join(self.config.in_process, 'file_data.json')
            json.dump(result, open(file_data, mode='w', encoding='utf-8'),
                      indent=4, ensure_ascii=False, default=str)
            self.actionfiles.file_compression(self.config.in_process)
            self.date_update = str(datetime.now())
            self.eventhandler.handler(queue=self.config.queue_response, task_id=task, event='send_data',
                                      text='first_run', index=self.name_index,
                                      period=self.last_period, new_message=new_messages, date_update=self.date_update)
            self.work_period(self.name_index, 'write_period', self.last_period)
            self.actionfiles.move_to_processed(self.config.processed_ipc)
        else:
            self.eventhandler.handler(queue=queue, task_id=task, event='send_error',
                                      text='Нет новых данных', index=self.name_index,
                                      new_message=new_messages)
            logging.info('Нет новых данных')
        logging.info(f'{self.name_index} {mode} данных закнончил')

    def load_file(self, url: str, index: str, queue: str, task: str) -> bytes:
        """
        Возвращает содержание html страницы
        """
        link = None
        page_content = self.html_content(url, index, queue, task)
        soup = BeautifulSoup(page_content, 'html.parser')
        index = soup.find(text=re.compile('Consumer Price Index')).parent
        for sibling in index.next_siblings:
            if 'Browse Data' in sibling.text:
                for child in sibling.children:
                    if child == '\n':
                        continue
                    elif child.name == 'a':
                        link = f"{self.config.url}/{child['href']}"
                        break
                break
        if link is None:
            logging.error('Проверьте селекторы на сайте')
            raise DataProcessError('Ошибка при обработке данных на сайте')
        else:
            content = self.html_content(link, index, queue, task)
        return content

    def get_period(self, data, olddate=None) -> Union[bool, tuple]:
        """
        Возвращает индекс начала поиска и список с периодами
        например
        (10, [2023-22, 2023-21])
        если задан date, то сравниваем его с последним периодом из data
        и инкрементируем его
        """
        period = []
        row_period = data.loc[data['Consumer Price Index'] == 'Descriptor']
        periods_col = row_period.values.tolist()[0]
        self.last_period = periods_col[-1]
        start_year = re.findall(r'^\d{4}', self.last_period)[0]
        # Если последняя дата в файле не изменилась, возвращаем False
        if olddate:
            if olddate == self.last_period:
                return False
            else:
                slice = self.get_start_index(periods_col, olddate)
                slice += 1
        else:
            # вычитаем пять лет, получаем стартовую дату
            date = re.sub(r'^\d{4}', str(int(start_year) - 5), self.last_period)
            slice = self.get_start_index(periods_col, date)
        for value in periods_col[slice:]:
            period.append(value)
        return (slice, period)

    def get_start_index(self, periods: list, search_period: str) -> int:
        """Возвращает индекс найденного значения"""
        high = len(periods) - 1
        low = 0
        index = 0
        while low <= high:
            index = (low + high) // 2
            if periods[index] == search_period:
                return index
            elif periods[index] > search_period:
                high = index - 1
            else:
                low = index + 1
        return index

    def handler_data(self, data, periods: tuple, result: dict) -> dict:
        # получаем данные из строки с индексами
        row_price = data.loc[data['Consumer Price Index'] == 'Prices, Consumer Price Index, All items, Average, Index']
        prices = row_price.values.tolist()[0]
        slice = periods[0]
        for index, value in enumerate(prices[slice:]):
            value = {
                'name_period': 'M',
                'period': periods[1][index],
                'index': value
            }
            result["data"].append(value)
        return result


class ICP(Index):
    def __init__(self, event_handler, actionfiles, config):
        super().__init__(event_handler, actionfiles, config)
        self.name_index = 'ICP'
        self.loaded_file = os.path.join(self.config.in_process, 'data.xml')

    def get_start_index(self, periods: list, search_period: str) -> int:
        """Возвращаем индекс по значению"""
        high = len(periods) - 1
        low = 0
        index = 0
        while low <= high:
            index = (low + high) // 2
            if periods[index].get('time_period') == search_period:
                return index
            elif periods[index].get('time_period') > search_period:
                high = index - 1
            else:
                low = index + 1
        return index

    def handler_data(self, data: bytes, period: str, result: dict, new_data=True) -> dict:
        """
        Сбор данных в заданном периоде
        param:data - данные для поиска
        param:period - период поиска
        param:new_data=True для поиска нового периода,
        new_data=False для поиска данных за 5 лет
        return: данные формата
        {'data':[{'name_period':'', 'period':'', 'type_index':'','okved2':'', 'country':'', 'index':''}, ..]}
        """
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
                            # получаем индекс элемента с которого начинаем запись
                            start_index = self.get_start_index(indicator.contents, period)
                            # Если не последний период в списке, записываем новые
                            if start_index and start_index != len(indicator.contents) - 1:
                                # если ищим данные не за 5 лет
                                if new_data:
                                    start_index += 1
                                for index, month in enumerate(indicator.contents[start_index:]):
                                    if self.last_period is None and index == len(indicator.contents[start_index:]) - 1:
                                        self.last_period = month['time_period']
                                    value = {
                                        'name_period': indicator['freq'],
                                        'period': month['time_period'],
                                        'type_index': indicator['data_domain'],
                                        'okved2': indicator['indicator'],
                                        'index': indicator.contents[-1]['obs_value']
                                    }
                                    result["data"].append(value)

            else:
                logging.info('В загруженных данных не найден тег series')
            return result
        except Exception as err:
            logging.info(f'Ошибка при парсинге данных {err}')
            raise DataProcessError('Ошибка при обработке данных на сайте')

    @staticmethod
    def get_start_period(data: bytes) -> list:
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
                            last_period = indicator.contents[-1]['time_period']
                            logging.info(f'Последний период {last_period}')
                            start_year = re.findall(r'^\d{4}', last_period)[0]
                            # вычитаем пять лет, получаем стартовую дату
                            start_period = re.sub(r'^\d{4}', str(int(start_year) - 5), last_period)
                            return [start_period, last_period]
                        except Exception as err:
                            logging.error(f'Проверьте формат периода на сайте {err}')
                            raise
        logging.info(f'Список периодов за 5 лет получен')
        return period

    def process(self, mode: str, new_messages=None, task=None, queue=None) -> None:
        logging.info(f'{self.name_index} {mode} данных начал')
        self.actionfiles.clean_dir(self.config.in_process)
        result = {"data": []}
        url = f"{self.config.url}/{self.config.url_tails[self.name_index]}"
        data = self.html_content(url, self.name_index, task, queue)
        if mode == 'update':
            # берем из файла
            period = self.work_period(self.name_index, 'read_period')
            result = self.handler_data(data, period, result)
        elif mode == 'migration':
            # находим от последней даты минус 5 лет
            period = self.get_start_period(data)
            result = self.handler_data(data, period[0], result, False)
        else:
            logging.error("Не корректно указан mode")
            raise
        if result['data']:
            self.actionfiles.save_file(self.loaded_file, data)
            # записываю новый период в файл
            self.work_period(self.name_index, 'write_period', self.last_period)
            # создаем file_data.json
            file_data = os.path.join(self.config.in_process, 'file_data.json')
            json.dump(result, open(file_data, mode='w', encoding='utf-8'),
                      indent=4, ensure_ascii=False, default=str)
            self.actionfiles.file_compression(self.config.in_process)
            self.eventhandler.handler(queue=queue, task_id=task, event='send_data',
                                      text='Выполнено', index=self.name_index,
                                      period=self.last_period, new_message=new_messages, date_update=self.date_update)
            self.actionfiles.move_to_processed(self.config.processed_icp)
        else:
            self.eventhandler.handler(queue=queue, task_id=task, event='send_error',
                                      text='Нет новых данных', index=self.name_index, new_message=new_messages)
            logging.info('Нет новых данных')
        logging.info(f'{self.name_index} {mode} данных закончил')
