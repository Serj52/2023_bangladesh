from business import Business
import os
from Lib.rabbit import Rabbit
from config import Config as cfg
import pytest
from Lib import exceptions


def test_exception():
    with pytest.raises(exceptions.WebsiteError):
        b = Business()
        b.get_data(cfg.bed_url['ICP'])

def test_handler_data():
    """
    Проверка парсинга данных на сайте по заданному периоду
    """
    b = Business()
    data = b.get_data(cfg.url['ICP'])
    result = b.handler_data(data, '2022-08')
    assert result['data'] != []
    for index in result['data']:
        assert index.get("name_period") != None
        assert index.get("period") != None
        assert index.get("type_index") != None
        assert index.get("okved2") != None
        assert index.get("country") != None
        assert index.get("index") != None

def test_work_period():
    """Проверка функции записи и извлечения периодов из файла period.json"""
    for index in ['IPC', 'ICP']:
        new_period = '2022-08'
        Business().work_period(index, 'write_period', new_period)
        period = Business().work_period(index, 'read_period')
        assert new_period == period

def test_robot():
    """
    Проверка всего цикла обработки валидного запроса
    """
    #перезапишем период в файле
    for index in ['ICP']:
        Business().work_period(index, 'write_period', '2022-08')
        request = os.path.join(cfg.folder_root, 'tests', 'request.json')
        Rabbit().producer_queue(queue_name='hello', data_path=request)
        Business().process_task()
        response = Rabbit().check_queue('goodbay')
        assert response != None
        assert response['header']['timestamp'] != ''
        assert response['header']['requestID'] != ''
        assert response['body']['date_update'] != ''
        assert response['body']['file_data'] != ''
        assert response['body']['files'] != []
        assert response['body']['period'] != ""

def test_first_start_business():
    """
    Проверка всего цикла обработки валидного запроса
    """
    #перезапишем период в файле
    Business().first_start_business()
    for index in ['IPC', 'ICP']:
        response = Rabbit().check_queue('goodbay')
        assert response != None
        assert response['header']['timestamp'] != ''
        assert response['body']['date_update'] != ''
        assert response['body']['file_data'] != ''
        assert response['body']['files'] != []
        assert response['body']['period'] != ""

def test_bad_period():
    """
    Проверка ответа на запрос с периодом за который отсутвуют данные
    в функции get_period нужно изменить cfg.period на cfg.bad_period
    """
    request = os.path.join(cfg.folder_root, 'tests', 'request.json')
    Rabbit().producer_queue(queue_name='hello', data_path=request)
    Business().process_task()
    response = Rabbit().check_queue('goodbay')
    assert response != None
    assert response['header']['timestamp'] != ''
    assert response['header']['requestID'] != ''
    assert response['body']['period'] != ''
    assert response['body']['date_update'] != ''
    assert response['body']['file_data'] == ''
    assert response['body']['files'] == []
    assert response['ErrorText'] == 'Нет новых данных'

def test_bad_request():
    """
    Проверка валидации данных. В запросе 'subject'=''
    """
    request = os.path.join(cfg.folder_root, 'tests', 'request_bad.json')
    Rabbit().producer_queue(queue_name='hello', data_path=request)
    Business().process_task()
    response = Rabbit().check_queue('goodbay')
    assert response != None
    assert response['header']['timestamp'] != ''
    assert response['header']['requestID'] != ''
    assert response['body']['date_update'] == ''
    assert response['body']['file_data'] == ''
    assert response['body']['files'] == []
    assert response['ErrorText'] == 'Запрос не валиден'

def test_bad_encoding_request():
    Rabbit().producer_queue_test(queue_name='hello')
    Business().process_task()
    response = Rabbit().check_queue(cfg.queue_error)
    assert response != None
    assert response['header']['timestamp'] != ''
    assert response['body']['date_update'] == ''
    assert response['body']['file_data'] == ''
    assert response['body']['files'] == []
    assert 'Запрос не валиден' in response['ErrorText']

