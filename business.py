import json
from datetime import datetime
import logging.config
import logging
import os
from typing import Union
import jsonschema.exceptions
from jsonschema import validate
from Templates.valid_shema import schema
from Lib.rabbit import Rabbit
from config import Config
from b_excel import BusinessExcel
from Lib.exceptions import WebsiteError, DataProcessError
from Lib import log
from index_handlers import ICP, IPC
from Lib.actionfiles import ActionFiles


class EventHandler:
    def __init__(self, actionfiles: ActionFiles, excel: BusinessExcel, rabbit: Rabbit, config: Config):
        self.actionfiles = actionfiles
        self.excel = excel
        self.rabbit = rabbit
        self.config = config

    def handler(self, queue: str, task_id: str, event: str, text: str, index: str, date_update=None, period=None,
                new_message=None) -> None:
        """
        Подготовка данных для ответа в шину данных
        param:queue - очередь для ответа
        param:task_id  - id ответа
        param:event  - send_data: для отправки найденных данных в шину,
        send_error для отправки в шину сообщения об ошибке
        param:text - текст для записи в ответ
        param:period - период за который были найдены данные
        """
        # TODO:Дописать отправку почты на целевом сервере
        message = self.create_message(type_response=event, task_id=task_id, period=period, index=index, text=text,
                                      date_update=date_update)
        self.rabbit.send_data_queue(queue, message)

        data = {
            "id": task_id,
            "request": new_message,
            "subject": index,
            'response': message,
            "status": text,
            "date response": str(datetime.now())
        }
        self.excel.write_task_log(data, self.config.task_log)

    def create_message(self, type_response: str, task_id: str, period: str, index: str, date_update, text='') -> json:
        """
        Создание сообщения для ответа в шину данных
        param:type_response - send_data: для отправки найденных данных в шину,
        send_error для отправки в шину сообщения об ошибке
        param:task_id - id запроса
        param:period - период за который были найдены данные
        param:text - текст для записи в ответ
        """
        with open(self.config.response, mode='r', encoding='utf-8') as file:
            response = json.load(file)
        response['header']['requestID'] = task_id
        response["header"]["timestamp"] = datetime.timestamp(datetime.now())
        response['header']['subject'] = index
        if type_response == 'send_data':
            for file in os.listdir(self.config.in_process):
                file_encoded = self.actionfiles.encode_base64(os.path.join(self.config.in_process, file))
                if 'file_data' in file:
                    response["body"]["file_data"] = file_encoded
                else:
                    response["body"]["files"].append({'name': file, 'base64': file_encoded})
            response["body"]["date_update"] = date_update
            path_response = os.path.join(self.config.in_process, 'response.json')
            response["body"]["period"] = period
            # сохраняю файл в директории in_process
            json.dump(response, open(path_response, mode='w', encoding='utf-8'),
                      indent=4, ensure_ascii=False, default=str)
        elif type_response == 'send_error':
            response["body"]["date_update"] = date_update
            response['ErrorText'] = text
            response["body"]["period"] = period
        # возвращаю файл в формате json
        message = json.dumps(response, indent=4, ensure_ascii=False, default=str)
        return message


class IndexBusiness:

    def __init__(self):
        self.config = Config()
        self.actionfiles = ActionFiles(self.config)
        self.excel = BusinessExcel()
        self.rabbit = Rabbit(self.config)
        self.eventhandler = EventHandler(self.actionfiles, self.excel, self.rabbit, self.config)

    @staticmethod
    def validator(task: json) -> bool:
        try:
            validate(task, schema)
            return True
        except jsonschema.exceptions.ValidationError as error:
            logging.info(f'Не валидный запрос {error}')
            return False

    def first_start(self):
        ICP(self.eventhandler, self.actionfiles, self.config).process(mode='migration',
                                                                      queue=self.config.queue_response)
        IPC(self.eventhandler, self.actionfiles, self.config).process(mode='migration',
                                                                      queue=self.config.queue_response)

    def indexhandler(self, type_index: str) -> Union[IPC, ICP]:
        if type_index == 'ICP':
            return ICP(self.eventhandler, self.actionfiles, self.config)
        elif type_index == 'IPC':
            return IPC(self.eventhandler, self.actionfiles, self.config)

    def process_task(self) -> None:
        """
        Процесс обработки запроса на получение данных
        """
        new_messages = {}
        type_index = ''
        try:
            new_messages = self.rabbit.check_queue(self.config.queue_request)
        except json.JSONDecodeError as err:
            self.eventhandler.handler(queue=self.config.queue_error, task_id=' ', event='send_error',
                                      text='Запрос не валиден. Ожидал JSON', index=' ')
            logging.error(f'Проверьте кодировку запроса {err}')
        if new_messages:
            task_id = new_messages['header']['requestID']
            queue = new_messages['header']['replayRoutingKey']
            try:
                if self.validator(new_messages):
                    type_index = new_messages['header']['subject']
                    handler = self.indexhandler(type_index)
                    handler.process('update', new_messages, task_id, queue)
                else:
                    self.eventhandler.handler(queue=queue, task_id=task_id, event='send_error',
                                              text='Запрос не валиден', index=type_index)
                    logging.info('Запрос не валиден')
            except WebsiteError as err:
                logging.info(err)
                self.eventhandler.handler(queue=queue, task_id=task_id, event='send_error',
                                          text='Не удалось перейти на сайт', index=type_index, new_message=new_messages)
            except DataProcessError as err:
                logging.info(err)
                self.eventhandler.handler(queue=queue, task_id=task_id, event='send_error',
                                          text='Ошибка при обработке данных на сайте', index=type_index,
                                          new_message=new_messages)
            except Exception as err:
                logging.info(err)
                self.eventhandler.handler(queue=queue, task_id=task_id, event='send_error',
                                          text='Не предвиденная ошибка при обработке запроса', index=type_index,
                                          new_message=new_messages)


if __name__ == '__main__':
    log.set_1(Config)
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': True,

    })
    logging.info('\n\n=== Start ===\n\n')
    logging.info(f'Режим запуска {Config}')
    b = IndexBusiness()
    b.first_start()
