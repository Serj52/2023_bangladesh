import pika
import time
from config import Config as cfg
import json
import logging
import logging.config



class Rabbit:
    def __int__(self):
        self.login = cfg.rabbit_login
        self.password = cfg.rabbit_pwd
        self.port = cfg.rabbit_port
        self.host = cfg.rabbit_host
        self.path = cfg.path
        self.task_id = None
        self.queue_response = None

    def connection(self, max_tries=5):
        try:
            credentials = pika.PlainCredentials(cfg.rabbit_login, cfg.rabbit_pwd)
            parameters = pika.ConnectionParameters(cfg.rabbit_host, cfg.rabbit_port, cfg.path, credentials)
            connection = pika.BlockingConnection(parameters)
            return connection
        except Exception:
            if max_tries == 0:
                logging.error('Попытки подключиться к RabbitMQ исчерпаны')
                raise
            else:
                logging.error('Ошибка подключения к RabbitMQ! Пробую подключиться повторно')
                max_tries -= 1

    def send_data_queue(self, queue_response, data):
        channel = self.connection().channel()
        channel.queue_declare(queue=queue_response, durable=True)
        # Отметить сообщения как устойчивые delivery_mode=2, защищенные от потери
        channel.basic_publish(exchange='',
                              routing_key=queue_response,
                              body=data,
                              properties=pika.BasicProperties(delivery_mode=2, )
                              )
        logging.info(f'Сообщение отправлено в очередь {queue_response}')
        #сохранение отправленного json в папке с запросом
        self.connection().close()

    def check_queue(self, queue):
        """
        Получить сообщения из очереди
        """
        channel = self.connection().channel()
        method_frame, header_frame, body = channel.basic_get(queue=queue)
        if method_frame:
            channel.basic_ack(method_frame.delivery_tag)
            data = json.loads(body)
            logging.info('Получено сообщение из очереди')
            return data

    def producer_queue(self, queue_name, data_path):
        channel = self.connection().channel()
        # # Создается очередь.устойчивая очередь durable=True к падению сервера с rabbit mq. Сообщения останутся в очереди после падения сервера
        # channel.queue_declare(queue=queue_name, durable=True)

        with open(data_path, encoding='utf-8') as file:
            # messageBody = json.dumps('Hello world', sort_keys=True, indent=4)
            messageBody = file.read()
            # Отметить сообщения как устойчивые delivery_mode=2, защищенные от потери
            channel.basic_publish(exchange='',
                                  routing_key=queue_name,
                                  body=messageBody,
                                  properties=pika.BasicProperties(delivery_mode=2, )
                                  )
        logging.info("Sent")
        time.sleep(2)
        self.connection().close()


    def producer_queue_test(self, queue_name):
        """
        Только для тестов
        """
        channel = self.connection().channel()
        channel.basic_publish(exchange='',
                              routing_key=queue_name,
                              body=b'messageBody',
                              properties=pika.BasicProperties(delivery_mode=2, )
                              )
        logging.info("Sent")
        time.sleep(2)
        self.connection().close()