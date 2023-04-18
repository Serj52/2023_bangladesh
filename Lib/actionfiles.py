import os
import logging
import zipfile
import re
import shutil
import base64




class ActionFiles:
    def __init__(self, config):
        self.config = config

    @staticmethod
    def clean_dir(dir):
        """
        Удаление файлов из директории
        """
        for file in os.listdir(dir):
            if os.path.isfile(os.path.join(dir, file)):
                os.remove(os.path.join(dir, file))

    @staticmethod
    def save_file(path, content):
        with open(path, "wb") as file:
            file.write(content)
            logging.info(f'Файл {path} сохранен')

    @staticmethod
    def file_compression(dir):
        """
        Архивирования файлов в заданной директории
        param:dir - директория с файлами для архивирования
        """
        for file in os.listdir(dir):
            if re.search('\.\w+', file):
                # меняем расширение файла
                new_name = re.sub('\.\w+', '.zip', file)
            else:
                logging.error('Ошибка обработки файла. Проверьте расширение файлов')
                raise
            with zipfile.ZipFile(os.path.join(dir, new_name), 'w') as zip:
                zip.write(os.path.join(dir, file), file, compress_type=zipfile.ZIP_DEFLATED)
            os.remove(os.path.join(dir, file))
            logging.info(f'Архив {new_name} создан. {file} удален')

    def move_to_processed(self, dir_processed):
        """
        Перенос файлов из in_process в processed
        """
        for file in os.listdir(self.config.in_process):
            shutil.move(os.path.join(self.config.in_process, file), os.path.join(dir_processed, file))

    @staticmethod
    def encode_base64(file):
        """
        Кодирование файла в base64
        param:file - путь до файла
        """
        with open(file, 'rb') as f:
            doc64 = base64.b64encode(f.read())
            logging.info(f'Закодировал {file} в base64')
            doc_b64 = doc64.decode('utf-8')
            return doc_b64