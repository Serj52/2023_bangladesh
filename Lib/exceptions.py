
class WebsiteError(Exception):
    def __init__(self, text):
        self.txt = text


class DownloadError(Exception):
    def __init__(self, text):
        self.txt = text


class BadPeriodError(Exception):
    def __init__(self, text):
        self.txt = text


class DataProcessError(Exception):
    def __init__(self, text):
        self.txt = text

class NoUpdatesError(Exception):
    def __init__(self, text):
        self.txt = text

class BadRequest(Exception):
    def __init__(self, text):
        self.txt = text


class NotFoundElement(Exception):
    def __init__(self, text):
        self.txt = text

class ExctractError(Exception):
    def __init__(self, text):
        self.txt = text

class FileNotFound(Exception):
    def __init__(self, text):
        self.txt = text
