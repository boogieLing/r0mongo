import logging
import time
import os.path
from os import makedirs, listdir, getcwd, access, W_OK
from hashlib import sha256


DEFAULT_LOG_DIR = '~/logs'
ALTERNATE_LOG_DIR = '/tmp/logs'
LOG_LEVELS = {
    'critical': logging.CRITICAL,
    'error': logging.ERROR,
    'warning': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG,
    'notset': logging.NOTSET,
}


def abspath(filepath):
    """Return absolute path to filepath"""
    return os.path.abspath(os.path.expanduser(filepath))


def repopath(filepath=''):
    """Return base git repo for filepath"""
    path = abspath(filepath) if filepath else getcwd()
    while not os.path.isdir(os.path.join(path, '.git')):
        path = os.path.dirname(path)
        if path == '/':
            break
    if path != '/':
        return path


def lazy_filename(text, ext=''):
    """Return a filename string for the given text and optional extension (ext)

    - http://stackoverflow.com/a/7406369
    """
    # Strip out and replace some things in case text is a url
    text = text.split('://')[-1].strip('/').replace('/', '--')
    ext = '.{}'.format(ext) if ext else ''

    return "".join([
        c
        for c in text
        if c.isalpha() or c.isdigit() or c in (' ', '-', '_', '+', '.')
    ]).rstrip().replace(' ', '-') + ext


def get_logger(module_name,
               logdir=DEFAULT_LOG_DIR,
               file_format='%(asctime)s - %(levelname)s - %(funcName)s: %(message)s',
               stream_format='%(asctime)s: %(message)s',
               file_level=logging.DEBUG,
               stream_level=logging.INFO):
    """Return a logger object with a file handler and stream/console handler

    - file_format: used for logging file handler; if empty string or None,
      don't use a file handler
    - stream_format: used for logging stream/console handler; if empty string
      or None, don't use a stream handler
    - file_level: logging level for file handler
    - stream_level: logging level for stream/console handler

    Note: file_level and stream_level params also accepts strings (critical,
    error, warning, info, debug, notset) and ints (50, 40, 30, 20, 10, 0)
    """
    assert file_format or stream_format, 'Must supply a file_format or stream_format'
    file_level = file_level if type(file_level) == int else LOG_LEVELS.get(file_level.lower(), 0)
    stream_level = stream_level if type(stream_level) == int else LOG_LEVELS.get(stream_level.lower(), 0)

    logger = logging.getLogger(module_name)
    logger.setLevel(logging.DEBUG)
    if file_format:
        logdir = abspath(logdir)
        if not os.path.isdir(logdir):
            try:
                makedirs(logdir)
            except(PermissionError, OSError):
                try:
                    makedirs(ALTERNATE_LOG_DIR)
                except FileExistsError:
                    logdir = ALTERNATE_LOG_DIR
                except(PermissionError, OSError):
                    msg = 'Cannot create {} or {}'.format(repr(logdir), repr(ALTERNATE_LOG_DIR))
                    raise Exception(msg)
                else:
                    logdir = ALTERNATE_LOG_DIR
        if not access(logdir, W_OK):
            if logdir != ALTERNATE_LOG_DIR:
                try:
                    makedirs(ALTERNATE_LOG_DIR)
                except FileExistsError:
                    msg = 'Cannot write in {} and cannot write in existing {}'.format(
                        repr(logdir), repr(ALTERNATE_LOG_DIR)
                    )
                    assert access(ALTERNATE_LOG_DIR, W_OK), msg
                    logdir = ALTERNATE_LOG_DIR
                except(PermissionError, OSError):
                    msg = 'Cannot write in {} and cannot create {}'.format(
                        repr(logdir), repr(ALTERNATE_LOG_DIR)
                    )
                    raise Exception(msg)
                else:
                    logdir = ALTERNATE_LOG_DIR
            else:
                msg = 'Cannot write in {}'.format(repr(logdir))
                raise Exception(msg)
        logfile = os.path.join(logdir, '{}.log'.format(module_name.replace('_', '-')))
        file_handler = logging.FileHandler(logfile, mode='a')
        file_handler.setLevel(file_level)
        file_handler.setFormatter(logging.Formatter(file_format))
        logger.addHandler(file_handler)
    if stream_format:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(stream_level)
        console_handler.setFormatter(logging.Formatter(stream_format))
        logger.addHandler(console_handler)
    return logger


def get_logfile_from_logger(logger):
    """Return the path to the log file if there is a logging.FileHandler"""
    try:
        file_handlers = [h for h in logger.handlers if type(h) == logging.FileHandler]
    except:
        pass
    else:
        if file_handlers:
            return file_handlers[0].baseFilename


def sha256sum(filepath):
    """Return the SHA256 checksum for specified file"""
    with open(abspath(filepath), 'rb') as fp:
        digest = sha256(fp.read()).hexdigest()
    return digest


def wait_for_write_age(filepath, age=10, sleeptime=1, verbose=False):
    """Wait until it has been age seconds since filepath was written to

    - sleeptime: number of seconds to sleep before each check
    - verbose: if True, print a message before each sleep
    """
    filepath = abspath(filepath)
    since_last_write = time.time() - os.path.getmtime(filepath)
    while since_last_write < age:
        if verbose:
            print('Only been {} seconds since last write to {}'.format(since_last_write, filepath))
        time.sleep(sleeptime)
        since_last_write = time.time() - os.path.getmtime(filepath)
    return True


def wait_for_empty_directory(dirpath, sleeptime=1, verbose=False):
    """Wait until the specified dirpath contains no files

    - sleeptime: number of seconds to sleep before each check
    - verbose: if True, print a message before each sleep
    """
    dirpath = abspath(dirpath)
    if os.path.isdir(dirpath):
        num_files = len(os.listdir(dirpath))
        while num_files > 0:
            if verbose:
                print('There are still {} files in {}.'.format(num_files, dirpath))
            time.sleep(sleeptime)
            num_files = len(os.listdir(dirpath))
        return True
