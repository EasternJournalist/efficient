from hashlib import md5
import pickle
import shutil
from typing import Callable
import functools
from pathlib import Path
from numbers import Number
import warnings

CACHE_DIR = '.cache'


__all__ = ['disk_cache', 'set_cache_dir']


def set_cache_dir(path: str):
    global CACHE_DIR
    CACHE_DIR = path


def _as_string(x):
    if isinstance(x, str):
        if any(c in x for c in r'\/:*?"|<>'"'"):
            raise ValueError()
        return "'" + x + "'"
    elif isinstance(x, Number):
        return str(x)
    elif isinstance(x, list):
        return '[' + ','.join(map(_as_string, x)) + ']'
    elif isinstance(x, tuple):
        return '(' + ','.join(map(_as_string, x)) + ')'
    elif isinstance(x, set):
        return '{' + ','.join(map(_as_string, x)) + '}'
    elif isinstance(x, dict):
        return '{' + ','.join(f'{_as_string(k)}:{_as_string(v)}' for k, v in x.items()) + '}'
    else:
        raise ValueError()


def _as_string_args(args, kwargs):
    str_args = ','.join(_as_string(arg) for arg in args)
    str_kwargs = ','.join(f'{k}={_as_string(v)}' for k, v in kwargs.items())
    return ','.join(filter(None, [str_args, str_kwargs]))


def _get_path(func, args, kwargs):
    func_name = func.__module__ + '.' + func.__qualname__

    args_string = None
    try:
        args_string = _as_string_args(args, kwargs)
    except ValueError:
        pass
    if args_string is None or len(args_string) > 100:
        args_string = md5(pickle.dumps((args, kwargs))).hexdigest()
    cache_name = args_string + '.cache'

    cache_path = Path(func_name) / cache_name
    return cache_path


def disk_cache(func: Callable) -> Callable:
    """
    Decorator for caching a function's results to disk {CACHE_DIR}/{func_name}/{args_string}.cache
    If args_string is too long, use md5 hash instead.
    """
    @functools.wraps(func)
    def _cached_function(*args, **kwargs):
        cache_path = Path(CACHE_DIR) / _get_path(func, args, kwargs)

        if cache_path.exists():
            try:
                data = pickle.load(cache_path.open('rb'))
            except pickle.UnpicklingError:
                warnings.warn(f"Load cache failed, remove bad file：{cache_path}")
                shutil.rmtree(cache_path)
                data = func(*args, **kwargs)
            return data
        else:
            res = func(*args, **kwargs)
            cache_path.parent.mkdir(exist_ok=True, parents=True)
            pickle.dump(res, cache_path.open('wb'))
            return res
    return _cached_function
