from functools import wraps


__all__ = ["fake_serializable"]


class FakeSerializable(object):
    """
    This class is to make any derived class serialiable, overriding the __getstate__
    as saving init args and kwargs, and the __setstate__ as initializing the class with the saved args and kwargs.
    """
    def __getstate__(self):
        return self._init_args, self._init_kwargs

    def __setstate__(self, state):
        args, kwargs = state
        self.__init__(*args, **kwargs)


def capture_init_args(func):
    """
    A decorator that captures the init args and kwargs of a class, 
    and save them in the __getstate__ method
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        self._init_args = args
        self._init_kwargs = kwargs
        return func(self, *args, **kwargs)
    return wrapper


def fake_serializable(cls):
    """
    A class decorator that makes a class serializable, overriding the __getstate__
    as saving init args and kwargs, and the __setstate__ as initializing the class with the saved args and kwargs.
    """
    cls.__getstate__ = FakeSerializable.__getstate__
    cls.__setstate__ = FakeSerializable.__setstate__
    cls.__init__ = capture_init_args(cls.__init__)

    return cls