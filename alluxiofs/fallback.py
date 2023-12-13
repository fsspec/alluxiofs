import logging
import os

import fsspec
from fsspec import AbstractFileSystem, filesystem

class FallbackFileSystem(AbstractFileSystem):
    protocol = "fallback"
    
    def __init__(
        self,
        target_protocol=None,
        target_options=None,
        fs=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if fs is None and target_protocol is None:
            raise ValueError(
                "Please provide filesystem instance(fs) or target_protocol"
            )
        self.kwargs = target_options or {}
        self.fs = fs if fs is not None else filesystem(target_protocol, **self.kwargs)

        def _strip_protocol(path):
            # acts as a method, since each instance has a difference target
            return self.fs._strip_protocol(type(self)._strip_protocol(path))

        self._strip_protocol: Callable = _strip_protocol

    def mkdir(self, *args, **kwargs):
        self.fs.mkdir(*args, **kwargs)

    def rmdir(self, *args, **kwargs):
        self.fs.rmdir(*args, **kwargs)
    
    def ls(self, *args, **kwargs): #NEED
        return self.fs.ls(*args, **kwargs)

    def copy(self, *args, **kwargs):
        self.fs.copy(*args, **kwargs)

    def mv(self, *args, **kwargs):
        self.fs.mv(*args, **kwargs)
    
    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        return self.fs._open(
            path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_options=cache_options,
            **kwargs,
        )

    def fetch_range(self, path, mode, start, end):
         return self.fs.fetch_range(path, mode, start, end)
    
    def __getattr__(self, name):
        """
        Delegate attribute access to the underlying filesystem object.
        """
        return getattr(self.fs, name)
        