#!/usr/bin/env python

from __future__ import division, print_function, unicode_literals

import time
import signal
import sys
import os
import platform
import errno
import logging
import threading

import numpy
import h5py

from p4p.client.thread import Context, Disconnected

_log = logging.getLogger(__name__)

filefmt = '%%s_%Y%m%d_%H%M%S.h5'

# arbitrarily force switch to a new file after # rows exceeds
maxrows = 120*60*60*2 # ~2 hours @120Hz

def getargs():
    from argparse import ArgumentParser
    A = ArgumentParser(description="""Poor man's BSAS archiver.

Run with *TBL PV name, and prefix for output files.

Switches to a new file on SIGHUP or SIGUSR1.
Graceful exit on SIGINT.
""")
    A.add_argument('pvname')
    A.add_argument('file_prefix')
    A.add_argument('-G', '--group', default='/', help='Store under H5 Group.  Default "/"')
    A.add_argument('-C', '--compress', action='store_true', default=False)
    A.add_argument('-v', '--verbose', action='store_const', const=logging.DEBUG, default=logging.INFO)
    return A.parse_args()

def matheader(fname):
    """The magic header used to distinguish Matlab .mat files from
    everyday ordinary run of the mill HDF5 files.

    https://pythonhosted.org/hdf5storage/storage_format.html#matlab-file-header
    """
    I = {}
    I['major'], I['minor'], I['micro'] = sys.version_info[:3]

    H = 'MATLAB 7.3 MAT-file, Platform: CPython %(major)d.%(minor)d.%(micro)d, Created on: %%a %%b %%d %%H:%%M:%%S %%Y HDF5 schema 1.00 .'%I
    H = time.strftime(H).encode('ascii')
    assert len(H)<(128-12), repr(H)

    H = H + b' '*(128-12-len(H)) + b'\0\0\0\0\0\0\0\0\x00\x02\x49\x4d'
    assert len(H)==128, repr(H)

    # we are writing into the userblock, which is assumed to be at least 128 bytes long.
    # we ensure this below when creating new files.
    with open(fname, 'r+b') as F:
        F.write(H)

# values for the magic 'MATLAB_class' attribute on datasets
_mat_class = {
    numpy.dtype('f4'): numpy.string_('single'),
    numpy.dtype('f8'): numpy.string_('double'),
    numpy.dtype('u1'): numpy.string_('uint8'),
    numpy.dtype('u2'): numpy.string_('uint16'),
    numpy.dtype('u4'): numpy.string_('uint32'),
    numpy.dtype('u8'): numpy.string_('uint64'),
    numpy.dtype('i1'): numpy.string_('int8'),
    numpy.dtype('i2'): numpy.string_('int16'),
    numpy.dtype('i4'): numpy.string_('int32'),
    numpy.dtype('i8'): numpy.string_('int64'),
    # TODO: bool and some string types
}

class TableWriter(object):
    context = Context('pva', unwrap=False)

    def __init__(self, args):
        self.pv = args.pvname
        self.fbase = args.file_prefix
        self.group = args.group
        self.compress = args.compress

        self.lock = threading.Lock() # guards open HDF5 file, and our attributes

        self.initial = True
        self.prevstart = None

        self.F, self.G = None, None # h5py.File and h5py.Group

        _log.info("Create subscription")
        self.S = self.context.monitor(self.pv, self._update, request='field()record[pipeline=True]', notify_disconnect=True)

    def close(self):
        _log.info("Close subscription")
        self.S.close()
        self.flush()

    def _update(self, val):
        start, prevstart = time.time(), self.prevstart
        self.prevstart = start

        _log.debug('Update')
        with self.lock:
            self.__update(val)

        if prevstart is None:
            return

        end = time.time()

        interval = start-prevstart # >= the server update interval (based on previous update)
        dT = end-start # our processing time for this update
        if dT >= interval*0.75:
            _log.warn("Processing time %.2f approches threshold %.2f", dT, interval)
        else:
            _log.debug("Processing time %.2f, threshold %.2f", dT, interval)

    def __update(self, val): # self.lock is locked

        if isinstance(val, Disconnected):
            _log.warn("Table PV disconnect")
            self.initial = True
            self.flush()
            return

        elif self.initial:
            _log.warn("Table PV (re)connect")
            self.initial = False
            return # ignore initial update

        elif self.F is None:
            self.open() # lazy (re)open on first update


        for fld, lbl in zip(val.value.keys(), val.labels):
            V = val.value[fld]
            new, = V.shape
            try:
                D = self.G[fld]
            except KeyError:
                kws = {}
                if self.compress:
                    kws['compression'] = 'gzip'
                    kws['shuffle'] = True
                D = self.G.create_dataset(fld, dtype=V.dtype,
                                          shape=(0, 1), chunks=None, maxshape=(None, 1),
                                          **kws)
                D.attrs['label'] = lbl
                D.attrs['MATLAB_class'] = _mat_class[V.dtype]

            cur, _one = D.shape
            D.resize((cur+new, 1))
            D[cur:, 0] = V # copy

        self.F.flush() # flush this update to disk

        if D.shape[0] > maxrows:
            # close file
            self.flush()

    def flush(self):
        if self.F is not None:
            _log.info('Close and flush')
            self.F.flush()
            self.F.close()
        self.F, self.G = None, None

    def open(self):
        self.flush()
        fname = time.strftime(filefmt)%self.fbase
        _log.info('Open "%s"', fname)

        F = h5py.File(fname, 'w-', userblock_size=512) # error if already exists
        assert F.userblock_size>=128, F.userblock_size
        F.close()
        matheader(fname)
        self.F = h5py.File(fname, 'r+') # error if not exists

        self.G = self.F.require_group(self.group)

    def __enter__(self):
        return self
    def __exit__(self, A,B,C):
        self.close()

class SigWake(object):
    def __enter__(self):
        self._R, self._W = os.pipe()
        self.prevHUP = signal.signal(signal.SIGHUP, self._wake)
        self.prevUSR1 = signal.signal(signal.SIGUSR1, self._wake)
        return self

    def __exit__(self, A,B,C):
        signal.signal(signal.SIGHUP, self.prevHUP)
        signal.signal(signal.SIGUSR1, self.prevUSR1)
        os.close(self._R)
        os.close(self._W)

    def _wake(self, num, frame):
        os.write(self._W, '!')

    def wait(self):
        try:
            os.read(self._R, 1)
        except OSError as e:
            if e.errno!=errno.EINTR:
                raise

        signal.signal(signal.SIGHUP, self._wake)
        signal.signal(signal.SIGUSR1, self._wake)

def set_proc_name(newname):
    if platform.system()!='Linux':
        _log.warn("Don't know how to set process name")
        return
    newname = newname[:15]
    from ctypes import cdll, byref, create_string_buffer
    libc = cdll.LoadLibrary(None)
    buff = create_string_buffer(len(newname)+1)
    buff.value = newname
    libc.prctl(15, byref(buff), 0, 0, 0) # PR_SET_NAME=15 on Linux

def main(args):
    with TableWriter(args) as W:
        _log.info("Running")
        try:
            with SigWake() as S:
                while True:
                    S.wait()
                    with W.lock:
                        W.flush()
        except KeyboardInterrupt:
            pass
        _log.info("Done")

if __name__=='__main__':
    set_proc_name('h5tablewriter')
    args = getargs()
    logging.basicConfig(level=args.verbose)
    main(args)
