#!/usr/bin/env python

from __future__ import division, print_function, unicode_literals

import time
import signal
import sys
import os
import shutil
import platform
import errno
import select
import logging
import threading

try:
    from ConfigParser import SafeConfigParser as _ConfigParser, NoOptionError
    class ConfigParser(_ConfigParser):
        # enable dict-like access as with py3
        class SectionProxy(object):
            def __init__(self, conf, sect):
                self.conf, self.sect = conf, sect
            def __getitem__(self, key):
                try:
                    return self.conf.get(self.sect, key)
                except NoOptionError:
                    raise KeyError(key)
            def get(self, key, defval=None):
                try:
                    return self.conf.get(self.sect, key)
                except NoOptionError:
                    return defval

        def __getitem__(self, section):
            if section!='DEFAULT' and not self.has_section(section):
                raise KeyError(section)
            return self.SectionProxy(self, section)

except ImportError:
    from configparser import SafeConfigParser as ConfigParser

import numpy
import h5py

from p4p.client.thread import Context, Disconnected

_log = logging.getLogger(__name__)

ref_dtype = h5py.special_dtype(ref=h5py.Reference)

def getargs():
    from argparse import ArgumentParser
    A = ArgumentParser(description="""Poor man's BSAS archiver.

Run with *TBL PV name, and prefix for output files.

Switches to a new file on SIGHUP or SIGUSR1.
Graceful exit on SIGINT.

Writes HDF5 files which attempt to be MAT 7.3 compatible.
""")
    A.add_argument('conf', metavar='FILE', help='configuration file')
    A.add_argument('--section', metavar='NAME', default='DEFAULT', help='configure file section to use')
    A.add_argument('-v', '--verbose', action='store_const', const=logging.DEBUG, default=logging.INFO)
    A.add_argument('-q', '--quiet', action='store_const', const=logging.WARN, dest='verbose')
    A.add_argument('-C', '--check', action='store_true', default=False, help="Exit after reading configuration file")
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

    def __init__(self, conf, wakeup=None, check=False):
        self._wakeup = wakeup

        # pull out mandatory config items now

        self.pv = conf['tablePV']

        self.ftemplate = conf['outfile'] # passed through time.strftime()

        self.ftemp = conf.get('scratch', '/tmp/bsas_%s.h5'%self.pv)

        self.temp_limit = int( float(conf.get('temp_limit', '0'))*(2**30) ) # in bytes
        if self.temp_limit <= 0:
            stat = os.statvfs(os.path.dirname(self.ftemp))
            # by default, limit ourself to a fraction of the FS capacity
            self.temp_limit = int(stat.f_frsize*stat.f_blocks*0.25)

        self.temp_period = float(conf.get('temp_period', '60'))*60.0 # in sec.

        self.group = conf.get('file_group', '/')

        if check:
            raise KeyboardInterrupt()

        # guards open HDF5 file, and our attributes.
        # serialize main thread and PVA worker
        self.lock = threading.Lock()

        self.nextref = 0

        self.initial = True
        self.prevstart = None

        self.F, self.G = None, None # h5py.File and h5py.Group

        self._migrate = None

        _log.info("Create subscription")
        self.S = self.context.monitor(self.pv, self._update, request='field()record[pipeline=True]', notify_disconnect=True)

    def close(self): # self.lock is locked
        _log.info("Close subscription")
        self.S.close()
        _log.info("Final flush")
        self.flush(force=True)
        if self._migrate is not None:
            _log.info("Wait for final migration")
            self._migrate.join()
            _log.info("final migration complete")

    def _update(self, val):
        # called from PVA worker only
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
            _log.warn("Processing time %.2f approaches threshold %.2f", dT, interval)
        else:
            _log.info("Processing time %.2f, threshold %.2f", dT, interval)

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

            if isinstance(V, numpy.ndarray):
                new, = V.shape
                try:
                    D = self.G[fld]
                except KeyError:
                    D = self.G.create_dataset(fld, dtype=V.dtype,
                                            shape=(0, 1), chunks=None, maxshape=(None, 1),
                                            shuffle=True, compression='gzip')
                    D.attrs['label'] = lbl
                    D.attrs['MATLAB_class'] = _mat_class[V.dtype]

                cur, _one = D.shape
                D.resize((cur+new, 1))
                D[cur:, 0] = V # copy

            elif isinstance(V, list): # union[]
                # store as cell array
                try:
                    D = self.G[fld]
                except KeyError:
                    D = self.G.create_dataset(fld, dtype=ref_dtype,
                                              shape=(0, 1), chunks=None, maxshape=(None, 1))
                    D.attrs['label'] = lbl
                    D.attrs['MATLAB_class'] = numpy.string_("cell")

                refs = []
                _refs_ = self.G.require_group('#refs#')
                _path = numpy.string_(_refs_.name.encode('ascii'))
                # placeholder for empty cells
                try:
                    null = _refs_['null']
                except KeyError:
                    null = _refs_.create_dataset('null', data=numpy.asarray([0,1], dtype='u8'))
                    null.attrs['MATLAB_class'] = numpy.string_('double')
                    null.attrs['H5PATH'] = _path
                    null.attrs['MATLAB_empty'] = numpy.asarray(1, dtype='u1')

                for img in V:
                    if img is None:
                        refs.append(null.ref)

                    else:
                        dset = _refs_.create_dataset('cellval%d'%self.nextref, data=img,
                                                    shuffle=True, compression='gzip', compression_opts=9)
                        dset.attrs['MATLAB_class'] = _mat_class[img.dtype]
                        dset.attrs['H5PATH'] = _path
                        refs.append(dset.ref)
                        self.nextref += 1

                cur, _one = D.shape
                D.resize((cur+new, 1))
                D[cur:, 0] = refs

        self.F.flush() # flush this update to disk

        self.flush()

    def flush(self, force=False): # self.lock is locked
        if self.F is not None:
            self.F.flush()

            age = time.time()-self.F_time
            size = os.stat(self.ftemp).st_size

            if not force and age < self.temp_period and size < self.temp_limit:
                _log.info('Skip rotate, too new (%.2f < %.2f) or too small (%d < %d)', age, self.temp_period, size, self.temp_limit)
                return

            _log.info('Close and rotate')
            self.F.close()

        self.F, self.G = None, None

        if os.path.isfile(self.ftemp):
            if self._migrate is not None:
                # We only pipeline a single migration.
                # If this hasn't completed, then we stall until it has.
                self._migrate.join(0.01)
                if self._migrate.isAlive():
                    _log.warn("Flush stalls waiting for previous migration to complete.  Prepare for data loss!")
                    self._migrate.join()

                self._migrate = None
                _log.info("Previous migration complete")

            _log.info("Starting migration of '%s'", self.ftemp)

            stage2 = self.ftemp+'.tmp'
            if os.path.isfile(stage2):
                _log.error("Overwriting debris '%s' !", stage2)

            os.rename(self.ftemp, stage2)

            self._migrate = threading.Thread(name='BSAS Migration', target=self._movefile, args=(stage2,))
            self._migrate.start()

    def _movefile(self, stage2):
        # called from migration thread only
        finalpath = None
        try:
            start = time.time()
            # expand template with last mod time (instead of current time)
            mtime = os.stat(stage2).st_mtime

            finalpath = time.strftime(self.ftemplate, time.gmtime(mtime)) # expand using UTC

            if os.path.isfile(finalpath):
                _log.error("Migration destination '%s' already exists.  Prepare for data loss!")
                os.remove(finalpath)

            _log.info('Migrate %s -> %s', stage2, finalpath)
            try:
                os.makedirs(os.path.dirname(finalpath))
            except OSError:
                pass #if we failed, then the move will also fail
            shutil.move(stage2, finalpath)

            end = time.time()

            _log.info("Migration of '%s' complete after %.2f sec", finalpath, end-start)
        except:
            _log.exception("Failure during Migration of '%s' -> '%s'", stage2, finalpath)

    def open(self): # self.lock is locked
        self.flush()

        _log.info('Open "%s"', self.ftemp)

        with h5py.File(self.ftemp, 'w-', userblock_size=512) as F: # error if already exists
            assert F.userblock_size>=128, F.userblock_size

        matheader(self.ftemp)
        self.F = h5py.File(self.ftemp, 'r+') # error if not exists
        self.F_time = time.time()

        self.G = self.F.require_group(self.group)
        self.nextref = 0

    def __enter__(self):
        return self
    def __exit__(self, A,B,C):
        with self.lock:
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
        self.poke()

    def poke(self):
        os.write(self._W, '!')

    def wait(self, timeout=None):
        try:
            Rs, _Ws, _Es = select.select([self._R], [], [], timeout)
            if self._R in Rs:
                os.read(self._R, 1)
        except select.error:
            pass # assume EINTR
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

def main(conf):
    try:
        with SigWake() as S:
            with TableWriter(conf, check=args.check) as W:
                _log.info("Running")
                while True:
                    S.wait(W.temp_period/4.)
                    with W.lock:
                        W.flush()
    except KeyboardInterrupt:
        pass
    _log.info("Done")

if __name__=='__main__':
    # set process name to allow external tools like eg. logrotate
    # to force us to start a new file.
    #   killall -s SIGUSR1 h5tablewriter
    set_proc_name('h5tablewriter')
    args = getargs()
    conf = ConfigParser({
        'PWD':os.path.dirname(args.conf),
        'scratch':'/tmp/%(tablePV)s.h5',
    })
    with open(args.conf, 'r') as F:
        conf.readfp(F)
    logging.basicConfig(level=args.verbose)
    main(conf[args.section])
