#!/usr/bin/env python

import logging

from p4p.client.thread import Context

_log = logging.getLogger(__name__)

def getargs():
    from argparse import ArgumentParser
    P = ArgumentParser()
    P.add_argument('pvname', help='SIGS pvname (eg. RX:SIG')
    P.add_argument('filename', help='list of BSA/signal PV names.  text, one per line')
    P.add_argument('-v', '--verbose', action='store_const', const=logging.DEBUG, default=logging.INFO)
    return P.parse_args()

def main(args):
    sigs = []
    with open(args.filename, 'r') as F:
        for line in F:
            line = line.strip()
            if len(line)==0 or line[:1]=='#':
                continue
            _log.debug("Read signal '%s'", line)
            sigs.append(line)

    with Context('pva') as ctxt:
        ctxt.put(args.pvname, sigs, wait=True)
        print("Success.  Signal list now")
        for sig in ctxt.get(args.pvname):
            print(sig)

if __name__=='__main__':
    args = getargs()
    logging.basicConfig(level=args.verbose)
    main(args)
