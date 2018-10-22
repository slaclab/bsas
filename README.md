Beam Synchronous data Acquisition Service
=========================================

Consists of two components: collector and file writer.

Collector is build as `bin/*/bsas`.
Code entry points are in [bsasApp/src/hooks.cpp](bsasApp/src/hooks.cpp).
See [iocBoot/ioctest/rx.cmd](iocBoot/ioctest/rx.cmd) for example configuration.

File writer is [python/h5tablewriter.py](python/h5tablewriter.py).
See [iocBoot/ioctest/test.ini](iocBoot/ioctest/test.ini) for example configuration.

Requires
--------

* [epics-base](https://github.com/epics-base/epics-base)
* [pvDataCPP](https://github.com/epics-base/pvDataCPP)
* [pvAccessCPP](https://github.com/epics-base/pvAccessCPP)
* [p4p](https://github.com/epics-base/p4p)

Testing
-------

Run [iocBoot/ioctest/tx1.cmd](iocBoot/ioctest/tx1.cmd)
and [iocBoot/ioctest/tx2.cmd](iocBoot/ioctest/tx2.cmd).

```sh
$ pvput RX:SIG X TX:cnt{1,2,3,4}
$ pvget RX:STS
$ pvget RX:TBL
```
