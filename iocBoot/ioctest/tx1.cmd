#!../../bin/linux-x86_64/bsas

## Register all support components
dbLoadDatabase("../../dbd/bsas.dbd",0,0)
bsas_registerRecordDeviceDriver(pdbbase) 

dbLoadRecords("tx1.db", "P=TX:")

iocInit()
