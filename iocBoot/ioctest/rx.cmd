#!../../bin/linux-x86_64/bsas

## Register all support components
dbLoadDatabase("../../dbd/bsas.dbd",0,0)
bsas_registerRecordDeviceDriver(pdbbase) 

var(collectorCaDebug, 1)
var(collectorDebug, 1)
var(receiverPVADebug, 1)

var(maxEventRate, 140.0)
var(collectorCaScalarMaxRate, 140.0)
var(collectorCaArrayMaxRate, 1.5)
var(bsasFlushPeriod, 2.0)

bsasTableAdd("RX:")

iocInit()
