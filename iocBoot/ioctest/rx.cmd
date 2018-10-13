#!../../bin/linux-x86_64/bsas

## Register all support components
dbLoadDatabase("../../dbd/bsas.dbd",0,0)
bsas_registerRecordDeviceDriver(pdbbase) 

var(collectorCaDebug, 1)
var(collectorDebug, 1)
var(receiverPVADebug, 1)

bsasTableAdd("RX:")

iocInit()
