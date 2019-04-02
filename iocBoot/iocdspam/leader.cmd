#!../../bin/linux-x86_64/dspam

dbLoadDatabase("../../dbd/dspam.dbd",0,0)
dspam_registerRecordDeviceDriver(pdbbase) 

spamControllerCreate("controller", "224.0.0.128", "127.0.0.1")
dbLoadRecords("../../db/dspamController.db", "P=ctrl:,NAME=controller,CNT=cntA0")

spammerCreate("spam", "224.0.0.128", "127.0.0.1")
dbLoadRecords("../../db/dspamCounter100.db", "P=cntA,NAME=spam")

iocInit()
