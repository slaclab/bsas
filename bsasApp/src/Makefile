TOP=../..

include $(TOP)/configure/CONFIG
#----------------------------------------
#  ADD MACRO DEFINITIONS AFTER THIS LINE
#=============================

PROD_SRCS += collector.cpp
PROD_SRCS += collect_ca.cpp
PROD_SRCS += receiver_pva.cpp
PROD_SRCS += coordinator.cpp


PROD_IOC = bsas
DBD += bsas.dbd

bsas_DBD += base.dbd
bsas_DBD += system.dbd
bsas_DBD += PVAServerRegister.dbd
bsas_DBD += qsrv.dbd
bsas_DBD += bsasSupport.dbd

bsas_SRCS += bsasMain.cpp
bsas_SRCS += bsas_registerRecordDeviceDriver.cpp
bsas_SRCS += hooks.cpp

PROD_HOST += test_collector
test_collector_SRCS += test_collector.cpp
TESTS += test_collector

PROD_HOST += test_receiver
test_receiver_SRCS += test_receiver.cpp
TESTS += test_receiver

PROD_LIBS += qsrv
PROD_LIBS += $(EPICS_BASE_PVA_CORE_LIBS)
PROD_LIBS += $(EPICS_BASE_IOC_LIBS)

TESTSCRIPTS_HOST += $(TESTS:%=%.t)

#===========================

include $(TOP)/configure/RULES
#----------------------------------------
#  ADD RULES AFTER THIS LINE

