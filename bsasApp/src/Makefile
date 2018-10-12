TOP=../..

include $(TOP)/configure/CONFIG
#----------------------------------------
#  ADD MACRO DEFINITIONS AFTER THIS LINE
#=============================

#=============================
# Build the IOC application

PROD_IOC = bsas
# bsas.dbd will be created and installed
DBD += bsas.dbd

# bsas.dbd will be made up from these files:
bsas_DBD += base.dbd

# Include dbd files from all support applications:
#bsas_DBD += xxx.dbd

# Add all the support libraries needed by this IOC
#bsas_LIBS += xxx

# bsas_registerRecordDeviceDriver.cpp derives from bsas.dbd
bsas_SRCS += bsas_registerRecordDeviceDriver.cpp

# Build the main IOC entry point on workstation OSs.
bsas_SRCS_DEFAULT += bsasMain.cpp
bsas_SRCS_vxWorks += -nil-

# Add support from base/src/vxWorks if needed
#bsas_OBJS_vxWorks += $(EPICS_BASE_BIN)/vxComLibrary

# Finally link to the EPICS Base libraries
bsas_LIBS += $(EPICS_BASE_IOC_LIBS)

#===========================

include $(TOP)/configure/RULES
#----------------------------------------
#  ADD RULES AFTER THIS LINE

