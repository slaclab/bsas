TOP=../..

include $(TOP)/configure/CONFIG
#----------------------------------------
#  ADD MACRO DEFINITIONS AFTER THIS LINE
#=============================

#=============================
# Build the IOC application

LIBRARY = dspamSupport

dspamSupport_SRCS += spammer.cpp

DBD += dspamSupport.dbd

PROD_IOC = dspam
# dspam.dbd will be created and installed
DBD += dspam.dbd

# dspam.dbd will be made up from these files:
dspam_DBD += base.dbd
dspam_DBD += dspamSupport.dbd
dspam_LIBS += dspamSupport

# dspam_registerRecordDeviceDriver.cpp derives from dspam.dbd
dspam_SRCS += dspam_registerRecordDeviceDriver.cpp

# Build the main IOC entry point on workstation OSs.
dspam_SRCS_DEFAULT += dspamMain.cpp
dspam_SRCS_vxWorks += -nil-

# Add support from base/src/vxWorks if needed
#dspam_OBJS_vxWorks += $(EPICS_BASE_BIN)/vxComLibrary

# Finally link to the EPICS Base libraries
dspam_LIBS += $(EPICS_BASE_IOC_LIBS)

#===========================

include $(TOP)/configure/RULES
#----------------------------------------
#  ADD RULES AFTER THIS LINE

