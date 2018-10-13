
#include <initHooks.h>
#include <iocsh.h>
#include <epicsExit.h>

#include <pv/pvAccess.h>

#include "collect_ca.h"
#include "collector.h"
#include "receiver_pva.h"
#include "coordinator.h"

#include <epicsExport.h>

namespace pva = epics::pvAccess;

namespace {

std::vector<std::string> prefixes;

std::tr1::shared_ptr<CAContext> cactxt;
std::vector<std::tr1::shared_ptr<Coordinator> > coordinators;
pvas::StaticProvider::shared_pointer provider;

void bsasExit(void *)
{
    // enforce shutdown order
    // PVA server may still be running at this point

    provider->close(true); // disconnect any PVA clients

    coordinators.clear(); // joins workers, cancels CA subscriptions

    provider.reset(); // server may still be holding a ref., but drop this one anyway

    cactxt.reset(); // CA context shutdown
}

void bsasHook(initHookState state)
{
    if(state!=initHookAfterIocRunning) return;
    epicsAtExit(bsasExit, 0);

    // our private CA context
    // place a lower prio than the Collector workers
    cactxt.reset(new CAContext(epicsThreadPriorityMedium));

    for(size_t i=0; i<prefixes.size(); i++) {
        std::tr1::shared_ptr<Coordinator> C(new Coordinator(*cactxt, *provider, prefixes[i]));
        std::tr1::shared_ptr<Coordinator::SignalsHandler> H(new Coordinator::SignalsHandler(C));
        C->pv_signals->setHandler(H);
        coordinators.push_back(C);
    }
}

} // namespace

extern "C"
void bsasTableAdd(const char *prefix)
{
    prefixes.push_back(prefix);
}

/* bsasTableAdd */
static const iocshArg bsasTableAddArg0 = { "prefix", iocshArgString};
static const iocshArg * const bsasTableAddArgs[] = {&bsasTableAddArg0};
static const iocshFuncDef bsasTableAddFuncDef = {
    "bsasTableAdd",1,bsasTableAddArgs};
static void bsasTableAddCallFunc(const iocshArgBuf *args)
{
    bsasTableAdd(args[0].sval);
}

static void bsasRegistrar()
{

    // register our (empty) provider before the PVA server is started

    provider.reset(new pvas::StaticProvider("bsas"));

    pva::ChannelProviderRegistry::servers()->addSingleton(provider->provider());

    iocshRegister(&bsasTableAddFuncDef, bsasTableAddCallFunc);
    initHookRegister(&bsasHook);
}

extern "C" {
epicsExportRegistrar(bsasRegistrar);
}
