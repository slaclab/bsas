
#include <initHooks.h>
#include <iocsh.h>
#include <epicsExit.h>

#include <pv/pvAccess.h>
#include <pv/reftrack.h>

#include "collect_ca.h"
#include "collector.h"
#include "receiver_pva.h"
#include "coordinator.h"

#include <epicsExport.h>

namespace pva = epics::pvAccess;

namespace {

std::tr1::shared_ptr<CAContext> cactxt;

// static after iocInit()
typedef std::map<std::string, std::tr1::shared_ptr<Coordinator> > coordinators_t;
coordinators_t coordinators;

pvas::StaticProvider::shared_pointer provider;

bool locked;

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
    if(state==initHookAtBeginning) locked = true;
    if(state!=initHookAfterIocRunning) return;
    epicsAtExit(bsasExit, 0);

    // our private CA context
    // place a lower prio than the Collector workers
    cactxt.reset(new CAContext(epicsThreadPriorityMedium));

    for(coordinators_t::iterator it(coordinators.begin()), end(coordinators.end()); it!=end; ++it) {
        std::tr1::shared_ptr<Coordinator> C(new Coordinator(*cactxt, *provider, it->first));
        std::tr1::shared_ptr<Coordinator::SignalsHandler> H(new Coordinator::SignalsHandler(C));
        C->pv_signals->setHandler(H);
        it->second = C;
    }
}

} // namespace

Coordinator* Coordinator::lookup(const std::string& name)
{
    coordinators_t::iterator it(coordinators.find(name));
    return it==coordinators.end() ? 0 : it->second.get();
}

extern "C"
void bsasTableAdd(const char *prefix)
{
    if(locked) {
        printf("Not allowed after iocInit()\n");
    } else {
        coordinators[prefix] = std::tr1::shared_ptr<Coordinator>();
    }
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
    epics::registerRefCounter("DBRValue", &DBRValue::Holder::num_instances);
    epics::registerRefCounter("CAContext", &CAContext::num_instances);
    epics::registerRefCounter("Subscription", &Subscription::num_instances);
    epics::registerRefCounter("Collector", &Collector::num_instances);
    epics::registerRefCounter("Coordinator", &Coordinator::num_instances);
    epics::registerRefCounter("PVAReceiver", &PVAReceiver::num_instances);

    // register our (empty) provider before the PVA server is started

    provider.reset(new pvas::StaticProvider("bsas"));

    pva::ChannelProviderRegistry::servers()->addSingleton(provider->provider());

    iocshRegister(&bsasTableAddFuncDef, bsasTableAddCallFunc);
    initHookRegister(&bsasHook);
}

extern "C" {
epicsExportRegistrar(bsasRegistrar);
}
