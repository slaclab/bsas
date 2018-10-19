
#include <initHooks.h>
#include <iocsh.h>
#include <epicsExit.h>
#include <drvSup.h>
#include <epicsStdio.h>

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

void bsas_report(int lvl)
{
    try {
        for(coordinators_t::const_iterator it(coordinators.begin()), end(coordinators.end()); it!=end; ++it) {
            printf("Table %s\n", it->first.c_str());
            if(lvl<1) continue;

            std::tr1::shared_ptr<const Coordinator> coord(it->second);

            Guard G(coord->mutex);
            if(!coord.get()) continue;

            // holding Coordinator::mutex prevents signal list change.

            for(size_t i=0, N=coord->collector->pvs.size(); i<N; i++) {
                if(!coord->collector->pvs[i].sub) continue;

                const Subscription* sub = coord->collector->pvs[i].sub.get();

                // intentionaly not locking to avoid slowing down collection
                printf("  %s\t conn=%c #dis=%zu #err=%zu #up=%zu #MB=%.1f #oflow=%zu\n",
                       sub->pvname.c_str(),
                       sub->connected?'Y':'_',
                       sub->nDisconnects,
                       sub->nErrors,
                       sub->nUpdates,
                       sub->nUpdateBytes/1048576.0,
                       sub->nOverflows);
            }
        }

    }catch(std::exception& e) {
        fprintf(stderr, "Error: %s\n", e.what());
    }
}

drvet bsas = {
    2,
    (DRVSUPFUN)&bsas_report,
    NULL,
};

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

extern "C"
void bsasStatReset(const char *name)
{
    try {
        for(coordinators_t::const_iterator it(coordinators.begin()), end(coordinators.end()); it!=end; ++it) {
            if(name && it->first!=name) continue;
            std::tr1::shared_ptr<const Coordinator> coord(it->second);

            Guard G(coord->mutex);
            if(!coord.get()) continue;

            for(size_t i=0, N=coord->collector->pvs.size(); i<N; i++) {
                if(!coord->collector->pvs[i].sub) continue;

                Subscription* sub = coord->collector->pvs[i].sub.get();

                Guard G2(sub->mutex); // establishes mutex order Coordinator::mutex -> Subscription::mutex

                sub->nDisconnects = sub->lDisconnects = 0u;
                sub->nErrors = sub->lErrors = 0u;
                sub->nUpdates = sub->lUpdates = 0u;
                sub->nUpdateBytes = sub->lUpdateBytes = 0u;
                sub->nOverflows = sub->lOverflows = 0u;
            }
        }

    }catch(std::exception& e) {
        fprintf(stderr, "Error: %s\n", e.what());
    }
}

/* bsasStatReset */
static const iocshArg bsasStatResetArg0 = { "prefix", iocshArgString};
static const iocshArg * const bsasStatResetArgs[] = {&bsasStatResetArg0};
static const iocshFuncDef bsasStatResetFuncDef = {
    "bsasStatReset",1,bsasStatResetArgs};
static void bsasStatResetCallFunc(const iocshArgBuf *args)
{
    bsasStatReset(args[0].sval);
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
    iocshRegister(&bsasStatResetFuncDef, bsasStatResetCallFunc);
    initHookRegister(&bsasHook);
}

extern "C" {
epicsExportRegistrar(bsasRegistrar);
epicsExportAddress(drvet, bsas);
}
