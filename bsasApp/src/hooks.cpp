
#include <fstream>

#include <initHooks.h>
#include <iocsh.h>
#include <epicsExit.h>
#include <drvSup.h>
#include <epicsStdio.h>

#include <pv/pvAccess.h>
#include <pva/client.h>
#include <pv/reftrack.h>

#include "collect_ca.h"
#include "collector.h"
#include "receiver_pva.h"
#include "coordinator.h"

#include <epicsExport.h>

namespace pvd = epics::pvData;
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
        /* lvl<=0 shows only table names
         * lvl==1 shows only PV w/ overflows
         * lvl==2 shows only PV w/ overflows or disconnected
         * lvl>=3 shows all
         *
         */
        for(coordinators_t::const_iterator it(coordinators.begin()), end(coordinators.end()); it!=end; ++it) {
            epicsStdoutPrintf("Table %s\n", it->first.c_str());

            std::tr1::shared_ptr<const Coordinator> coord(it->second);

            Guard G(coord->mutex);
            if(!coord.get()) continue;

            epicsStdoutPrintf("    Overflows=%zu Complete=%zu\n", coord->collector->nOverflow, coord->collector->nComplete);
            if(lvl<1) continue;

            // holding Coordinator::mutex prevents signal list change.

            for(size_t i=0, N=coord->collector->pvs.size(); i<N; i++) {
                if(!coord->collector->pvs[i].sub) continue;

                const Subscription* sub = coord->collector->pvs[i].sub.get();

                Guard G2(sub->mutex); // mutex order: Coordinator::mutex -> Subscription::mutex

                if(lvl<2 && sub->nOverflows==0) continue;
                if(lvl<3 && !sub->connected) continue;

                epicsStdoutPrintf("  %s\t %zu/%zu conn=%c #dis=%zu #err=%zu #up=%zu #MB=%.1f #oflow=%zu\n",
                                  sub->pvname.c_str(),
                                  sub->values.size(),
                                  sub->limit,
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

            coord->collector->nOverflow = 0u;
            coord->collector->nComplete = 0u;

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

extern "C"
void bsasTableSet(const char *name, const char *filename)
{
    try {
        pvd::shared_vector<std::string> signals;
        {
            std::ifstream strm(filename);

            if(!strm.is_open()) {
                fprintf(stderr, "Unable to open: %s\n", filename);
                return;
            }

            std::string line;
            while(std::getline(strm, line)) {
                size_t prefix = line.find_first_not_of(" \t");
                size_t suffix = line.find_last_not_of(" \t");

                if(prefix>=line.size() || prefix>suffix) {
                    continue; // blank line
                } else if(line.at(prefix)=='#') {
                    continue; // comment
                }

                signals.push_back(line.substr(prefix, suffix-prefix+1));
            }

            if(!strm.eof()) {
                fprintf(stderr, "Error processing: %s\n", filename);
                return;
            }
        }

        pvac::ClientProvider ctxt("server:bsas");

        ctxt.connect(name)
            .put()
            .set("value", pvd::freeze(signals))
            .exec();

    }catch(std::exception& e) {
        fprintf(stderr, "Error: %s\n", e.what());
    }
}

/* bsasTableSet */
static const iocshArg bsasTableSetArg0 = { "pvname", iocshArgString};
static const iocshArg bsasTableSetArg1 = { "filename", iocshArgString};
static const iocshArg * const bsasTableSetArgs[] = {&bsasTableSetArg0, &bsasTableSetArg1};
static const iocshFuncDef bsasTableSetFuncDef = {
    "bsasTableSet",2,bsasTableSetArgs};
static void bsasTableSetCallFunc(const iocshArgBuf *args)
{
    bsasTableSet(args[0].sval, args[1].sval);
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
    iocshRegister(&bsasTableSetFuncDef, bsasTableSetCallFunc);
    initHookRegister(&bsasHook);
}

extern "C" {
epicsExportRegistrar(bsasRegistrar);
epicsExportAddress(drvet, bsas);
}
