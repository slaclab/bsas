#ifndef COORDINATOR_H
#define COORDINATOR_H

#include "coordinator.h"
#include "receiver_pva.h"

struct Coordinator
{
    static size_t num_instances;

    static Coordinator* lookup(const std::string&);

    Coordinator(CAContext& ctxt, pvas::StaticProvider& provider, const std::string& prefix);
    ~Coordinator();

    CAContext& ctxt;
    pvas::StaticProvider& provider;
    const std::string prefix;

    epics::auto_ptr<Collector> collector;
    epics::auto_ptr<PVAReceiver> table_receiver;

    pvas::SharedPV::shared_pointer pv_signals,
                                   pv_status;

    epics::pvData::PVStructurePtr root_status;

    epics::pvData::Thread handler;

    Collector::names_t signals;
    bool signals_changed;

    epicsMutex mutex;
    bool running;
    epicsEvent wakeup;

    void handle();

    struct SignalsHandler : public pvas::SharedPV::Handler {
        const std::tr1::weak_ptr<Coordinator> coordinator;
        SignalsHandler(const std::tr1::shared_ptr<Coordinator>& coordinator) :coordinator(coordinator) {}
        virtual void onPut(const pvas::SharedPV::shared_pointer& pv, pvas::Operation& op);
    };
};

#endif // COORDINATOR_H
