#ifndef RECEIVER_PVA_H
#define RECEIVER_PVA_H

#include <pva/sharedstate.h>

#include "collector.h"

extern "C"
int bsasBackFill;

struct PVAReceiver : public Receiver
{
    static size_t num_instances;

    PVAReceiver(Collector& collector);
    virtual ~PVAReceiver();

    Collector& collector;
    const pvas::SharedPV::shared_pointer pv;

    epicsMutex mutex;

    enum state_t {
        NeedRetype,
        RetypeInProg,
        Run,
    } state;

    epicsEvent stateRun;

    struct ColCopy {
        PVAReceiver& receiver;
        explicit ColCopy(PVAReceiver& receiver) :receiver(receiver) {}
        virtual ~ColCopy() {}
        virtual void copy(const slices_t& s, size_t coln) =0;
    };

    struct Column {
        std::string fname;
        std::tr1::shared_ptr<ColCopy> copier;
        bool isarray;
        epics::pvData::ScalarType ftype;

        // last populated value, used to backfill
        DBRValue last;

        Column() :isarray(false), ftype(epics::pvData::pvDouble) {}
    };

    typedef std::vector<Column> columns_t;
    columns_t columns;

    epics::pvData::shared_vector<const std::string> labels;

    epics::pvData::PVStructurePtr root;
    epics::pvData::PVUIntArrayPtr fsec, fnsec;
    epics::pvData::BitSet changed;

    void close();

    virtual void names(const std::vector<std::string>& n);
    virtual void slices(const slices_t& s);
};

#endif // RECEIVER_PVA_H
