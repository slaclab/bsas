#ifndef COLLECTOR_H
#define COLLECTOR_H

#include <vector>
#include <map>
#include <set>

#include <epicsTypes.h>
#include <epicsEvent.h>
#include <pv/thread.h>
#include <pv/sharedPtr.h>

#include "collect_ca.h"

struct Receiver {
    typedef std::vector<std::pair<epicsUInt64, std::vector<DBRValue> > > slices_t;
    virtual ~Receiver() {}
    virtual void names(const std::vector<std::string>& n) =0;
    virtual void slices(const slices_t& s) =0;
};

struct Collector
{
    static size_t num_instances;

    typedef epics::pvData::shared_vector<const std::string> names_t;

    explicit Collector(CAContext &ctxt,
                       const names_t& names,
                       unsigned int prio);
    ~Collector();

    CAContext& ctxt;

    epicsMutex mutex;

    struct PV {
        std::tr1::shared_ptr<Subscription> sub;
        bool ready;
        bool connected;
        PV() :ready(false), connected(false) {}
    };
    typedef std::vector<PV> pvs_t;
    pvs_t pvs;

    typedef std::set<Receiver*> receivers_t;
    receivers_t receivers;
    bool receivers_changed;

    epicsEvent wakeup;

    bool waiting;
    bool run;

    epics::pvData::Thread processor;

    void close();

    void notEmpty(Subscription* sub);

    void add_receiver(Receiver*);
    void remove_receiver(Receiver*);

    // only for unittest code
    inline Subscription* subscription(size_t column) { return pvs[column].sub.get(); }

private:
    // locals for processor thread

    typedef std::map<epicsUInt64, Receiver::slices_t::value_type::second_type> events_t;
    events_t events;

    receivers_t receivers_shadow;

    epicsTimeStamp now;
    epicsUInt64 now_key,
                oldest_key; // oldest key sent to Receviers
    Receiver::slices_t completed;

    void process();
    void process_dequeue();
    void process_test();

    EPICS_NOT_COPYABLE(Collector)
};

extern double bsasFlushPeriod;

#endif // COLLECTOR_H
