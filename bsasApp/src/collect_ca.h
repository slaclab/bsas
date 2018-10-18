#ifndef COLLECT_CA_H
#define COLLECT_CA_H

#include <string>
#include <deque>

#include <epicsTime.h>
#include <epicsMutex.h>
#include <epicsGuard.h>
#include <alarm.h>
#include <pv/noDefaultMethods.h>
#include <pv/sharedVector.h>

typedef epicsGuard<epicsMutex> Guard;
typedef epicsGuardRelease<epicsMutex> UnGuard;

// cf. cadef.h
struct ca_client_context;
struct oldChannelNotify;
struct oldSubscription;
struct connection_handler_args;

struct Collector;

struct DBRValue {
    struct Holder {
        static size_t num_instances;

        epicsTimeStamp ts; // in epics epoch
        epicsUInt16 sevr, // [0-3] or 4 (Disconnect)
                    stat; // status code a la Base alarm.h
        epicsUInt32 count;
        epics::pvData::shared_vector<const void> buffer; // contains DBF_* mapped to pvd:pv* code
        Holder();
        ~Holder();
    };
private:
    std::tr1::shared_ptr<Holder> held;
public:

    DBRValue() {}
    DBRValue(Holder *H) :held(H) {}

    bool valid() const { return !!held; }
    Holder* operator->() {return held.get();}
    const Holder* operator->() const {return held.get();}

    void swap(DBRValue& o) {
        held.swap(o.held);
    }
};

struct CAContext {
    static size_t num_instances;

    explicit CAContext(unsigned int prio, bool fake=false);
    ~CAContext();

    struct ca_client_context *context;

    // manage attachment of a context to the current thread
    struct Attach {
        struct ca_client_context *previous;
        Attach(const CAContext&);
        ~Attach();
    };

    EPICS_NOT_COPYABLE(CAContext)
};

struct Subscription {
    static size_t num_instances;

    const std::string pvname;
    const CAContext& context;
    Collector& collector;
    const size_t column;

    // set before callbacks are possible, cleared after callbacks are impossible
    struct oldChannelNotify *chid;
    // effectively a local of a CA worker, set and cleared from onConnect()
    struct oldSubscription *evid;

    epicsMutex mutex;

    bool connected;
    size_t nDisconnects, nErrors, nUpdates, nUpdateBytes, nOverflows;
    size_t limit;

    std::deque<DBRValue> values;

    Subscription(const CAContext& context,
                 size_t column,
                 const std::string& pvname,
                 Collector& collector);
    ~Subscription();

    void close();

    // dequeue one update
    DBRValue pop();

    // for test code only
    void push(const DBRValue& v);

private:
    void _push(DBRValue& v);

    static void onConnect (struct connection_handler_args args);
    static void onEvent (struct event_handler_args args);

    EPICS_NOT_COPYABLE(Subscription)
};

#endif // COLLECT_CA_H
