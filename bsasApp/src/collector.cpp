
#include <list>
#include <algorithm>

#include <epicsMath.h>
#include <errlog.h>
#include <pv/reftrack.h>

#include "collector.h"

#include <epicsExport.h>

namespace pvd = epics::pvData;

// limit on number of potentially complete events to track
static double maxEventRate = 20;
// timeout to flush partial events
static double maxEventAge = 2.5;
// holdoff after delivering events
double bsasFlushPeriod = 2.0;

int collectorDebug;

size_t Collector::num_instances;

Collector::Collector(CAContext& ctxt, const names_t &names, unsigned int prio)
    :ctxt(ctxt)
    ,receivers_changed(false)
    ,nComplete(0u)
    ,nOverflow(0u)
    ,waiting(false)
    ,run(true)
    ,processor(pvd::Thread::Config(this, &Collector::process)
               .name("BSA Processor")
               .prio(prio))
    ,oldest_key(0u)
{
    REFTRACE_INCREMENT(num_instances);

    pvs.resize(names.size());

    for(size_t i=0, N=names.size(); i<N; i++)
    {
        pvs[i].sub.reset(new Subscription(ctxt, i, names[i], *this));
    }

    processor.start();
}

Collector::~Collector()
{
    REFTRACE_DECREMENT(num_instances);
    close();
}

void Collector::close()
{
    for(size_t i=0, N=pvs.size(); i<N; i++) {
        pvs[i].sub->close();
    }

    {
        Guard G(mutex);
        run = false;
    }
    wakeup.signal();
    processor.exitWait();
}

void Collector::notEmpty(Subscription *sub)
{
    bool wakeme;
    {
        Guard G(mutex);
        pvs[sub->column].ready = true;
        wakeme = waiting;
    }
    if(collectorDebug>2)
        errlogPrintf("## %s notEmpty %s\n", sub->pvname.c_str(), wakeme?" wakeup":"");
    if(wakeme)
        wakeup.signal();
}


void Collector::add_receiver(Receiver* recv)
{
    std::vector<std::string> names;
    {
        Guard G(mutex);
        receivers.insert(recv);
        receivers_changed = true;

        names.reserve(pvs.size());
        for(size_t i=0, N=pvs.size(); i<N; i++) {
            names.push_back(pvs[i].sub->pvname);
        }
    }
    recv->names(names);
}

void Collector::remove_receiver(Receiver* recv)
{
    Guard G(mutex);
    receivers.erase(recv);
    receivers_changed = true;
}

void Collector::process()
{
    Guard G(mutex);

    epicsTimeGetCurrent(&now);

    while(run) {
        waiting = false; // set if input queues emptied

        if(collectorDebug>2) {
            char buf[30];
            epicsTimeToStrftime(buf, sizeof(buf), "%H:%M:%S.%f", &now);
            errlogPrintf("## processor wakeup %s\n", buf);
        }

        now_key = now.secPastEpoch;
        now_key <<= 32;
        now_key |= now.nsec;

        process_dequeue();
        process_test();

        if(receivers_changed) {
            // copy for use while unlocked
            receivers_shadow = receivers;
            receivers_changed = false;
        }

        bool willwait = waiting;
        {
            nComplete += completed.size();
            UnGuard U(G);

            if(!completed.empty()) {
                for(receivers_t::iterator it(receivers_shadow.begin()), end(receivers_shadow.end()); it!=end; ++it) {
                    (*it)->slices(completed);
                }
                epicsThreadSleep(bsasFlushPeriod);
            }

            if(willwait)
                wakeup.wait();
            epicsTimeGetCurrent(&now);
        }
    }
}

void Collector::process_dequeue()
{
    // process input queues
    bool nothing = false; // true if all queues empty
    // break if:
    // * nothing to do
    // * # of potentially complete events exceeds limit
    unsigned maxEvents = std::max(10.0, std::min(maxEventRate*bsasFlushPeriod, 1000.0));
    while(!nothing && events.size() < maxEvents) {
        nothing = true;

        std::list<events_t::mapped_type> slices_done;

        for(size_t i=0, N=pvs.size(); i<N; i++) {
            PV& pv = pvs[i];

            if((i!=0 && !pv.ready) || !pv.sub) continue;

            DBRValue val(pv.sub->pop());
            if(!val.valid()) {
                pv.ready = false;
                continue;
            }
            pv.ready = true;

            nothing = false; // we will do something

            epicsUInt64 key = val->ts.secPastEpoch;
            key <<= 32;
            key |= val->ts.nsec;

            pv.connected = val->sevr<=3;

            if(collectorDebug>3) {
                errlogPrintf("## %s event:%llx sevr %u\n", pv.sub->pvname.c_str(), key, val->sevr);
            }

            if(pv.connected && key > oldest_key) {
                // data event

                // create/update a slice

                events_t::mapped_type& slice = events[key]; // implicitly allocs new slice
                slice.resize(pvs.size());

                if(slice[i].valid()) {
                    if(collectorDebug>=0) {
                        errlogPrintf("%s : ignore duplicate key %llx\n", pvs[i].sub->pvname.c_str(), key);
                    }

                } else {
                    slice[i].swap(val);
                }

            } else if(pv.connected) {
                // disconnect event
            } else if(collectorDebug>0) {
                if(collectorDebug>=0) {
                    errlogPrintf("## %s ignore leftovers of %llx\n", pvs[i].sub->pvname.c_str(), key);
                }
            }
        }
    }

    if(!nothing) {
        nOverflow++;
        // overflowed event buffer.
        // only carry over 4 events per PV

        for(size_t i=0, N=pvs.size(); i<N; i++) {
            PV& pv = pvs[i];
            if(pv.sub) {
                pv.sub->clear(4);
            }
        }

    }

    waiting = nothing; // wait if we emptied all queues
}

void Collector::process_test()
{
    epicsUInt64 max_age = maxEventAge;
    max_age <<= 32;
    max_age |= epicsUInt32(1000000000u * fmod(maxEventAge, 1.0));

    events_t::iterator first_partial(events.end()); // first element _not_ to flush.

    size_t i=events.size();
    {
        // iterate from newest.  Find most recent incomplete/partial event.
        events_t::reverse_iterator it(events.rbegin()), end(events.rend());
        for(; it!=end; ++it, i--) {
            // flush if

            // * slice key is too old
            epicsInt64 key_age = epicsInt64(now_key) - epicsInt64(it->first);

            if(key_age >= epicsInt64(max_age)) {
                if(collectorDebug>4) {
                    errlogPrintf("## test slice %llx too old %llx >= %llx\n", it->first, key_age, max_age);
                }
                // everything earlier is also too old.  We will flush all.
                if(collectorDebug>1) {
                    errlogPrintf("Reconstruct buffer overflow\n");
                    // TODO: stat counter...
                }
                break;
            }

            // * all PVs are either disconnected or have data
            const events_t::mapped_type& slice = it->second;
            // test if all data available or disconnected
            bool complete = true;
            for(size_t i=0, N=pvs.size(); complete && i<N; i++) {
                complete = !pvs[i].connected || slice[i].valid();

                if(!complete && collectorDebug>4) {
                    errlogPrintf("## test slice %llx found incomplete %s %sconn %svalid\n",
                                 it->first, pvs[i].sub->pvname.c_str(),
                                 !pvs[i].connected?"dis":"",
                                 slice[i].valid()?"":"in");
                }
            }

            if(!complete) {
                // found it
                first_partial = --it.base();
                assert(it->first==first_partial->first);
                break;
            }
        }
    }

    completed.clear(); // paranoia, should already be empty

    // 'it' points to first element _not_ to remove

    if(collectorDebug>3) {
        if(first_partial==events.begin() || events.empty()) {
            errlogPrintf("## No events complete\n");
        } else {
            errlogPrintf("## %zu events complete\n", events.size()-i);
        }
    }

    // flush all events before the most recent incomplete/partial event
    events_t::iterator it(events.begin()), end(first_partial);

    completed.reserve(events.size()-i);
    while(it!=end) {
        events_t::iterator cur = it++;

        if(collectorDebug>4) {
            errlogPrintf("## complete key %llx\n", cur->first);
        }
        assert(cur->first > oldest_key);
        oldest_key = cur->first;

        completed.push_back(*cur);

        events.erase(cur);
    }

    while(events.size()>4) {
        // only carry over 4 partials

        events.erase(events.begin());
        nOverflow++;
    }
}

extern "C" {
epicsExportAddress(double, maxEventRate);
epicsExportAddress(double, maxEventAge);
epicsExportAddress(int, collectorDebug);
epicsExportAddress(double, bsasFlushPeriod);
}
