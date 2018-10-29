
#include <testMain.h>
#include <epicsMath.h>
#include <errlog.h>
#include <pv/pvUnitTest.h>
#include <pv/current_function.h>
#include <pv/sharedVector.h>

#include "collector.h"

namespace pvd = epics::pvData;

extern int collectorDebug; // see collector.cpp
extern double bsasFlushPeriod;

namespace {

struct TestReceiver : public Receiver
{
    Collector& collector;
    epicsMutex mutex;
    epicsEvent wakeup;
    std::vector<std::string> mynames;
    Receiver::slices_t myslices;

    explicit TestReceiver(Collector& collector)
        :collector(collector)
    {
        collector.add_receiver(this);
    }
    virtual ~TestReceiver() {
        collector.remove_receiver(this);
    }

    virtual void names(const std::vector<std::string>& n) {
        Guard G(mutex);
        mynames = n;
    }
    virtual void slices(const slices_t& s) {
        {
            Guard G(mutex);
            myslices.reserve(myslices.size()+s.size());
            for(size_t i=0, N=s.size(); i<N; i++)
                myslices.push_back(s[i]);
        }
        wakeup.signal();
    }

    void clear() {
        Guard G(mutex);
        myslices.clear();
    }

    epicsTimeStamp now;
    void start(epicsTimeStamp& T) {
        epicsTimeGetCurrent(&now);
        T = now;
    }
    void push(size_t column, double val) {
        testDiag("column %zu push %f @%x%x", column, val, now.secPastEpoch, now.nsec);
        pvd::shared_vector<double> V;
        V.push_back(val);
        DBRValue value(new DBRValue::Holder);
        value->ts = now;
        value->sevr = value->stat = 0; // NO_ALARM
        value->buffer = pvd::static_shared_vector_cast<const void>(pvd::freeze(V));
        collector.subscription(column)->push(value);
    }

    void push_disconn(size_t column) {
        testDiag("column %zu push disconnect @%x%x", column, now.secPastEpoch, now.nsec);
        DBRValue value(new DBRValue::Holder);
        value->ts = now;
        collector.subscription(column)->push(value);
    }

    void notify(size_t column) {
        testDiag("column %zu notify", column);
        collector.notEmpty(collector.subscription(column));
    }
};

struct TestFooBar {
    CAContext ctxt;
    epics::auto_ptr<Collector> collect;
    epics::auto_ptr<TestReceiver> R;
    TestFooBar()
        :ctxt(epicsThreadPriorityMedium, true)
    {
        pvd::shared_vector<std::string> names;
        names.push_back("foo");
        names.push_back("bar");

        collect.reset(new Collector(ctxt, pvd::freeze(names), epicsThreadPriorityMedium));
        R.reset(new TestReceiver(*collect));
        testEqual(R->mynames.size(), 2u);
    }

    void testSlice(size_t S, const epicsTimeStamp& ts, double foo, double bar)
    {
        Guard G(R->mutex);
        testDiag("test slice %zu", S);
        if(S >= R->myslices.size()) {
            testFail("slice %zu out of range %zu", S, R->myslices.size());
            testSkip(1, "slice out of range");
        } else {
            const Receiver::slices_t::value_type& slice = R->myslices[S];
            testValue(slice.second[0], ts, foo, "foo");
            testValue(slice.second[1], ts, bar, "bar");
        }
    }

    void testValue(const DBRValue& value, const epicsTimeStamp& ts, double val, const char *label)
    {
        if(isnan(val)) {
            // expect disconnected
            if(!value.valid() || value->sevr>3) {
                testPass("Expect %s disconnected.", label );
            } else {
                double actual = pvd::shared_vector_convert<const double>(value->buffer)[0]; // assumes size()>=1
                testFail("Unexpected %s value %f", label, actual);
            }
        } else if(!value.valid()) {
            testFail("%s not valid", label);
        } else {
            double actual = pvd::shared_vector_convert<const double>(value->buffer)[0]; // assumes size()>=1
            bool test = value->ts.secPastEpoch==ts.secPastEpoch && value->ts.nsec==ts.nsec && val==actual;
            testTrue(test)
                    <<" ts "<<std::hex<<ts.secPastEpoch<<std::hex<<ts.nsec<<"=="<<std::hex<<value->ts.secPastEpoch<<std::hex<<value->ts.nsec
                    <<" "<<val<<"=="<<actual;
        }
    }

    void sync_initial() {
        testDiag("==== enter %s", CURRENT_FUNCTION);

        epicsTimeStamp T0;
        R->start(T0);
        R->push(0, 1.0);
        R->notify(0);
        testDiag("column 1 is initially disconnected, so this completes the first event");

        testDiag("Wait for event");
        testOk1(R->wakeup.wait(1.0));
        errlogFlush();

        testSlice(0, T0, 1.0, epicsNAN);
        testEqual(R->myslices.size(), 1u);

        testDiag("the first update for column 1 @T0 will ignored");
        R->push(1, 2.0);
        R->notify(1);

        testDiag("==== exit %s", CURRENT_FUNCTION);
    }

    void push_start() {
        testDiag("==== %s", CURRENT_FUNCTION);

        sync_initial();

        testDiag("Start second event");
        epicsTimeStamp T1;
        R->start(T1);
        R->push(0, 3.0);
        R->notify(0);
        R->push(1, 4.0);
        R->notify(1);

        testDiag("Start third (incomplete) event");
        epicsTimeStamp T2;
        R->start(T2);
        R->push(0, 5.0);
        R->notify(0);

        testDiag("Wait for event");
        testOk1(R->wakeup.wait(1.0));
        errlogFlush();

        testSlice(1, T1, 3.0, 4.0);
        testEqual(R->myslices.size(), 2u);

        // T2 never completes
    }

    void push_disconn() {
        testDiag("==== %s", CURRENT_FUNCTION);

        sync_initial();

        testDiag("Start second event");
        epicsTimeStamp T1;
        R->start(T1);
        R->push(0, 3.0);
        R->notify(0);
        R->push(1, 4.0);
        R->notify(1);

        testDiag("Wait for event");
        testOk1(R->wakeup.wait(1.0));
        errlogFlush();

        testSlice(1, T1, 3.0, 4.0);
        testEqual(R->myslices.size(), 2u);

        epicsTimeStamp T2;
        R->start(T2);
        R->push_disconn(0);
        R->notify(0);
        R->push(1, 6.0);
        R->notify(1);

        testDiag("Wait for event");
        testOk1(R->wakeup.wait(1.0));
        errlogFlush();

        testSlice(2, T2, epicsNAN, 6.0);
        testEqual(R->myslices.size(), 3u);
    }
};

}

MAIN(test_collector)
{
    collectorDebug = 5;
    bsasFlushPeriod = 0.0;
    testPlan(22);
    TEST_METHOD(TestFooBar, push_start);
    TEST_METHOD(TestFooBar, push_disconn);
    return testDone();
}
