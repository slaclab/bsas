
#include <testMain.h>
#include <epicsMath.h>
#include <errlog.h>
#include <pv/pvUnitTest.h>
#include <pv/current_function.h>
#include <pv/sharedVector.h>

#include "receiver_pva.h"

namespace pvd = epics::pvData;

namespace {

struct TestPVA {
    CAContext ctxt;
    epics::auto_ptr<Collector> collect;
    epics::auto_ptr<PVAReceiver> R;

    Receiver::slices_t slices;

    TestPVA()
        :ctxt(epicsThreadPriorityMedium, true)
    {
        pvd::shared_vector<std::string> names;
        names.push_back("foo");
        names.push_back("bar");

        collect.reset(new Collector(ctxt, pvd::freeze(names), epicsThreadPriorityMedium));
        R.reset(new PVAReceiver(*collect));
        testEqual(R->columns.size(), 2u);
    }

    void push_scalar(const epicsTimeStamp& ts, size_t r, size_t c, double v)
    {
        slices.resize(std::max(slices.size(), r+1));

        Receiver::slices_t::value_type& slice = slices[r];
        slice.second.resize(2);

        DBRValue V(new DBRValue::Holder);
        V->sevr = V->stat = 0;
        V->ts = ts;
        V->count = 1;

        pvd::shared_vector<double> temp(1);
        temp[0] = v;
        V->buffer = pvd::static_shared_vector_cast<const void>(pvd::freeze(temp));

        slice.second.at(c) = V;
    }

    void test_simple()
    {
        epicsTimeStamp T0;
        epicsTimeGetCurrent(&T0);
        push_scalar(T0, 0, 0, 1.0);
        push_scalar(T0, 0, 1, 2.0);

        epicsTimeStamp T1;
        epicsTimeGetCurrent(&T1);
        push_scalar(T1, 1, 0, 3.0);
        push_scalar(T1, 1, 1, 4.0);

        R->slices(slices);
        testShow()<<R->changed<<"\n"<<R->root;

        pvd::PVDoubleArrayPtr farr;

        {
            pvd::shared_vector<double> arr(2);
            arr[0] = 1.0;
            arr[1] = 3.0;
            testFieldEqual<pvd::PVDoubleArray>(R->root, "value.foo", pvd::freeze(arr));
        }
        {
            pvd::shared_vector<double> arr(2);
            arr[0] = 2.0;
            arr[1] = 4.0;
            testFieldEqual<pvd::PVDoubleArray>(R->root, "value.bar", pvd::freeze(arr));
        }
    }
};

} // namespace

MAIN(test_receiver)
{
    testPlan(3);
    TEST_METHOD(TestPVA, test_simple);
    return testDone();
}
