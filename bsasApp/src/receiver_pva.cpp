
#include <epicsMath.h>
#include <errlog.h>

#include <pv/standardField.h>

#include "receiver_pva.h"

#include <epicsExport.h>

namespace pvd = epics::pvData;

static int receiverPVADebug;

namespace {
// adjust name to be a valid field name.  [A-Za-z_][A-Za-z0-9_]*
void mangleName(std::string& name)
{
    if(name.empty())
        throw std::runtime_error("Empty PV name not allowed");

    for(size_t i=0, N=name.size(); i<N; i++) {
        char c = name[i];
        if(c>='A' && c<='Z') {}
        else if(c>='a' && c<='z') {}
        else if(i!=0 && c>='0' && c<='9') {}
        else if(c=='_') {}
        else {
            name[i] = '_';
        }
    }
}

template<typename T>
struct default_value { static inline T is() { return 0; } };
template<> struct default_value<float>  { static inline float is() { return epicsNAN; } };
template<> struct default_value<double>  { static inline double is() { return epicsNAN; } };
template<> struct default_value<std::string>  { static inline std::string is() { return ""; } };

// scalar types other than string
template<typename T>
struct NumericScalarCopier : public PVAReceiver::ColCopy
{
    pvd::PVDoubleArrayPtr field;

    NumericScalarCopier(PVAReceiver& receiver, size_t coln) :PVAReceiver::ColCopy(receiver)
    {
        field = receiver.root
                ->getSubFieldT<pvd::PVStructure>("value")
                ->getSubFieldT<pvd::PVDoubleArray>(receiver.columns.at(coln).fname);
    }
    virtual ~NumericScalarCopier() {}

    virtual void copy(const PVAReceiver::slices_t &s, size_t coln)
    {
        pvd::shared_vector<T> scratch(s.size(), default_value<T>::is());
        PVAReceiver::Column& column = receiver.columns.at(coln);

        for(size_t r=0, R=s.size(); r<R; r++) {
            const DBRValue& cell = s[r].second.at(coln);

            if(!cell.valid()) continue;

            if(cell->count!=1 || cell->buffer.original_type()!=(pvd::ScalarType)pvd::ScalarTypeID<T>::value) {
                column.ftype = (pvd::ScalarType)pvd::ScalarTypeID<T>::value;
                column.isarray = cell->count!=1;
                receiver.retype = true;
                return;
            }

            // could just alias cell->buffer.data()
            const pvd::shared_vector<const T>& elem(pvd::static_shared_vector_cast<const T>(cell->buffer));
            assert(elem.size()==1);

            scratch[r] = elem[0];
        }

        field->replace(pvd::freeze(scratch));
        receiver.changed.set(field->getFieldOffset());
    }
};

} // namespace

PVAReceiver::PVAReceiver(Collector& collector)
    :collector(collector)
    ,pv(pvas::SharedPV::buildReadOnly())
    ,retype(true)
{
    collector.add_receiver(this); // calls our names()
}

PVAReceiver::~PVAReceiver() {close();}

void PVAReceiver::close()
{
    collector.remove_receiver(this);
    pv->close();
}

void PVAReceiver::names(const std::vector<std::string>& pvs)
{
    columns_t cols(pvs.size());
    pvd::shared_vector<std::string> Ls(pvs.size());

    for(size_t i=0, N=pvs.size(); i<N; i++) {
        Column& col = cols[i];
        col.fname = Ls[i] = pvs[i];
        mangleName(col.fname);

        // assume a signals are scalar double until proven false
        col.ftype = epics::pvData::pvDouble;
        col.isarray = false;

    }

    Ls.push_back("secondsPastEpoch");
    Ls.push_back("nanoseconds");

    {
        Guard G(mutex);
        columns.swap(cols);
        labels = pvd::freeze(Ls);

        root.reset();
        changed.clear();

        retype = true;
    }

    pv->close(); // paranoia?

    slices(slices_t()); // push an empty event to trigger initial (re)type
}

void PVAReceiver::slices(const slices_t& s)
{
    {
        Guard G(mutex); // safely on Collector worker thread, but paranoia...

        if(retype) {
            retype = false;
            if(receiverPVADebug>0) {
                errlogPrintf("PVAReceiver type change\n");
            }

            pvd::FieldBuilderPtr builder(pvd::getFieldCreate()->createFieldBuilder()
                                         ->setId("epics:nt/NTTable:1.0")
                                         ->addArray("labels", pvd::pvString)
                                         ->addNestedStructure("value"));

            for(size_t i=0, N=columns.size(); i<N; i++) {
                Column& col = columns[i];
                if(!col.isarray) {
                    builder = builder->addArray(col.fname, col.ftype);
                } else {
                    builder = builder->addNestedUnionArray(col.fname)
                                        ->addArray("arr", col.ftype)
                                     ->endNested();
                }
            }

            pvd::StructureConstPtr type(builder
                                            ->addArray("secondsPastEpoch", pvd::pvUInt)
                                            ->addArray("nanoseconds", pvd::pvUInt)
                                        ->endNested() // end of .value
                                        //->add("alarm", pvd::getStandardField()->alarm())
                                        //->add("timeStamp", pvd::getStandardField()->timeStamp())
                                        ->createStructure());
            root = pvd::getPVDataCreate()->createPVStructure(type);
            changed.clear();

            fsec = root->getSubFieldT<pvd::PVUIntArray>("value.secondsPastEpoch");
            fnsec = root->getSubFieldT<pvd::PVUIntArray>("value.nanoseconds");

            {
                pvd::PVStringArrayPtr flabels(root->getSubFieldT<pvd::PVStringArray>("labels"));
                flabels->replace(labels);
                changed.set(flabels->getFieldOffset());
            }

            pvd::PVStructurePtr value(root->getSubFieldT<pvd::PVStructure>("value"));

            for(size_t c=0, C=columns.size(); c<C; c++) {
                Column& col = columns[c];

                if(!col.isarray && col.ftype==pvd::pvDouble) {
                    col.copier.reset(new NumericScalarCopier<double>(*this, c));
                } else {
                    // TODO: not supported
                }
            }

            {
                UnGuard U(G);
                pv->close();
                pv->open(*root, changed);
            }
        } // end retype

        changed.clear();

        pvd::shared_vector<pvd::uint32> sec(s.size()), nsec(s.size());

        for(size_t r=0, R=s.size(); r<R; r++) {
            epicsUInt64 key = s[r].first;
            sec[r] = (key>>32) + POSIX_TIME_AT_EPICS_EPOCH;
            nsec[r] = key;
        }

        fsec->replace(pvd::freeze(sec));
        fnsec->replace(pvd::freeze(nsec));
        changed.set(fsec->getFieldOffset());
        changed.set(fnsec->getFieldOffset());

        for(size_t c=0, C=columns.size(); c<C; c++) { // for each column
            Column& col = columns[c];

            if(col.copier)
                col.copier->copy(s, c);
        }

        {
            UnGuard U(G);
            pv->post(*root, changed);
        }

    }

}

extern "C" {
epicsExportAddress(int, receiverPVADebug);
}
