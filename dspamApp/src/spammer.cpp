/** Distributed load testing driver.
 *
 * Multiple dspam nodes cooperate to generate streams
 * of DB updates with matching timestamps.
 *
 * Coordination is accomplished by UDP multicast from
 * one controller node.
 */

#include <memory>
#include <string>
#include <map>
#include <vector>
#include <stdexcept>
#include <sstream>

#include <osiSock.h>
#include <errlog.h>
#include <dbDefs.h>
#include <devSup.h>
#include <iocsh.h>
#include <link.h>
#include <dbScan.h>
#include <recGbl.h>
#include <dbCommon.h>
#include <alarm.h>
#include <epicsTime.h>
#include <epicsMutex.h>
#include <epicsGuard.h>
#include <epicsThread.h>
#include <epicsEvent.h>
#include <epicsStdio.h>
#include <epicsExit.h>

#include <aoRecord.h>
#include <aiRecord.h>

#include "iocshelper.h"

#include <epicsExport.h>

namespace {

typedef epicsGuard<epicsMutex> Guard;
typedef epicsGuardRelease<epicsMutex> UnGuard;

#define TRY(klass) klass *self = (klass*)prec->dpvt; \
    try { \
        if(!self) throw std::runtime_error("Not initialized");

// translate exception to alarm in dset functions
#define CATCH() } catch(std::exception& e) { \
        errlogPrintf("%s : error %s\n", prec->name, e.what()); \
        (void)recGblSetSevr(prec, COMM_ALARM, INVALID_ALARM); \
    }

struct SocketError : public std::runtime_error
{
    static std::string build(int err, const std::string& msg) {
        char buf[40];
        epicsSocketConvertErrorToString(buf, sizeof(buf), err);
        std::ostringstream strm;
        strm<<buf<<" ("<<err<<") : "<<msg;
        return strm.str();
    }

    SocketError(int err, const std::string& msg)
        :std::runtime_error(build(err, msg))
    {}
    virtual ~SocketError() {}
};

struct Socket {
    SOCKET sock;
    Socket(int domain, int type, int protocol)
        :sock(epicsSocketCreate(domain, type, protocol))
    {
        if(sock==INVALID_SOCKET)
            throw std::runtime_error("epicsSocketCreate");
    }
    ~Socket() {
        epicsSocketDestroy(sock);
    }
};

struct spam_message {
    epicsUInt32 ts_sec;
    epicsUInt32 ts_nsec;
    epicsUInt32 counter;
};
static_assert(sizeof(spam_message)==4*3, "No padding");

struct Receiver : public epicsThreadRunable {
    static std::map<std::string, std::unique_ptr<Receiver> > receivers;

    const std::string name;

    Socket sock;

    IOSCANPVT scan;

    epicsMutex lock;
    epicsThread worker;

    bool running;

    bool valid;
    epicsUInt32 counter;
    epicsTimeStamp stamp;

    Receiver(const char *name, const char *maddr, const char *iface);
    virtual ~Receiver() {}

    void close();

    virtual void run() override final;

    static long init_record(dbCommon *prec);
    static long get_io_intr_info(int detach, struct dbCommon *prec, IOSCANPVT* pscan);
    static long read_counter(aiRecord *prec);
};
std::map<std::string, std::unique_ptr<Receiver> > Receiver::receivers;

Receiver::Receiver(const char *name, const char *maddr, const char *iface)
    :name(name)
    ,sock(AF_INET, SOCK_DGRAM, 0)
    ,worker(*this,
            name,
            epicsThreadGetStackSize(epicsThreadStackBig),
            epicsThreadPriorityCAServerHigh+1)
    ,running(true)
    ,valid(false)
{
    scanIoInit(&scan);

    epicsSocketEnableAddressUseForDatagramFanout(sock.sock);

    osiSockAddr bindaddr = {0};
    bindaddr.ia.sin_family = AF_INET;
    bindaddr.ia.sin_addr.s_addr = htonl(INADDR_ANY);
    bindaddr.ia.sin_port = htons(9876);

    if(bind(sock.sock, &bindaddr.sa, sizeof(bindaddr)))
        throw SocketError(SOCKERRNO, "bind");

    if(iface && *iface) {
        if(aToIPAddr(iface, 9876, &bindaddr.ia))
            throw std::runtime_error("aToIPAddr");
    }

    ip_mreqn req = {0};
    {
        sockaddr_in temp;
        if(aToIPAddr(maddr, 9876, &temp))
            throw std::runtime_error("aToIPAddr");
        req.imr_multiaddr.s_addr = temp.sin_addr.s_addr;
    }
    req.imr_address.s_addr = bindaddr.ia.sin_addr.s_addr;

    fprintf(stderr, "ADD_MEMBERSHIP %08x %08x\n", (unsigned)htonl(req.imr_multiaddr.s_addr), (unsigned)ntohl(req.imr_address.s_addr));

    if(setsockopt(sock.sock, SOL_IP, IP_ADD_MEMBERSHIP, &req, sizeof(req)))
        throw SocketError(SOCKERRNO, "IP_ADD_MEMBERSHIP");

    worker.start();
}

void Receiver::close()
{
    {
        Guard G(lock);
        running = false;
    }

    switch(epicsSocketSystemCallInterruptMechanismQuery()) {
    case esscimqi_socketBothShutdownRequired:
        shutdown(sock.sock, SHUT_RDWR);
        break;
    case esscimqi_socketCloseRequired:
        epicsSocketDestroy(sock.sock);
        break;
    case esscimqi_socketSigAlarmRequired: // no longer used, but still hanging around...
        break;
    }

    worker.exitWait();
}

void Receiver::run()
{
    Guard G(lock);
    while(running) {
        union {
            spam_message msg;
            char b[sizeof(spam_message)];
        } buf;

        int err;
        {
            UnGuard U(G);

            err = recvfrom(sock.sock, buf.b, sizeof(buf.b), 0, NULL, NULL);
            if(err<0) {
                errlogPrintf("%s : Error: %s\n", name.c_str(), SocketError::build(SOCKERRNO, "recvfrom()").c_str());
                // limit error spam
                epicsThreadSleep(0.1);
                continue;

            } else if(err!=sizeof(buf.b)) {
                errlogPrintf("%s : recvfrom() Error: incorrect size %d != %zu\n", name.c_str(), err, sizeof(buf.b));
                continue;
            }
        }

        valid = true;
        counter = ntohl(buf.msg.counter);
        stamp.secPastEpoch = ntohl(buf.msg.ts_sec)-POSIX_TIME_AT_EPICS_EPOCH;
        stamp.nsec = ntohl(buf.msg.ts_nsec);

        {
            UnGuard U(G);

            scanIoImmediate(scan, priorityHigh);
            scanIoImmediate(scan, priorityMedium);
            scanIoImmediate(scan, priorityLow);
        }
    }
}

long Receiver::init_record(dbCommon *prec)
{
    try {
        DBLINK* dlink = dbGetDevLink(prec);
        assert(dlink->type==INST_IO);

        auto it = receivers.find(dlink->value.instio.string);
        if(it==receivers.end())
            throw std::runtime_error("Not such Controller");

        prec->dpvt = (void*)it->second.get();
    CATCH()
    return 0;
}

long Receiver::get_io_intr_info(int detach, struct dbCommon *prec, IOSCANPVT* pscan)
{
    TRY(Receiver) {
        *pscan = self->scan;
        return 0;
    }CATCH()
    return S_dev_noDeviceFound;
}

long Receiver::read_counter(aiRecord *prec)
{
    TRY(Receiver) {
        prec->val = self->counter;
        prec->time = self->stamp;
        if(!self->valid)
            (void)recGblSetSevr(prec, COMM_ALARM, INVALID_ALARM);

        return 2; // disable convert
    } CATCH()
    return S_dev_noDeviceFound;
}

void spammerCreate(const char *name, const char *ep, const char *iface)
{
    try {
        std::unique_ptr<Receiver> inst(new Receiver(name, ep, iface));

        Receiver::receivers.insert(std::make_pair(name, std::move(inst)));

    }catch(std::exception& e){
        fprintf(stderr, "Error: %s\n", e.what());
    }
}

struct Controller : public epicsThreadRunable {
    static std::map<std::string, std::unique_ptr<Controller> > controllers;

    const std::string name;

    Socket sock;
    osiSockAddr dest;

    epicsMutex lock;
    epicsThread worker;
    epicsEvent wake;

    bool running;
    double period;
    epicsUInt32 counter;

    Controller(const char *name, const char *maddr, const char *iface);
    virtual ~Controller() {}

    void close();

    virtual void run() override final;

    static long init_record(dbCommon *prec);
    static long write_period(aoRecord* prec);
};

std::map<std::string, std::unique_ptr<Controller> > Controller::controllers;

Controller::Controller(const char *name, const char *maddr, const char *iface)
    :name(name)
    ,sock(AF_INET, SOCK_DGRAM, 0)
    ,worker(*this,
            name,
            epicsThreadGetStackSize(epicsThreadStackBig),
            epicsThreadPriorityCAServerHigh+1)
    ,running(true)
    ,period(1.0)
    ,counter(0u)
{
    if(aToIPAddr(maddr, 9876, &dest.ia))
        throw std::runtime_error("aToIPAddr");

    if(iface && iface[0]) {
        sockaddr_in iaddr;
        if(aToIPAddr(iface, 9876, &iaddr))
            throw std::runtime_error("aToIPAddr");

        struct ip_mreqn mcast_conf = {0};

        mcast_conf.imr_multiaddr.s_addr = dest.ia.sin_addr.s_addr;
        mcast_conf.imr_address.s_addr = iaddr.sin_addr.s_addr;

        if(setsockopt(sock.sock, SOL_IP, IP_MULTICAST_IF, &mcast_conf, sizeof(mcast_conf)))
            throw std::runtime_error("IP_MULTICAST_IF");
    }

    {
        osiSockOptMcastLoop_t val = 1;
        if(setsockopt(sock.sock, SOL_IP, IP_MULTICAST_LOOP, &val, sizeof(val)))
            throw std::runtime_error("IP_MULTICAST_LOOP");
    }

    {
        osiSockOptMcastTTL_t val = 1;
        if(setsockopt(sock.sock, SOL_IP, IP_MULTICAST_TTL, &val, sizeof(val)))
            throw std::runtime_error("IP_MULTICAST_TTL");
    }

    worker.start();
}

void Controller::close()
{
    {
        Guard G(lock);
        running = false;
    }
    wake.signal();
    worker.exitWait();
}

void Controller::run()
{
    Guard G(lock);

    while(running) {
        double waitfor = period;
        {
            UnGuard U(G);

            if(wake.wait(waitfor))
                break; // if not timeout, then we are done.
        }

        epicsTimeStamp now;
        epicsTimeGetCurrent(&now);

        spam_message msg;
        msg.ts_sec = htonl(now.secPastEpoch + POSIX_TIME_AT_EPICS_EPOCH);
        msg.ts_nsec = htonl(now.nsec);
        msg.counter = htonl(counter++);

        if(sendto(sock.sock, (void*)&msg, sizeof(msg), 0,
                  &dest.sa, sizeof(dest))!=sizeof(msg))
        {
            errlogPrintf("%s : %s\n", name.c_str(), SocketError::build(SOCKERRNO, "sendto()").c_str());
        }
    }
}

long Controller::init_record(dbCommon* prec)
{
    try {
        DBLINK* dlink = dbGetDevLink(prec);
        assert(dlink->type==INST_IO);

        auto it = controllers.find(dlink->value.instio.string);
        if(it==controllers.end())
            throw std::runtime_error("Not such Controller");

        prec->dpvt = (void*)it->second.get();

    CATCH()
    return 2; // disable scaling
}

long Controller::write_period(aoRecord* prec)
{
    TRY(Controller) {
        Guard G(self->lock);
        self->period = prec->val;
        return 0;
    } CATCH()
    return S_dev_noDeviceFound;
}

void spamControllerCreate(const char *name, const char *ep, const char *iface)
{
    try {
        std::unique_ptr<Controller> inst(new Controller(name, ep, iface));

        Controller::controllers.insert(std::make_pair(name, std::move(inst)));

    }catch(std::exception& e){
        fprintf(stderr, "Error: %s\n", e.what());
    }
}

void dspamExit(void *)
{
    try {
        for(auto& it : Controller::controllers) {
            it.second->close();
        }
        for(auto& it : Receiver::receivers) {
            it.second->close();
        }
    }catch(std::exception& e){
        fprintf(stderr, "Error during exit: %s\n", e.what());
    }
}

void dspamReg()
{
    epics::iocshRegister<const char*,
                         const char*,
                         const char*,
                         &spammerCreate>(
                "spammerCreate",
                "name",
                "maddr",
                "iface");
    epics::iocshRegister<const char*,
                         const char*,
                         const char*,
                         &spamControllerCreate>(
                "spamControllerCreate",
                "name",
                "maddr",
                "iface");

    epicsAtExit(&dspamExit, 0);
}

template<class REC>
struct dset6 {
    typed_dset base;
    long (*io)(REC *prec);
    long (*special_linconv)(REC *prec, int after);
};

dset6<aiRecord> devSpamCounter = {{6, 0,0, &Receiver::init_record, &Receiver::get_io_intr_info}, &Receiver::read_counter};
dset6<aoRecord> devSpamControlPeriod = {{6, 0,0, &Controller::init_record, 0}, &Controller::write_period};

} // namespace

extern "C" {
epicsExportRegistrar(dspamReg);
epicsExportAddress(dset, devSpamCounter);
epicsExportAddress(dset, devSpamControlPeriod);
}
