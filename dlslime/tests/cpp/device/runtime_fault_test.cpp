#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdlib>
#include <future>
#include <iostream>

#include "fake_driver.h"
using namespace dlslime::device;
using namespace dlslime::device::test;
void check(bool condition, const char* message)
{
    if (!condition)
        throw std::runtime_error(message);
}
template<class F>
void injected(F&& f)
{
    bool caught = false;
    try {
        f();
    }
    catch (const InjectedFailure&) {
        caught = true;
    }
    check(caught, "expected the injected provider failure");
}
void empty(const std::shared_ptr<FakeDriver>& d)
{
    check(d->allocationCount() == 0 && d->streamCount() == 0 && d->eventCount() == 0 && d->pendingCount() == 0,
          "fake resources leaked");
}
void dependencies(const DeviceRuntime& rt)
{
    auto source = rt.allocate(4096, MemoryKind::HostPinned), middle = rt.allocate(4096, MemoryKind::Device),
         target = rt.allocate(4096, MemoryKind::HostPinned);
    std::memset(source.data(), 0x5a, 4096);
    std::memset(target.data(), 0x17, 4096);
    auto producer = rt.createStream(), consumer = rt.createStream();
    auto produced = rt.copy(middle.view(0, 4096), source.view(0, 4096), producer);
    check(!produced.ready(), "fake copy completed without progress");
    consumer.wait(produced);
    auto done = rt.copy(target.view(0, 4096), middle.view(0, 4096), consumer);
    source    = {};
    middle    = {};
    produced  = {};
    check(!done.ready(), "consumer completed before the producer fence");
    for (size_t i = 0; i < 4096; ++i)
        check(static_cast<unsigned char*>(target.data())[i] == 0x17, "copy executed too early");
    done.wait();
    check(done.ready(), "consumer failed to complete");
    for (size_t i = 0; i < 4096; ++i)
        check(static_cast<unsigned char*>(target.data())[i] == 0x5a, "producer ordering or owner retention failed");
}
void normalAndFailureTests()
{
    auto d  = std::make_shared<FakeDriver>();
    auto rt = detail::RuntimeTestAccess::create(d);
    dependencies(rt);
    empty(d);
    {
        auto source = rt.allocate(4096, MemoryKind::HostPinned), target = rt.allocate(4096, MemoryKind::Device);
        std::memset(source.data(), 0x6b, 4096);
        auto stream = rt.createStream();
        auto done   = rt.copy(target.view(0, 4096), source.view(0, 4096), stream);
        source      = {};
        target      = {};
        check(d->allocationCount() == 2 && d->pendingCount() == 1, "pending event lost allocation ownership");
        done = {};
        check(d->lastChecksum() == uint64_t(4096) * 0x6b, "last event did not drain the copy");
        check(d->allocationCount() == 0 && d->pendingCount() == 0,
              "last event failed to release resources after drain");
    }
    empty(d);
    d->failNext(Fault::Allocate);
    injected([&] { rt.allocate(32, MemoryKind::HostPinned); });
    empty(d);
    d->failNext(Fault::CreateStream);
    injected([&] { rt.createStream(); });
    empty(d);
    for (auto point : {Fault::CreateEvent, Fault::CopyBefore, Fault::CopyAfter, Fault::Record, Fault::RecordAfter}) {
        {
            auto source = rt.allocate(4096, MemoryKind::HostPinned), target = rt.allocate(4096, MemoryKind::Device);
            std::memset(source.data(), 0x2c, 4096);
            auto stream    = rt.createStream();
            auto completed = d->completedCopies();
            d->failNext(point);
            injected([&] { rt.copy(target.view(0, 4096), source.view(0, 4096), stream); });
            check(d->pendingCount() == 0 && d->eventCount() == 0,
                  "failed submission did not drain and retire the native event");
            const bool posted = point == Fault::CopyAfter || point == Fault::Record || point == Fault::RecordAfter;
            check(d->completedCopies() == completed + size_t(posted), "unexpected partial-submission retirement");
            if (posted)
                check(d->lastChecksum() == uint64_t(4096) * 0x2c,
                      "partially submitted copy did not finish before returning failure");
            auto retry = rt.copy(target.view(0, 4096), source.view(0, 4096), stream);
            retry.wait();
        }
        empty(d);
    }
    for (auto point : {Fault::Record, Fault::RecordAfter}) {
        auto stream = rt.createStream();
        d->failNext(point);
        injected([&] { rt.record(stream); });
        check(d->eventCount() == 0, "failed record leaked event");
        auto event = rt.record(stream);
        stream     = {};
        check(event.ready(), "stream destruction did not drain marker");
    }
    empty(d);
    for (auto point : {Fault::StreamWaitBefore, Fault::StreamWaitAfter}) {
        {
            auto source = rt.allocate(4096, MemoryKind::HostPinned);
            auto target = rt.allocate(4096, MemoryKind::Device);
            std::memset(source.data(), 0x39, 4096);
            auto producer = rt.createStream(), consumer = rt.createStream();
            auto event = rt.copy(target.view(0, 4096), source.view(0, 4096), producer);
            source     = {};
            target     = {};
            d->failNext(point);
            injected([&] { consumer.wait(event); });
            if (point == Fault::StreamWaitAfter)
                check(event.ready(), "accepted consumer wait was not drained on error");
            event = {};
            check(d->eventCount() == 0 && d->pendingCount() == 0 && d->allocationCount() == 0,
                  "failed consumer wait released an in-use event or leaked owners");
            check(d->lastChecksum() == uint64_t(4096) * 0x39, "producer data lost during failed wait cleanup");
            auto retry = rt.record(producer);
            consumer.wait(retry);
            retry = {};
            consumer.synchronize();
        }
        empty(d);
    }
    std::cout << "PASS CPU fake record/wait errors before and after submission, event retirement and stream reuse\n";
    std::cout
        << "PASS CPU fake delayed completion, producer dependencies, dropped owners, partial copy failure, event-record failure, recovery, and stream drain\n";
}
FakeDriver* fatalDriver = nullptr;
void        failClosed(bool streamFailure)
{
    auto pid = fork();
    check(pid >= 0, "fork failed");
    if (pid == 0) {
        rlimit limit{0, 0};
        setrlimit(RLIMIT_CORE, &limit);
        std::set_terminate([] {
            std::_Exit(fatalDriver && fatalDriver->allocationCount() == 2 && fatalDriver->pendingCount() == 1 ? 86 :
                                                                                                                87);
        });
        auto d      = std::make_shared<FakeDriver>();
        fatalDriver = d.get();
        auto rt     = detail::RuntimeTestAccess::create(d);
        auto source = rt.allocate(128, MemoryKind::HostPinned), target = rt.allocate(128, MemoryKind::Device);
        auto stream = rt.createStream();
        auto done   = rt.copy(target.view(0, 128), source.view(0, 128), stream);
        source      = {};
        target      = {};
        d->failNext(streamFailure ? Fault::Synchronize : Fault::Wait);
        if (streamFailure)
            stream = {};
        else
            done = {};
        std::_Exit(88);
    }
    int status = 0;
    check(waitpid(pid, &status, 0) == pid, "waitpid failed");
    check(WIFEXITED(status) && WEXITSTATUS(status) == 86,
          "failed quiescence must terminate before releasing referenced memory");
    std::cout << "PASS CPU fake fail-closed " << (streamFailure ? "stream synchronize" : "event wait")
              << " preserves pending allocations\n";
}
void failClosedSubmission(bool recordFailure)
{
    auto pid = fork();
    check(pid >= 0, "fork failed");
    if (pid == 0) {
        rlimit limit{0, 0};
        setrlimit(RLIMIT_CORE, &limit);
        std::set_terminate([] {
            std::_Exit(fatalDriver && fatalDriver->synchronizeAttempts() == 1 && fatalDriver->allocationCount() == 2
                               && fatalDriver->pendingCount() == 1 ?
                           86 :
                           87);
        });
        auto d      = std::make_shared<FakeDriver>();
        fatalDriver = d.get();
        auto rt     = detail::RuntimeTestAccess::create(d);
        auto source = rt.allocate(128, MemoryKind::HostPinned), target = rt.allocate(128, MemoryKind::Device);
        auto producer = rt.createStream(), consumer = rt.createStream();
        auto event = rt.copy(target.view(0, 128), source.view(0, 128), producer);
        source     = {};
        target     = {};
        d->failSequence({recordFailure ? Fault::RecordAfter : Fault::StreamWaitAfter, Fault::Synchronize});
        if (recordFailure)
            rt.record(producer);
        else
            consumer.wait(event);
        std::_Exit(88);
    }
    int status = 0;
    check(waitpid(pid, &status, 0) == pid, "waitpid failed");
    check(WIFEXITED(status) && WEXITSTATUS(status) == 86,
          "failed submission drain must terminate before releasing pending resources");
    std::cout << "PASS CPU fake fail-closed " << (recordFailure ? "record" : "consumer wait")
              << " submission preserves pending allocations\n";
}
void stress()
{
    auto                           d  = std::make_shared<FakeDriver>();
    auto                           rt = detail::RuntimeTestAccess::create(d);
    std::vector<std::future<void>> tasks;
    for (int worker = 0; worker < 8; ++worker)
        tasks.push_back(std::async(std::launch::async, [rt] {
            for (int i = 0; i < 64; ++i)
                dependencies(rt);
        }));
    for (auto& task : tasks)
        task.get();
    empty(d);
    check(d->completedCopies() == 1024, "missing concurrent copies");
    std::cout
        << "PASS CPU fake 8 workers x 64 delayed producer/consumer cycles, 1024 verified copies, zero live resources\n";
}
int main(int argc, char** argv)
{
    try {
        if (argc == 2 && std::string(argv[1]) == "stress")
            stress();
        else {
            normalAndFailureTests();
            failClosed(false);
            failClosed(true);
            failClosedSubmission(false);
            failClosedSubmission(true);
        }
        return 0;
    }
    catch (const std::exception& e) {
        std::cerr << "FAIL " << e.what() << '\n';
        return 1;
    }
}
