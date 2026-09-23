/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/*
Authors:
Michael Berg <michael.berg@zalf.de>

Maintainers:
Currently maintained by the authors.

This file is part of the ZALF model and simulation infrastructure.
Copyright (C) Leibniz Centre for Agricultural Landscape Research (ZALF)
*/

// Tests for the FBP channel, driven through in-process capnp clients (no networking involved).
//
// Note on the test setup: calls on a local capnp client run the server method synchronously
// inside send(), while promise continuations only run when the event loop is turned. That makes
// the interleavings a channel has to survive - a writer being unblocked while a reader is about
// to block, a write being canceled while others wait, ... - reproducible instead of timing
// dependent. Use waitScope.poll() to let queued continuations run.

#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <map>
#include <string>
#ifndef _WIN32
#include <unistd.h>
#endif

#include <kj/async-io.h>
#include <kj/debug.h>
#include <kj/exception.h>
#include <kj/string.h>
#include <kj/vector.h>

#include <capnp/any.h>

#include "channel.h"

using namespace mas::infrastructure::common;

namespace {

typedef AnyPointerChannel::ChanReader::Client ReaderClient;
typedef AnyPointerChannel::ChanWriter::Client WriterClient;

// a channel plus the client capability keeping it alive
struct TestChannel {
  TestChannel(uint64_t bufferSize, kj::Timer& timer) : TestChannel(kj::heap<Channel>(
    "test", "channel under test", bufferSize, timer)) {}

  Channel& server() { return *_server; }

  ReaderClient reader(kj::WaitScope& ws) { return _client.readerRequest().send().wait(ws).getR(); }

  WriterClient writer(kj::WaitScope& ws) { return _client.writerRequest().send().wait(ws).getW(); }

  kj::Promise<void> setBufferSize(uint64_t size) {
    auto req = _client.setBufferSizeRequest();
    req.setSize(size);
    return req.send().ignoreResult();
  }

  kj::Promise<void> close(bool waitForEmptyBuffer) {
    auto req = _client.closeRequest();
    req.setWaitForEmptyBuffer(waitForEmptyBuffer);
    return req.send().ignoreResult();
  }

private:
  explicit TestChannel(kj::Own<Channel> channel) : _server(channel.get()), _client(kj::mv(channel)) {}

  Channel* _server;
  AnyPointerChannel::Client _client;
};

capnp::Request<AnyPointerMsg, AnyPointerChannel::ChanWriter::WriteResults>
writeRequest(WriterClient w, kj::StringPtr text) {
  auto req = w.writeRequest();
  req.getValue().setAs<capnp::Text>(text);
  return req;
}

kj::Promise<void> write(WriterClient w, kj::StringPtr text) {
  return writeRequest(w, text).send().ignoreResult();
}

kj::Promise<void> writeDone(WriterClient w) {
  auto req = w.writeRequest();
  req.setDone();
  return req.send().ignoreResult();
}

// write "<prefix>-0" .. "<prefix>-<count-1>", each write awaited before the next one is sent,
// which is how an FBP component writes into its out port
kj::Promise<void> writeSeq(WriterClient w, kj::String prefix, uint count, uint idx = 0) {
  if (idx >= count) return kj::READY_NOW;
  return write(w, kj::str(prefix, "-", idx))
         .then([w, prefix = kj::mv(prefix), count, idx]() mutable {
           return writeSeq(w, kj::mv(prefix), count, idx + 1);
         });
}

kj::String valueOf(capnp::AnyPointer::Reader value) { return kj::str(value.getAs<capnp::Text>()); }

// read count messages, collecting them in out
kj::Promise<void> readSeq(ReaderClient r, uint count, kj::Vector<kj::String>& out) {
  if (count == 0) return kj::READY_NOW;
  return r.readRequest().send().then([r, count, &out](auto&& msg) mutable -> kj::Promise<void> {
    KJ_REQUIRE(msg.isValue(), "reader received done/noMsg although more messages were expected",
               out.size(), count);
    out.add(valueOf(msg.getValue()));
    return readSeq(r, count - 1, out);
  });
}

// read until the channel reports that it is done, collecting all messages in out
kj::Promise<void> readUntilDone(ReaderClient r, kj::Vector<kj::String>& out) {
  return r.readRequest().send().then([r, &out](auto&& msg) mutable -> kj::Promise<void> {
    if (msg.isDone()) return kj::READY_NOW;
    KJ_REQUIRE(msg.isValue(), "reader received noMsg from a blocking read");
    out.add(valueOf(msg.getValue()));
    return readUntilDone(r, out);
  });
}

// read count messages, idling for gap between them (a reader which does some work per message)
kj::Promise<void> readSeqWithGaps(ReaderClient r, uint count, kj::Vector<kj::String>& out,
                                  kj::Timer& timer, kj::Duration gap) {
  if (count == 0) return kj::READY_NOW;
  return r.readRequest().send()
          .then([&out](auto&& msg) {
            KJ_REQUIRE(msg.isValue(), "reader received done/noMsg although more was expected");
            out.add(valueOf(msg.getValue()));
          })
          .then([&timer, gap]() { return timer.afterDelay(gap); })
          .then([r, count, &out, &timer, gap]() mutable {
            return readSeqWithGaps(r, count - 1, out, timer, gap);
          });
}

// check that every "<prefix w>-<i>" was received exactly once
void assertReceivedEachMessageOnce(kj::Vector<kj::String>& received, uint noOfWriters,
                                   uint msgsPerWriter) {
  KJ_ASSERT(received.size() == noOfWriters * msgsPerWriter, "wrong number of messages received",
            received.size(), noOfWriters * msgsPerWriter);
  std::map<std::string, int> counts;
  for (auto& msg : received) counts[msg.cStr()]++;
  for (uint w = 0; w < noOfWriters; w++) {
    for (uint i = 0; i < msgsPerWriter; i++) {
      auto expected = kj::str("w", w, "-", i);
      KJ_ASSERT(counts[expected.cStr()] == 1, "message not received exactly once", expected,
                counts[expected.cStr()]);
    }
  }
}

// ---------------------------------------------------------------------------------------- tests

// Several writers writing into one channel concurrently (the N-writers/1-reader pattern used for
// components with parallelProcesses > 1) must not lose a single message and no writer may fail.
void concurrentWritersLoseNoMessages(kj::WaitScope& ws, kj::Timer& timer) {
  constexpr uint noOfWriters = 10;
  constexpr uint msgsPerWriter = 20;

  TestChannel channel(1, timer);
  auto reader = channel.reader(ws);

  auto proms = kj::heapArrayBuilder<kj::Promise<void>>(noOfWriters + 1);
  kj::Vector<kj::String> received;
  proms.add(readSeq(reader, noOfWriters * msgsPerWriter, received));
  for (uint w = 0; w < noOfWriters; w++) {
    proms.add(writeSeq(channel.writer(ws), kj::str("w", w), msgsPerWriter));
  }
  kj::joinPromisesFailFast(proms.finish()).wait(ws);

  assertReceivedEachMessageOnce(received, noOfWriters, msgsPerWriter);
}

// The whole FBP lifecycle: several writers write concurrently into one channel and send done when
// they are finished, the reader reads until the channel is done. Neither a message in between nor
// the last one before the done message may get lost.
void concurrentWritersUntilDone(kj::WaitScope& ws, kj::Timer& timer) {
  constexpr uint noOfWriters = 8;
  constexpr uint msgsPerWriter = 25;

  TestChannel channel(4, timer);
  auto reader = channel.reader(ws);

  auto proms = kj::heapArrayBuilder<kj::Promise<void>>(noOfWriters + 1);
  kj::Vector<kj::String> received;
  proms.add(readUntilDone(reader, received));
  for (uint w = 0; w < noOfWriters; w++) {
    auto writer = channel.writer(ws);
    proms.add(writeSeq(writer, kj::str("w", w), msgsPerWriter)
              .then([writer]() mutable { return writeDone(writer); }));
  }
  kj::joinPromisesFailFast(proms.finish()).wait(ws);

  assertReceivedEachMessageOnce(received, noOfWriters, msgsPerWriter);
}

// The pattern of a flow whose workers are busy computing and then all write their one result at
// roughly the same time: the writers sit idle, wake up together, write once each and go back to
// sleep, while the reader takes its time between messages. That drives every message through the
// block/unblock path, unlike a continuous flood where the buffer rarely runs empty.
void burstyWritersLoseNoMessages(kj::WaitScope& ws, kj::Timer& timer) {
  constexpr uint noOfWriters = 10;
  constexpr uint noOfRounds = 15;

  TestChannel channel(1, timer);
  auto reader = channel.reader(ws);
  // the writers are created once up front, as the channel service does it
  kj::Vector<WriterClient> writers;
  for (uint w = 0; w < noOfWriters; w++) writers.add(channel.writer(ws));

  kj::Vector<kj::String> received;
  for (uint round = 0; round < noOfRounds; round++) {
    auto proms = kj::heapArrayBuilder<kj::Promise<void>>(noOfWriters + 1);
    proms.add(readSeqWithGaps(reader, noOfWriters, received, timer, 1 * kj::MILLISECONDS));
    for (uint w = 0; w < noOfWriters; w++) {
      proms.add(write(writers[w], kj::str("w", w, "-", round)));
    }
    kj::joinPromisesFailFast(proms.finish()).wait(ws);
  }

  KJ_ASSERT(received.size() == noOfWriters * noOfRounds, received.size());
  std::map<std::string, int> counts;
  for (auto& msg : received) counts[msg.cStr()]++;
  for (uint w = 0; w < noOfWriters; w++) {
    for (uint round = 0; round < noOfRounds; round++) {
      auto expected = kj::str("w", w, "-", round);
      KJ_ASSERT(counts[expected.cStr()] == 1, "message not received exactly once", expected,
                counts[expected.cStr()]);
    }
  }
}

// Same burst, but the reader only turns up once every writer is already blocked on the full
// buffer, so the whole burst has to be drained through the unblock path.
void readerArrivingAfterTheBurstGetsEverything(kj::WaitScope& ws, kj::Timer& timer) {
  constexpr uint noOfWriters = 10;

  TestChannel channel(1, timer);
  auto reader = channel.reader(ws);
  kj::Vector<WriterClient> writers;
  for (uint w = 0; w < noOfWriters; w++) writers.add(channel.writer(ws));

  auto proms = kj::heapArrayBuilder<kj::Promise<void>>(noOfWriters + 1);
  for (uint w = 0; w < noOfWriters; w++) proms.add(write(writers[w], kj::str("w", w)));
  ws.poll(); // one writer buffers its message, all others block

  kj::Vector<kj::String> received;
  proms.add(readSeqWithGaps(reader, noOfWriters, received, timer, 1 * kj::MILLISECONDS));
  kj::joinPromisesFailFast(proms.finish()).wait(ws);

  KJ_ASSERT(received.size() == noOfWriters, received.size());
  std::map<std::string, int> counts;
  for (auto& msg : received) counts[msg.cStr()]++;
  for (uint w = 0; w < noOfWriters; w++) {
    KJ_ASSERT(counts[kj::str("w", w).cStr()] == 1, "message not received exactly once", w);
  }
}

// Messages of a single writer have to arrive in the order they were written.
void messagesKeepTheirOrder(kj::WaitScope& ws, kj::Timer& timer) {
  constexpr uint noOfMsgs = 50;

  TestChannel channel(3, timer);
  auto reader = channel.reader(ws);

  kj::Vector<kj::String> received;
  auto readProm = readSeq(reader, noOfMsgs, received);
  writeSeq(channel.writer(ws), kj::str("msg"), noOfMsgs).wait(ws);
  readProm.wait(ws);

  KJ_ASSERT(received.size() == noOfMsgs, received.size());
  for (uint i = 0; i < noOfMsgs; i++) {
    KJ_ASSERT(received[i] == kj::str("msg-", i), received[i], i);
  }
}

// A writer which was blocked on a full buffer may not simply store its message in the buffer once
// it is unblocked: a reader may have started waiting in the meantime, in which case the message
// would sit in the buffer while the reader blocks forever.
void unblockedWriterReachesWaitingReader(kj::WaitScope& ws, kj::Timer& timer) {
  TestChannel channel(1, timer);
  auto reader = channel.reader(ws);

  auto w1 = write(channel.writer(ws), "first");  // -> buffered
  auto w2 = write(channel.writer(ws), "second"); // -> blocks, buffer is full

  auto r1 = reader.readRequest().send();         // -> takes "first", unblocks the second writer
  auto r2 = reader.readRequest().send();         // -> blocks, the second writer didn't run yet
  ws.poll();                                     // let the unblocked writer run

  KJ_ASSERT(r2.poll(ws), "message of the unblocked writer never reached the waiting reader");
  KJ_ASSERT(valueOf(r1.wait(ws).getValue()) == "first");
  KJ_ASSERT(valueOf(r2.wait(ws).getValue()) == "second");
  w1.wait(ws);
  w2.wait(ws);
}

// Canceling one blocked write (e.g. because that writer's process went away) may not disturb the
// other writers waiting on the same channel.
void canceledWriteLeavesOtherWritersAlone(kj::WaitScope& ws, kj::Timer& timer) {
  TestChannel channel(1, timer);
  auto reader = channel.reader(ws);
  auto writer = channel.writer(ws);

  auto buffered = write(writer, "buffered"); // -> buffered
  auto blocked1 = write(writer, "blocked1"); // -> blocked
  {
    auto canceled = write(writer, "canceled"); // -> blocked and dropped right away
  }
  auto blocked2 = write(writer, "blocked2"); // -> blocked
  ws.poll();

  kj::Vector<kj::String> received;
  readSeq(reader, 3, received).wait(ws);
  buffered.wait(ws);
  blocked1.wait(ws);
  blocked2.wait(ws);

  KJ_ASSERT(received.size() == 3, received.size());
  KJ_ASSERT(received[0] == "buffered", received[0]);
  KJ_ASSERT(received[1] == "blocked1", received[1]);
  KJ_ASSERT(received[2] == "blocked2", received[2]);
}

// Same for readers: canceling one blocked read may not disturb the other waiting readers.
void canceledReadLeavesOtherReadersAlone(kj::WaitScope& ws, kj::Timer& timer) {
  TestChannel channel(1, timer);
  auto reader = channel.reader(ws);

  auto r1 = reader.readRequest().send(); // -> blocked
  {
    auto canceled = reader.readRequest().send(); // -> blocked and dropped right away
  }
  auto r2 = reader.readRequest().send(); // -> blocked
  ws.poll();

  writeSeq(channel.writer(ws), kj::str("msg"), 2).wait(ws);

  KJ_ASSERT(valueOf(r1.wait(ws).getValue()) == "msg-0");
  KJ_ASSERT(valueOf(r2.wait(ws).getValue()) == "msg-1");
}

// Canceled calls must not wedge the channel. A buffer slot reserved for an unblocked writer has
// to be released again in every case - if one leaks, freeBufferSlots() stays 0 forever, no
// blocked writer is ever woken again and the channel silently stops forwarding without any error.
void canceledCallsDoNotWedgeTheChannel(kj::WaitScope& ws, kj::Timer& timer) {
  constexpr uint noOfRounds = 50;

  TestChannel channel(1, timer);
  auto reader = channel.reader(ws);
  auto writer1 = channel.writer(ws);
  auto writer2 = channel.writer(ws);
  auto writer3 = channel.writer(ws);

  for (uint round = 0; round < noOfRounds; round++) {
    // a read canceled while it is waiting on the empty channel
    {
      auto canceledRead = reader.readRequest().send();
      ws.poll();
    }
    ws.poll();

    auto buffered = write(writer1, kj::str("buffered-", round)); // -> buffered
    auto blocked = write(writer2, kj::str("blocked-", round));   // -> blocks on the full buffer
    {
      auto canceledWrite = write(writer3, kj::str("canceled-", round)); // -> blocks, then canceled
      ws.poll();
    }
    ws.poll();

    kj::Vector<kj::String> received;
    readSeq(reader, 2, received).wait(ws);
    buffered.wait(ws);
    blocked.wait(ws);

    KJ_ASSERT(received.size() == 2, received.size());
    KJ_ASSERT(received[0] == kj::str("buffered-", round), received[0], round);
    KJ_ASSERT(received[1] == kj::str("blocked-", round), received[1], round);
  }
}

// A read is a destructive take: the moment the channel hands a message to a waiting reader the
// message is gone from the channel, and the read() RPC carries it to the client. If the client
// then drops that call - an asyncio task cancellation, a race lost against another port - the
// message dies with the response and nobody notices: the writer was told the write succeeded.
//
// This pins that property rather than endorsing it. A client must never cancel a read it might
// still need; fixing it on this side needs an acknowledged read (hand out the message, keep it
// until the reader confirms it), which is a schema change.
void canceledReadAfterHandoverLosesTheMessage(kj::WaitScope& ws, kj::Timer& timer) {
  TestChannel channel(1, timer);
  auto reader = channel.reader(ws);
  auto writer = channel.writer(ws);

  {
    auto read = reader.readRequest().send(); // -> waits in the channel
    ws.poll();
    write(writer, "handed over").wait(ws);   // -> handed to the waiting reader, write reports success
    KJ_ASSERT(read.poll(ws), "the message should have reached the read call");
    // the client drops the call without ever looking at the response
  }
  ws.poll();

  auto next = reader.readRequest().send();
  KJ_ASSERT(!next.poll(ws), "channel unexpectedly kept the message - has read become acknowledged?");
}

// The channel must not tell a reader that the writers are done while there are still buffered
// messages, neither for a reader asking afterwards nor for one already waiting.
void doneIsSentOnlyAfterTheBufferIsEmpty(kj::WaitScope& ws, kj::Timer& timer) {
  TestChannel channel(3, timer);
  auto reader = channel.reader(ws);
  auto writer = channel.writer(ws);

  writeSeq(writer, kj::str("msg"), 2).wait(ws);
  writeDone(writer).wait(ws); // last writer gone -> FBP semantics close the channel down

  auto r1 = reader.readRequest().send().wait(ws);
  KJ_ASSERT(r1.isValue() && valueOf(r1.getValue()) == "msg-0");
  auto r2 = reader.readRequest().send().wait(ws);
  KJ_ASSERT(r2.isValue() && valueOf(r2.getValue()) == "msg-1");
  auto r3 = reader.readRequest().send().wait(ws);
  KJ_ASSERT(r3.isDone(), "reader should have been told that the channel is done");
}

// A reader already waiting when the last writer sends done gets the done message.
void waitingReaderIsClosedDown(kj::WaitScope& ws, kj::Timer& timer) {
  TestChannel channel(1, timer);
  auto reader = channel.reader(ws);
  auto writer = channel.writer(ws);

  auto r = reader.readRequest().send(); // -> blocks
  ws.poll();
  writeDone(writer).wait(ws);

  KJ_ASSERT(r.poll(ws), "waiting reader was not woken up by the done message");
  KJ_ASSERT(r.wait(ws).isDone());
}

// Increasing the buffer size has to unblock writers waiting for space.
void growingTheBufferUnblocksWriters(kj::WaitScope& ws, kj::Timer& timer) {
  TestChannel channel(1, timer);
  auto reader = channel.reader(ws);

  auto w1 = write(channel.writer(ws), "first");  // -> buffered
  auto w2 = write(channel.writer(ws), "second"); // -> blocked
  ws.poll();
  KJ_ASSERT(!w2.poll(ws), "writer should still be blocked on the full buffer");

  channel.setBufferSize(4).wait(ws);
  KJ_ASSERT(w2.poll(ws), "writer was not unblocked although the buffer grew");
  w1.wait(ws);
  w2.wait(ws);

  kj::Vector<kj::String> received;
  readSeq(reader, 2, received).wait(ws);
  KJ_ASSERT(received[0] == "first" && received[1] == "second");
}

// writeIfSpace must not block, it reports whether the message was taken.
void writeIfSpaceReportsAFullBuffer(kj::WaitScope& ws, kj::Timer& timer) {
  TestChannel channel(2, timer);
  auto reader = channel.reader(ws);
  auto writer = channel.writer(ws);

  for (uint i = 0; i < 2; i++) {
    auto req = writer.writeIfSpaceRequest();
    req.getValue().setAs<capnp::Text>(kj::str("msg-", i));
    KJ_ASSERT(req.send().wait(ws).getSuccess(), "buffer should still have space", i);
  }

  auto req = writer.writeIfSpaceRequest();
  req.getValue().setAs<capnp::Text>("too much");
  KJ_ASSERT(!req.send().wait(ws).getSuccess(), "buffer should be full");

  kj::Vector<kj::String> received;
  readSeq(reader, 2, received).wait(ws);
  KJ_ASSERT(received[0] == "msg-0" && received[1] == "msg-1");
}

// Closing the channel may not leave blocked writers hanging (their promises would be destroyed
// unfulfilled, which the writers see as "PromiseFulfiller was destroyed without fulfilling ...").
void closingTheChannelReleasesBlockedWriters(kj::WaitScope& ws, kj::Timer& timer) {
  TestChannel channel(1, timer);
  auto reader = channel.reader(ws);

  auto w1 = write(channel.writer(ws), "first");  // -> buffered
  auto w2 = write(channel.writer(ws), "second"); // -> blocked
  auto w3 = write(channel.writer(ws), "third");  // -> blocked
  ws.poll();

  channel.close(false).wait(ws);
  w1.wait(ws);
  w2.wait(ws);
  w3.wait(ws);
}

// ------------------------------------------------------------------------------------ test main

// A test that waits forever means a message was lost or a waiter was never woken up - which is
// exactly the failure mode this file is about, so turn it into a test failure instead of a hang.
constexpr uint TEST_TIMEOUT_IN_SECONDS = 30;
const char* currentTest = "";

#ifndef _WIN32
extern "C" void onTestTimeout(int) {
  const char* msg = "TIMEOUT ";
  auto ignore = [](ssize_t) {};
  ignore(::write(2, msg, ::strlen(msg)));
  ignore(::write(2, currentTest, ::strlen(currentTest)));
  ignore(::write(2, " (waited forever - lost message or waiter never woken up)\n", 57));
  _exit(3);
}
#endif

void startWatchdog(kj::StringPtr testName) {
#ifndef _WIN32
  currentTest = testName.cStr();
  ::signal(SIGALRM, &onTestTimeout);
  ::alarm(TEST_TIMEOUT_IN_SECONDS);
#endif
}

void stopWatchdog() {
#ifndef _WIN32
  ::alarm(0);
#endif
}

typedef void (*TestFn)(kj::WaitScope&, kj::Timer&);

struct Test {
  kj::StringPtr name;
  TestFn fn;
};

const Test TESTS[] = {
  {"concurrentWritersLoseNoMessages", &concurrentWritersLoseNoMessages},
  {"concurrentWritersUntilDone", &concurrentWritersUntilDone},
  {"burstyWritersLoseNoMessages", &burstyWritersLoseNoMessages},
  {"readerArrivingAfterTheBurstGetsEverything", &readerArrivingAfterTheBurstGetsEverything},
  {"messagesKeepTheirOrder", &messagesKeepTheirOrder},
  {"unblockedWriterReachesWaitingReader", &unblockedWriterReachesWaitingReader},
  {"canceledWriteLeavesOtherWritersAlone", &canceledWriteLeavesOtherWritersAlone},
  {"canceledReadLeavesOtherReadersAlone", &canceledReadLeavesOtherReadersAlone},
  {"canceledCallsDoNotWedgeTheChannel", &canceledCallsDoNotWedgeTheChannel},
  {"canceledReadAfterHandoverLosesTheMessage", &canceledReadAfterHandoverLosesTheMessage},
  {"doneIsSentOnlyAfterTheBufferIsEmpty", &doneIsSentOnlyAfterTheBufferIsEmpty},
  {"waitingReaderIsClosedDown", &waitingReaderIsClosedDown},
  {"growingTheBufferUnblocksWriters", &growingTheBufferUnblocksWriters},
  {"writeIfSpaceReportsAFullBuffer", &writeIfSpaceReportsAFullBuffer},
  {"closingTheChannelReleasesBlockedWriters", &closingTheChannelReleasesBlockedWriters},
};

} // namespace

int main(int argc, char* argv[]) {
  kj::StringPtr only = argc > 1 ? kj::StringPtr(argv[1]) : nullptr;

  // CHANNEL_TEST_VERBOSE=1 shows the channel's own KJ_LOG(INFO, ...) trace
  if (getenv("CHANNEL_TEST_VERBOSE") != nullptr) kj::_::Debug::setLogLevel(kj::LogSeverity::INFO);

  uint failed = 0;
  uint run = 0;
  for (auto& test : TESTS) {
    if (only != nullptr && test.name != only) continue;
    run++;
    auto io = kj::setupAsyncIo();
    startWatchdog(test.name);
    KJ_IF_MAYBE(e, kj::runCatchingExceptions([&]() {
      test.fn(io.waitScope, io.provider->getTimer());
    })) {
      stopWatchdog();
      failed++;
      std::cout << "FAIL " << test.name.cStr() << std::endl << "     " << kj::str(*e).cStr() << std::endl;
    } else {
      stopWatchdog();
      std::cout << "ok   " << test.name.cStr() << std::endl;
    }
  }

  if (run == 0) {
    std::cout << "no test matched '" << only.cStr() << "'" << std::endl;
    return 2;
  }
  std::cout << (run - failed) << "/" << run << " channel tests passed" << std::endl;
  return failed == 0 ? 0 : 1;
}
