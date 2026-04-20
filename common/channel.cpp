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

#include "channel.h"

#include <deque>
#include <tuple>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <numeric>

#include <kj/async.h>
#include <kj/common.h>
#include <kj/debug.h>
#include <kj/exception.h>

#include <capnp/capability.h>
#include <capnp/dynamic.h>
#include <capnp/message.h>
#include <capnp/schema.h>

#include "sole.hpp"

using namespace std;
using namespace mas::infrastructure::common;

struct Channel::Impl {
  Channel& self;
  mas::infrastructure::common::Restorer* restorer{nullptr};
  kj::Timer& timer;
  kj::String id;
  kj::String name{kj::str("Channel")};
  kj::String description;
  kj::HashMap<kj::String, AnyPointerChannel::ChanReader::Client> readers;
  kj::HashMap<kj::String, AnyPointerChannel::ChanWriter::Client> writers;
  std::deque<kj::Own<kj::PromiseFulfiller<kj::Maybe<AnyPointerMsg::Reader>>>> blockingReadFulfillers;
  std::deque<kj::Own<kj::PromiseFulfiller<void>>> blockingWriteFulfillers;
  uint64_t bufferSize{1};
  std::deque<kj::Own<kj::Decay<AnyPointerMsg::Reader>>> buffer;
  AnyPointerChannel::CloseSemantics autoCloseSemantics{AnyPointerChannel::CloseSemantics::FBP};
  bool sendCloseOnEmptyBuffer{false};
  AnyPointerChannel::Client client{nullptr};
  // mas::schema::common::Action::Client unregisterAction{nullptr};
  bool channelShouldBeClosedOnEmptyBuffer = false;
  bool channelCanBeClosed = false;
  kj::Own<kj::PromiseFulfiller<void>> closeChannelFulfiller;
  uint64_t totalNoOfIpsReceived{0};

  struct StatsCB {
    AnyPointerChannel::StatsCallback::Client callback;
    uint32_t updateIntervalInMs{1000};
    uint32_t dueInMs{0};
  };

  kj::HashMap<kj::String, StatsCB> statsCBs;
  kj::HashMap<kj::String, StatsCB> immediateStatsCBs;

  class UnregStatsCallback : public AnyPointerChannel::StatsCallback::Unregister::Server {
    kj::HashMap<kj::String, StatsCB>& _statsCBs;
    kj::String _id;

  public:
    UnregStatsCallback(kj::HashMap<kj::String, StatsCB>& statsCBs, kj::StringPtr id) : _statsCBs(statsCBs)
      , _id(kj::str(id)) {}

    ~UnregStatsCallback() = default;

    kj::Promise<void> unreg(UnregContext context) override {
      // KJ_DBG("UnregStatsCallback::unreg:", _id);
      _statsCBs.erase(_id);
      return kj::READY_NOW;
    }
  };

  AnyPointerChannel::StatsCallback::Unregister::Client createAndStoreStatsCallback(
    AnyPointerChannel::StatsCallback::Client statsCB,
    uint32_t updateIntervalInMs) {
    auto id = kj::str(sole::uuid4().str());
    // KJ_DBG(updateIntervalInMs, id, "before statsCBs:", statsCBs.size(), immediateStatsCBs.size());
    if (updateIntervalInMs == 0) {
      immediateStatsCBs.insert(kj::str(id), Impl::StatsCB{statsCB, 0, 0});
      // KJ_DBG("after immediateStatsCB:", immediateStatsCBs.size());
      return kj::heap<UnregStatsCallback>(immediateStatsCBs, id);
    }
    statsCBs.insert(kj::str(id), Impl::StatsCB{statsCB, updateIntervalInMs, updateIntervalInMs});
    // KJ_DBG(updateIntervalInMs, id, "after statsCBs:", statsCBs.size());
    return kj::heap<UnregStatsCallback>(statsCBs, id);
  }

  std::string formatLocalTimeWithMillis(std::chrono::system_clock::time_point tp) {
    using namespace std::chrono;

    auto tt = system_clock::to_time_t(tp);
    std::tm tm = *std::localtime(&tt);

    //    auto ms = duration_cast<milliseconds>(tp.time_since_epoch()) % 1000;

    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
    //<< '.' << std::setw(3) << std::setfill('0') << ms.count();

    return oss.str();
  }


  kj::Promise<void> sendStats() {
    KJ_LOG(INFO, "sendStats", statsCBs.size());
    bool listenersAttached = statsCBs.size() > 0;

    // no listeners for stats attached
    if (!listenersAttached) {
      KJ_LOG(INFO, "no listeners", statsCBs.size());
      return timer.afterDelay(1000 * kj::MILLISECONDS).then([this] { return sendStats(); });
    }

    auto proms = kj::heapArrayBuilder<kj::Promise<void>>(statsCBs.size() + 1);
    uint32_t minIntervalInMs = 0;
    // update due time
    for (auto& [key, statsCB] : statsCBs) {
      minIntervalInMs = minIntervalInMs == 0
                          ? statsCB.updateIntervalInMs
                          : std::min(minIntervalInMs, statsCB.updateIntervalInMs);
    }
    uint32_t minDueInMs = minIntervalInMs;

    for (auto& [id, statsCB] : statsCBs) {
      // KJ_DBG(id, statsCB.dueInMs, statsCB.updateIntervalInMs);

      auto req = statsCB.callback.statusRequest();
      auto stats = req.initStats();
      fillInStats(stats, statsCB.updateIntervalInMs);

      // count down the due time for the next update
      auto dueInMs = static_cast<int64_t>(statsCB.dueInMs) - minIntervalInMs;
      // send update for this listener
      if (dueInMs <= 0) {
        statsCB.dueInMs = statsCB.updateIntervalInMs;
        // KJ_DBG("trying to send stats", statsCBs.size());
        proms.add(req.send().then([](auto&& res) {},
                                  [this, id = kj::str(id)](kj::Exception&& err) {
                                    // KJ_DBG("Error sending stats.", err);
                                    statsCBs.erase(id);
                                  }));
      } else {
        statsCB.dueInMs = static_cast<uint32_t>(dueInMs);
        proms.add(kj::READY_NOW);
      }

      minDueInMs = std::min(minDueInMs, statsCB.dueInMs);
    }

    auto newDelay = std::min(minDueInMs, minIntervalInMs);
    // KJ_DBG(newDelay, minIntervalInMs);
    proms.add(timer.afterDelay(newDelay * kj::MILLISECONDS).then([] {}));
    return kj::joinPromises(proms.finish()).then([this] { return sendStats(); });
  }

  kj::Promise<void> sendImmediateStats() {
    KJ_LOG(INFO, "sendImmediateStats", immediateStatsCBs.size());
    if (immediateStatsCBs.size() > 0) {
      auto proms = kj::heapArrayBuilder<kj::Promise<void>>(immediateStatsCBs.size());

      for (auto& [id, statsCB] : immediateStatsCBs) {
        // KJ_DBG("sendImmediateStats:", id, statsCB.dueInMs, statsCB.updateIntervalInMs);

        auto req = statsCB.callback.statusRequest();
        auto stats = req.initStats();
        fillInStats(stats, statsCB.updateIntervalInMs);

        // KJ_DBG("trying to send stats", immediateStatsCBs.size());
        proms.add(req.send().then([](auto&& res) {},
                                  [this, id = kj::str(id)](kj::Exception&& err) {
                                    // KJ_DBG("Error sending immediate stats.", err);
                                    immediateStatsCBs.erase(id);
                                  }));
      }

      return kj::joinPromises(proms.finish());
    }

    KJ_LOG(INFO, "there are no immediate stats to send, ready now");
    return kj::READY_NOW;
  }

  void fillInStats(AnyPointerChannel::StatsCallback::Stats::Builder stats, uint32_t updateIntervalInMs) {
    stats.setNoOfWaitingWriters(blockingWriteFulfillers.size());
    stats.setNoOfWaitingReaders(blockingReadFulfillers.size());
    stats.setNoOfIpsInQueue(buffer.size());
    stats.setTotalNoOfIpsReceived(totalNoOfIpsReceived);
    stats.setUpdateIntervalInMs(updateIntervalInMs);
    const auto now = std::chrono::system_clock::now();
    auto time = formatLocalTimeWithMillis(now);
    stats.setTimestamp(time);
  }

  Impl(Channel& self, mas::infrastructure::common::Restorer* restorer, kj::StringPtr name,
       kj::StringPtr description,
       uint64_t bufferSize,
       kj::Timer& timer)
  : self(self)
  , timer(timer)
  , id(kj::str(sole::uuid4().str()))
  , name(kj::str(name))
  , description(kj::str(description))
  , bufferSize(std::max(static_cast<uint64_t>(1), bufferSize)) {
    setRestorer(restorer);
  }

  ~Impl() = default;

  void setRestorer(mas::infrastructure::common::Restorer* restorer) {
    if (restorer != nullptr) {
      this->restorer = restorer;
      // restorer->setRestoreCallback([this](kj::StringPtr containerId) -> capnp::Capability::Client {
      //   if(containerId == nullptr) return client;
      //   else return loadContainer(containerId);
      // });
    }
  }

  AnyPointerChannel::ChanReader::Client createReader() {
    auto r = kj::heap<Reader>(self);
    auto id = r->id();
    AnyPointerChannel::ChanReader::Client rc = kj::mv(r);
    readers.insert(kj::str(id), rc);
    return rc;
  }

  AnyPointerChannel::ChanWriter::Client createWriter() {
    auto w = kj::heap<Writer>(self);
    auto id = w->id();
    AnyPointerChannel::ChanWriter::Client wc = kj::mv(w);
    writers.insert(kj::str(id), wc);
    return wc;
  }
};

Channel::Channel(kj::StringPtr name, kj::StringPtr description, uint64_t bufferSize, kj::Timer& timer,
                 Restorer* restorer)
: impl(kj::heap<Impl>(*this, restorer, name, description, bufferSize, timer)) {}

Channel::~Channel() = default;

kj::Promise<void> Channel::info(InfoContext context) {
  KJ_LOG(INFO, "Channel::info: message received");
  auto rs = context.getResults();
  rs.setId(impl->id);
  rs.setName(impl->name);
  rs.setDescription(impl->description);
  return kj::READY_NOW;
}

kj::Promise<void> Channel::save(SaveContext context) {
  KJ_LOG(INFO, "Channel::save: message received");
  if (impl->restorer) {
    return impl->restorer->save(impl->client, context.getResults().initSturdyRef(), context.getResults().initUnsaveSR())
               .ignoreResult();
  }
  return kj::READY_NOW;
}

void Channel::closedReader(kj::StringPtr readerId) {
  impl->readers.erase(readerId);
  // now that all readers disconnected, turn of auto-closing readers
  if (kj::size(impl->readers) == 0) impl->sendCloseOnEmptyBuffer = false;
  KJ_LOG(INFO, "Channel::closedReader: number of readers left:", kj::size(impl->readers));
  // cout << "Channel::closedReader: number of readers left:" << kj::size(impl->readers) << endl;
}

void Channel::closedWriter(kj::StringPtr writerId) {
  impl->writers.erase(writerId);
  KJ_LOG(INFO, "Channel::closedWriter: number of writers left:", kj::size(impl->writers), impl->autoCloseSemantics);
  // cout << "Channel::closedWriter: number of writers left:" << kj::size(impl->writers) << " FBP Close semantics: " <<
  // (impl->autoCloseSemantics == AnyPointerChannel::CloseSemantics::FBP ? "true" : "false")  << endl; cout <<
  // "Channel::closedWriter:number of readers:" << kj::size(impl->readers) << " FBP Close semantics: " <<
  // (impl->autoCloseSemantics == AnyPointerChannel::CloseSemantics::FBP)  << endl;

  if (impl->autoCloseSemantics == AnyPointerChannel::CloseSemantics::FBP && kj::size(impl->writers) == 0) {
    impl->sendCloseOnEmptyBuffer = true;
    KJ_LOG(INFO, "Channel::closedWriter: FBP semantics and no writers left -> sending done to readers");
    // cout << "Channel::closedWriter: FBP semantics and no writers left -> sending done to readers" << endl;

    // as we just received a done message which should be distributed and would
    // fill the buffer, unblock all readers, so they send the done message
    while (kj::size(impl->blockingReadFulfillers) > 0) {
      auto& brf = impl->blockingReadFulfillers.back();
      brf->fulfill(nullptr); // kj::Maybe<AnyPointerMsg::Reader>());
      impl->blockingReadFulfillers.pop_back();
      KJ_LOG(INFO, "Channel::closedWriter: sent done to reader on last finished writer");
      // cout << "Channel::closedWriter: sent done to reader on last finished writer" << endl;
    }
    KJ_LOG(INFO, kj::size(impl->blockingReadFulfillers));
    KJ_LOG(INFO, kj::size(impl->blockingWriteFulfillers));
  }
}

kj::Promise<void> Channel::setBufferSize(SetBufferSizeContext context) {
  KJ_LOG(INFO, "Channel::setBufferSize: message received");
  impl->bufferSize = context.getParams().getSize();
  return kj::READY_NOW;
}

kj::Promise<void> Channel::reader(ReaderContext context) {
  KJ_LOG(INFO, "Channel::reader: message received");
  context.getResults().setR(impl->createReader());
  return kj::READY_NOW;
}

kj::Promise<void> Channel::writer(WriterContext context) {
  KJ_LOG(INFO, "Channel::writer: info message received");
  context.getResults().setW(impl->createWriter());
  return kj::READY_NOW;
}

kj::Promise<void> Channel::endpoints(EndpointsContext context) {
  KJ_LOG(INFO, "Channel::endpoints: message received");
  context.getResults().setR(impl->createReader());
  context.getResults().setW(impl->createWriter());
  return kj::READY_NOW;
}

kj::Promise<void> Channel::setAutoCloseSemantics(SetAutoCloseSemanticsContext context) {
  KJ_LOG(INFO, "Channel::setAutoCloseSemantics: message received", context.getParams().getCs());
  impl->autoCloseSemantics = context.getParams().getCs();
  return kj::READY_NOW;
}

kj::Promise<void> Channel::closeChannel() {
  auto paf = kj::newPromiseAndFulfiller<void>();
  impl->closeChannelFulfiller = kj::mv(paf.fulfiller);
  return kj::mv(paf.promise);
}

kj::Promise<void> Channel::close(CloseContext context) {
  KJ_LOG(INFO, "Channel::close: message received", context.getParams().getWaitForEmptyBuffer());
  if (!context.getParams().getWaitForEmptyBuffer() || impl->buffer.empty()) {
    impl->channelCanBeClosed = true;
    impl->closeChannelFulfiller->fulfill();
  } else {
    impl->channelShouldBeClosedOnEmptyBuffer = true;
    impl->sendCloseOnEmptyBuffer = true;
  }
  return kj::READY_NOW;
}

kj::Promise<void> Channel::registerStatsCallback(RegisterStatsCallbackContext context) {
  KJ_LOG(INFO, "Channel::registerStatsCallback: message received");
  auto unregCB = impl->createAndStoreStatsCallback(
                                                   context.getParams().getCallback(),
                                                   context.getParams().
                                                           getUpdateIntervalInMs());
  context.getResults().setUnregisterCallback(unregCB);
  return kj::READY_NOW;
}

kj::Promise<void> Channel::sendStats() { return impl->sendStats(); }

AnyPointerChannel::Client Channel::getClient() { return impl->client; }

void Channel::setClient(AnyPointerChannel::Client c) { impl->client = c; }

// mas::schema::common::Action::Client Channel::getUnregisterAction() { return impl->unregisterAction; }
// void Channel::setUnregisterAction(mas::schema::common::Action::Client unreg) { impl->unregisterAction = unreg; }

void Channel::setRestorer(mas::infrastructure::common::Restorer* restorer) { impl->setRestorer(restorer); }

Reader::Reader(Channel& c) : _channel(c)
                           , _id(kj::str(sole::uuid4().str())) {}

kj::Promise<void> Reader::info(InfoContext context) {
  KJ_LOG(INFO, "Reader::info: message received");
  auto rs = context.getResults();
  auto channelNameOrId = kj::str(_channel.impl->name.size() > 0 ? _channel.impl->name : _channel.impl->id);
  rs.setId(_id);
  rs.setName(kj::str(channelNameOrId, "::", _id));
  rs.setDescription(
                    kj::str("Port (ID: ", _id, ") @ Channel '", _channel.impl->name, "' (ID: ", _channel.impl->id,
                            ")"));
  return kj::READY_NOW;
}

kj::Promise<void> Reader::save(SaveContext context) {
  KJ_LOG(INFO, "Reader::save: message received");
  if (_channel.impl->restorer) {
    KJ_IF_MAYBE(client, _channel.impl->readers.find(_id)) {
      return _channel.impl->restorer
                     ->save(*client, context.getResults().initSturdyRef(), context.getResults().initUnsaveSR())
                     .ignoreResult();
    }
  }
  return kj::READY_NOW;
}

kj::Promise<void> Reader::read(ReadContext context) {
  KJ_REQUIRE(!_closed, "Reader already closed.", _closed);

  auto& c = _channel;
  auto& b = c.impl->buffer;

  // the buffer is not empty, send next value
  if (!b.empty()) {
    KJ_LOG(INFO, "Reader::read: buffer not empty, send next value");
    auto&& v = b.back();
    KJ_ASSERT(v.get()->isValue(), "Msg contains a value, because before buffering we checked for done.");
    context.getResults().setValue(v.get()->getValue());
    b.pop_back();

    // unblock a writer unless we're about to close down
    if (!c.impl->blockingWriteFulfillers.empty() && !c.impl->sendCloseOnEmptyBuffer) {
      KJ_LOG(INFO, "Reader::read: unblock next writer");
      auto&& bwf = c.impl->blockingWriteFulfillers.back();
      bwf->fulfill();
      c.impl->blockingWriteFulfillers.pop_back();
    }

    // check if the channel is supposed to be closed and just waiting for an empty buffer
    if (b.empty() && c.impl->channelShouldBeClosedOnEmptyBuffer) {
      c.impl->channelCanBeClosed = true;
      c.impl->closeChannelFulfiller->fulfill();
    }

    return kj::READY_NOW;
  }

  // don't read if the channel is supposed to close
  if (c.impl->channelCanBeClosed) {
    return kj::READY_NOW;
  }

  // buffer is empty, but we are supposed to close down
  if (c.impl->sendCloseOnEmptyBuffer) {
    KJ_LOG(INFO, "Reader::read: buffer is empty, but close down");
    context.getResults().setDone();
    c.closedReader(id());

    // if there are other readers waiting close them as well
    while (!c.impl->blockingReadFulfillers.empty()) {
      KJ_LOG(INFO, "Reader::read: close other waiting readers");
      auto&& brf = c.impl->blockingReadFulfillers.back();
      brf->fulfill(nullptr);
      c.impl->blockingReadFulfillers.pop_back();
    }

    return kj::READY_NOW;
  }

  KJ_LOG(INFO, "Reader::read: block, because no value to read");
  auto paf = kj::newPromiseAndFulfiller<kj::Maybe<AnyPointerMsg::Reader>>();
  auto* fulfillerPtr = paf.fulfiller.get();
  c.impl->blockingReadFulfillers.push_front(kj::mv(paf.fulfiller));

  // This guard runs its lambda when it is destroyed (i.e. when the promise is canceled).
  auto cancelGuard = kj::defer([this, fulfillerPtr]() {
    auto& q = _channel.impl->blockingReadFulfillers;
    for (auto it = q.begin(); it != q.end(); ++it) {
      if (it->get() == fulfillerPtr) {
        q.erase(it);
        KJ_LOG(INFO, "Reader::read: canceled, fulfiller removed from queue");
        break;
      }
    }
  });

  return kj::mv(paf.promise)
         .then([context, this](kj::Maybe<AnyPointerMsg::Reader> msg) mutable {
           KJ_REQUIRE(!_closed, "Reader already closed.", _closed);

           if (_channel.impl->sendCloseOnEmptyBuffer && msg == nullptr) {
             context.getResults().setDone();
             KJ_LOG(INFO, "Reader::read: promise_lambda: sending done to reader");
             _channel.closedReader(id());
           } else {
             KJ_IF_MAYBE(m, msg) {
               context.getResults().setValue(m->getValue());
               KJ_LOG(INFO, "Reader::read: promise_lambda: sending value to reader");
             }
           }
         })
         .attach(kj::mv(cancelGuard));
}

kj::Promise<void> Reader::readIfMsg(ReadIfMsgContext context) {
  KJ_REQUIRE(!_closed, "Reader already closed.", _closed);

  auto& c = _channel;
  auto& b = c.impl->buffer;

  // the buffer is not empty, send next the value
  if (!b.empty()) {
    KJ_LOG(INFO, "Reader::readIfMsg: buffer not empty, send next value");
    auto&& v = b.back();
    KJ_ASSERT(v.get()->isValue(), "Msg contains a value, because before buffering we checked for done.");
    context.getResults().setValue(v.get()->getValue());
    b.pop_back();

    // unblock a writer unless we're about to close down
    if (!c.impl->blockingWriteFulfillers.empty() && !c.impl->sendCloseOnEmptyBuffer) {
      KJ_LOG(INFO, "Reader::readIfMsg: unblock next writer");
      auto&& bwf = c.impl->blockingWriteFulfillers.back();
      bwf->fulfill();
      c.impl->blockingWriteFulfillers.pop_back();
    }

    // check if the channel is supposed to be closed and just waiting for an empty buffer
    if (b.empty() && c.impl->channelShouldBeClosedOnEmptyBuffer) {
      c.impl->channelCanBeClosed = true;
      c.impl->closeChannelFulfiller->fulfill();
    }

    return kj::READY_NOW;
  }

  // don't read if the channel is supposed to close
  if (c.impl->channelCanBeClosed) {
    return kj::READY_NOW;
  }

  // buffer is empty, but we are supposed to close down
  if (c.impl->sendCloseOnEmptyBuffer) {
    KJ_LOG(INFO, "Reader::readIfMsg: buffer is empty, but close down");
    context.getResults().setDone();
    c.closedReader(id());

    // if there are other readers waiting close them as well
    while (!c.impl->blockingReadFulfillers.empty()) {
      KJ_LOG(INFO, "Reader::readIfMsg: close other waiting readers");
      auto&& brf = c.impl->blockingReadFulfillers.back();
      brf->fulfill(nullptr);
      c.impl->blockingReadFulfillers.pop_back();
    }
    return kj::READY_NOW;
  }

  // no message to read, return that information
  KJ_LOG(INFO, "Reader::readIfMsg: return noMsg, because no value to read");
  context.getResults().setNoMsg();
  return kj::READY_NOW;
}

kj::Promise<void> Reader::close(CloseContext context) {
  KJ_LOG(INFO, "Reader::close: received close message id: ", id());
  _channel.closedReader(id());
  return kj::READY_NOW;
}

Writer::Writer(Channel& c) : _channel(c)
                           , _id(kj::str(sole::uuid4().str())) {}

kj::Promise<void> Writer::info(InfoContext context) {
  KJ_LOG(INFO, "Writer::info: message received");
  auto rs = context.getResults();
  auto channelNameOrId = kj::str(_channel.impl->name.size() > 0 ? _channel.impl->name : _channel.impl->id);
  rs.setId(_id);
  rs.setName(kj::str(channelNameOrId, "::", _id));
  rs.setDescription(
                    kj::str("Port (ID: ", _id, ") @ Channel '", _channel.impl->name, "' (ID: ", _channel.impl->id,
                            ")"));
  return kj::READY_NOW;
}

kj::Promise<void> Writer::save(SaveContext context) {
  KJ_LOG(INFO, "Writer::save: message received");
  if (_channel.impl->restorer) {
    KJ_IF_MAYBE(client, _channel.impl->writers.find(_id)) {
      return _channel.impl->restorer
                     ->save(*client, context.getResults().initSturdyRef(), context.getResults().initUnsaveSR())
                     .ignoreResult();
    }
  }
  return kj::READY_NOW;
}

kj::Promise<void> Writer::write(WriteContext context) {
  KJ_REQUIRE(!_closed, "Writer already closed.", _closed);

  auto v = context.getParams();
  auto& c = _channel;
  auto& b = c.impl->buffer;

  // don't accept any further writes if the channel is supposed to be closed (now or when the buffer is empty)
  if (c.impl->channelCanBeClosed || c.impl->channelShouldBeClosedOnEmptyBuffer) {
    KJ_LOG(INFO, "Writer::write:", c.impl->channelCanBeClosed, c.impl->channelShouldBeClosedOnEmptyBuffer);
    return c.impl->sendImmediateStats(); //kj::READY_NOW;
  }

  // if we received a done, this writer can be removed
  if (v.isDone()) {
    KJ_LOG(INFO, "Writer::write: received done -> remove writer", id());
    c.closedWriter(id());
    return c.impl->sendImmediateStats(); //kj::READY_NOW;
  }

  // there's a reader waiting
  if (!c.impl->blockingReadFulfillers.empty()) {
    KJ_LOG(INFO, "Writer::write: unblock waiting reader");
    auto&& brf = c.impl->blockingReadFulfillers.back();
    brf->fulfill(v);
    c.impl->totalNoOfIpsReceived++;
    c.impl->blockingReadFulfillers.pop_back();
    return c.impl->sendImmediateStats(); //kj::READY_NOW;
  }

  // there space to store the message
  if (b.size() < c.impl->bufferSize) {
    KJ_LOG(INFO, "Writer::write: no reader waiting and space in buffer -> storing message");
    b.push_front(capnp::clone(v));
    c.impl->totalNoOfIpsReceived++;
    return c.impl->sendImmediateStats(); //kj::READY_NOW;
  }

  // block until the buffer has space
  KJ_LOG(INFO, "Writer::write: no reader waiting and no space in buffer -> block, waiting for reader");
  auto paf = kj::newPromiseAndFulfiller<void>();
  auto* fulfillerPtr = paf.fulfiller.get();
  c.impl->blockingWriteFulfillers.push_front(kj::mv(paf.fulfiller));

  // This guard runs its lambda when it is destroyed (i.e. when the promise is canceled).
  auto cancelGuard = kj::defer([this, fulfillerPtr]() {
    auto& q = _channel.impl->blockingWriteFulfillers;
    for (auto it = q.begin(); it != q.end(); ++it) {
      if (it->get() == fulfillerPtr) {
        q.erase(it);
        KJ_LOG(INFO, "Writer::write: canceled, fulfiller removed from queue");
        break;
      }
    }
  });

  return paf.promise
            .then([context, this]() mutable {
              KJ_REQUIRE(!_closed, "promise_lambda: Writer already closed.", _closed);
              auto v = context.getParams();
              _channel.impl->buffer.push_front(capnp::clone(v));
              _channel.impl->totalNoOfIpsReceived++;
              KJ_LOG(INFO, "Writer::write: promise_lambda: wrote value to buffer");
            }).then([this]() { return _channel.impl->sendImmediateStats(); })
            .attach(kj::mv(cancelGuard));
}

kj::Promise<void> Writer::writeIfSpace(WriteIfSpaceContext context) {
  KJ_REQUIRE(!_closed, "Writer already closed.", _closed);

  auto v = context.getParams();
  auto& c = _channel;
  auto& b = c.impl->buffer;

  // don't accept any further writes if the channel is supposed to be closed (now or when the buffer is empty)
  if (c.impl->channelCanBeClosed || c.impl->channelShouldBeClosedOnEmptyBuffer) {
    KJ_LOG(INFO, "Writer::writeIfSpace:", c.impl->channelCanBeClosed, c.impl->channelShouldBeClosedOnEmptyBuffer);
    return c.impl->sendImmediateStats(); //kj::READY_NOW;
  }

  // if we received a done, this writer can be removed
  if (v.isDone()) {
    KJ_LOG(INFO, "Writer::writeIfSpace: received done -> remove writer");
    // cout << "Writer::write: received done message id: " << id().cStr() << endl;
    c.closedWriter(id());
    context.getResults().setSuccess(true);
    return c.impl->sendImmediateStats(); //kj::READY_NOW;
  }

  // there's a reader waiting
  if (!c.impl->blockingReadFulfillers.empty()) {
    KJ_LOG(INFO, "Writer::writeIfSpace: unblock waiting reader");
    auto&& brf = c.impl->blockingReadFulfillers.back();
    brf->fulfill(v);
    c.impl->totalNoOfIpsReceived++;
    c.impl->blockingReadFulfillers.pop_back();
    context.getResults().setSuccess(true);
    return c.impl->sendImmediateStats(); //kj::READY_NOW;
  }

  // there space to store the message
  if (b.size() < c.impl->bufferSize) {
    KJ_LOG(INFO, "Writer::writeIfSpace: no reader waiting and space in buffer -> storing message");
    b.push_front(capnp::clone(v));
    c.impl->totalNoOfIpsReceived++;
    context.getResults().setSuccess(true);
    return c.impl->sendImmediateStats(); //kj::READY_NOW;
  }

  KJ_LOG(INFO, "Writer::writeIfSpace: no reader waiting and no space in buffer -> return success=false");
  context.getResults().setSuccess(false);
  return kj::READY_NOW;
}

kj::Promise<void> Writer::close(CloseContext context) {
  KJ_LOG(INFO, "Writer::close: received close message id: ", id());
  _channel.closedWriter(id());
  return kj::READY_NOW;
}

