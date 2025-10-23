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

#include <tuple>

#include <kj/debug.h>
#include <kj/thread.h>
#include <kj/common.h>
#include <kj/async.h>
#include <kj/exception.h>

#include <capnp/capability.h>
#include <capnp/message.h>
#include <capnp/schema.h>
#include <capnp/dynamic.h>
#include <iostream>

#include "sole.hpp"

using namespace std;
using namespace mas::infrastructure::common;

struct Channel::Impl {
  Channel &self;
  mas::infrastructure::common::Restorer *restorer{nullptr};
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
  //mas::schema::common::Action::Client unregisterAction{nullptr};
  bool channelShouldBeClosedOnEmptyBuffer = false;
  bool channelCanBeClosed = false;

  Impl(Channel &self, mas::infrastructure::common::Restorer *restorer, kj::StringPtr name, kj::StringPtr description,
       uint64_t bufferSize)
      : self(self), id(kj::str(sole::uuid4().str())), name(kj::str(name)), description(kj::str(description)),
        bufferSize(std::max(static_cast<uint64_t>(1), bufferSize)) {
    setRestorer(restorer);
  }

  ~Impl() = default;

  void setRestorer(mas::infrastructure::common::Restorer *restorer) {
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

Channel::Channel(kj::StringPtr name, kj::StringPtr description, uint64_t bufferSize, Restorer *restorer)
    : impl(kj::heap<Impl>(*this, restorer, name, description, bufferSize)) {
}

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
    return impl->restorer->save(impl->client, context.getResults().initSturdyRef(),
                                context.getResults().initUnsaveSR()).ignoreResult();
  }
  return kj::READY_NOW;
}


void Channel::closedReader(kj::StringPtr readerId) {
  impl->readers.erase(readerId);
  // now that all readers disconnected, turn of auto-closing readers
  if (kj::size(impl->readers) == 0) impl->sendCloseOnEmptyBuffer = false;
  KJ_LOG(INFO, "Channel::closedReader: number of readers left:", kj::size(impl->readers));
  //cout << "Channel::closedReader: number of readers left:" << kj::size(impl->readers) << endl;
}

void Channel::closedWriter(kj::StringPtr writerId) {
  impl->writers.erase(writerId);
  KJ_LOG(INFO, "Channel::closedWriter: number of writers left:", kj::size(impl->writers), impl->autoCloseSemantics);
  //cout << "Channel::closedWriter: number of writers left:" << kj::size(impl->writers) << " FBP Close semantics: " << (impl->autoCloseSemantics == AnyPointerChannel::CloseSemantics::FBP ? "true" : "false")  << endl;
  //cout << "Channel::closedWriter:number of readers:" << kj::size(impl->readers) << " FBP Close semantics: " << (impl->autoCloseSemantics == AnyPointerChannel::CloseSemantics::FBP)  << endl;

  if (impl->autoCloseSemantics == AnyPointerChannel::CloseSemantics::FBP && kj::size(impl->writers) == 0) {
    impl->sendCloseOnEmptyBuffer = true;
    KJ_LOG(INFO, "Channel::closedWriter: FBP semantics and no writers left -> sending done to readers");
    //cout << "Channel::closedWriter: FBP semantics and no writers left -> sending done to readers" << endl;

    // as we just received a done message which should be distributed and would
    // fill the buffer, unblock all readers, so they send the done message
    while (kj::size(impl->blockingReadFulfillers) > 0) {
      auto &brf = impl->blockingReadFulfillers.back();
      brf->fulfill(nullptr);//kj::Maybe<AnyPointerMsg::Reader>());
      impl->blockingReadFulfillers.pop_back();
      KJ_LOG(INFO, "Channel::closedWriter: sent done to reader on last finished writer");
      //cout << "Channel::closedWriter: sent done to reader on last finished writer" << endl;
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

bool Channel::canBeClosed() const {
  return impl->channelCanBeClosed;
}

kj::Promise<void> Channel::close(CloseContext context) {
  KJ_LOG(INFO, "Channel::close: message received", context.getParams().getWaitForEmptyBuffer());
  if (!context.getParams().getWaitForEmptyBuffer() || impl->buffer.empty()) {
    impl->channelCanBeClosed = true;
  } else {
    impl->channelShouldBeClosedOnEmptyBuffer = true;
    impl->sendCloseOnEmptyBuffer = true;
  }
  return kj::READY_NOW;
}


AnyPointerChannel::Client Channel::getClient() { return impl->client; }

void Channel::setClient(AnyPointerChannel::Client c) { impl->client = c; }

//mas::schema::common::Action::Client Channel::getUnregisterAction() { return impl->unregisterAction; }
//void Channel::setUnregisterAction(mas::schema::common::Action::Client unreg) { impl->unregisterAction = unreg; }

void Channel::setRestorer(mas::infrastructure::common::Restorer *restorer) {
  impl->setRestorer(restorer);
}

Reader::Reader(Channel &c)
    : _channel(c), _id(kj::str(sole::uuid4().str())) {}

kj::Promise<void> Reader::info(InfoContext context) {
  KJ_LOG(INFO, "Reader::info: message received");
  auto rs = context.getResults();
  auto channelNameOrId = kj::str(_channel.impl->name.size() > 0 ? _channel.impl->name : _channel.impl->id);
  rs.setId(_id);
  rs.setName(kj::str(channelNameOrId, "::", _id));
  rs.setDescription(kj::str("Port (ID: ", _id, ") @ Channel '", _channel.impl->name,
    "' (ID: ", _channel.impl->id, ")"));
  return kj::READY_NOW;
}


kj::Promise<void> Reader::save(SaveContext context) {
  KJ_LOG(INFO, "Reader::save: message received");
  if (_channel.impl->restorer) {
    KJ_IF_MAYBE(client, _channel.impl->readers.find(_id)) {
      return _channel.impl->restorer->save(*client, context.getResults().initSturdyRef(),
                                context.getResults().initUnsaveSR()).ignoreResult();
    }
  }
  return kj::READY_NOW;
}

kj::Promise<void> Reader::read(ReadContext context) {
  KJ_REQUIRE(!_closed, "Reader already closed.", _closed);

  auto &c = _channel;
  auto &b = c.impl->buffer;

  // buffer not empty, send next value
  if (!b.empty()) {
    KJ_LOG(INFO, "Reader::read: buffer not empty, send next value");
    auto &&v = b.back();
    KJ_ASSERT(v.get()->isValue(), "Msg contains a value, because before buffering we checked for done.");
    context.getResults().setValue(v.get()->getValue());
    b.pop_back();

    // unblock a writer unless we're about to close down
    if (!c.impl->blockingWriteFulfillers.empty() && !c.impl->sendCloseOnEmptyBuffer) {
      KJ_LOG(INFO, "Reader::read: unblock next writer");
      auto &&bwf = c.impl->blockingWriteFulfillers.back();
      bwf->fulfill();
      c.impl->blockingWriteFulfillers.pop_back();
    }

    //check if channel is supposed to be closed and just waiting for an empty buffer
    if (b.empty() && c.impl->channelShouldBeClosedOnEmptyBuffer) c.impl->channelCanBeClosed = true;
  } else if (!c.impl->channelCanBeClosed) { // don't read if channel is supposed to close
    // buffer is empty, but we are supposed to close down
    if (c.impl->sendCloseOnEmptyBuffer) {
      KJ_LOG(INFO, "Reader::read: buffer is empty, but close down");
      context.getResults().setDone();
      //cout << "Reader::read: sending done to reader" << endl;
      c.closedReader(id());

      // if there are other readers waiting close them as well
      while (!c.impl->blockingReadFulfillers.empty()) {
        KJ_LOG(INFO, "Reader::read: close other waiting readers");
        auto &&brf = c.impl->blockingReadFulfillers.back();
        brf->fulfill(nullptr);
        c.impl->blockingReadFulfillers.pop_back();
      }
    } else { // block because no value to read
      KJ_LOG(INFO, "Reader::read: block, because no value to read");
      auto paf = kj::newPromiseAndFulfiller<kj::Maybe<AnyPointerMsg::Reader>>();
      c.impl->blockingReadFulfillers.push_front(kj::mv(paf.fulfiller));
      return paf.promise.then([context, this](kj::Maybe<AnyPointerMsg::Reader> msg) mutable {
        KJ_REQUIRE(!_closed, "Reader already closed.", _closed);

        if (_channel.impl->sendCloseOnEmptyBuffer && msg == nullptr) {
          //KJ_DBG("setResults");
          context.getResults().setDone();
          KJ_LOG(INFO, "Reader::read: promise_lambda: sending done to reader");
          //cout << "Reader::read: promise_lambda: sending done to reader" << endl;
          _channel.closedReader(id());
        } else {
          KJ_IF_MAYBE(m, msg) {
            //KJ_DBG("Reader::read setResults");
            context.getResults().setValue(m->getValue());
            KJ_LOG(INFO, "Reader::read: promise_lambda: sending value to reader");
          }
        }
      }, [](kj::Exception &&e) {
        KJ_LOG(ERROR, "Reader::read: promise_lambda: error: ", e.getDescription().cStr());
      });
    }
  }

  return kj::READY_NOW;
}

kj::Promise<void> Reader::readIfMsg(ReadIfMsgContext context) {
  KJ_REQUIRE(!_closed, "Reader already closed.", _closed);

  auto &c = _channel;
  auto &b = c.impl->buffer;

  // buffer not empty, send next value
  if (!b.empty()) {
    KJ_LOG(INFO, "Reader::readIfMsg: buffer not empty, send next value");
    auto &&v = b.back();
    KJ_ASSERT(v.get()->isValue(), "Msg contains a value, because before buffering we checked for done.");
    context.getResults().setValue(v.get()->getValue());
    b.pop_back();

    // unblock a writer unless we're about to close down
    if (!c.impl->blockingWriteFulfillers.empty() && !c.impl->sendCloseOnEmptyBuffer) {
      KJ_LOG(INFO, "Reader::readIfMsg: unblock next writer");
      auto &&bwf = c.impl->blockingWriteFulfillers.back();
      bwf->fulfill();
      c.impl->blockingWriteFulfillers.pop_back();
    }

    //check if channel is supposed to be closed and just waiting for an empty buffer
    if (b.empty() && c.impl->channelShouldBeClosedOnEmptyBuffer) c.impl->channelCanBeClosed = true;
  } else if (!c.impl->channelCanBeClosed) { //don't read if channel is supposed to close
    // buffer is empty, but we are supposed to close down
    if (c.impl->sendCloseOnEmptyBuffer) {
      KJ_LOG(INFO, "Reader::readIfMsg: buffer is empty, but close down");
      context.getResults().setDone();
      //cout << "Reader::read: sending done to reader" << endl;
      c.closedReader(id());

      // if there are other readers waiting close them as well
      while (!c.impl->blockingReadFulfillers.empty()) {
        KJ_LOG(INFO, "Reader::readIfMsg: close other waiting readers");
        auto &&brf = c.impl->blockingReadFulfillers.back();
        brf->fulfill(nullptr);
        c.impl->blockingReadFulfillers.pop_back();
      }
    } else { // block because no value to read
      KJ_LOG(INFO, "Reader::readIfMsg: return noMsg, because no value to read");
      context.getResults().setNoMsg();
    }
  }

  return kj::READY_NOW;
}

kj::Promise<void> Reader::close(CloseContext context) {
  KJ_LOG(INFO, "Reader::close: received close message id: ", id());
  _channel.closedReader(id());
  return kj::READY_NOW;
}

Writer::Writer(Channel &c)
    : _channel(c), _id(kj::str(sole::uuid4().str())) {}

kj::Promise<void> Writer::info(InfoContext context) {
  KJ_LOG(INFO, "Writer::info: message received");
  auto rs = context.getResults();
  auto channelNameOrId = kj::str(_channel.impl->name.size() > 0 ? _channel.impl->name : _channel.impl->id);
  rs.setId(_id);
  rs.setName(kj::str(channelNameOrId, "::", _id));
  rs.setDescription(kj::str("Port (ID: ", _id, ") @ Channel '", _channel.impl->name,
    "' (ID: ", _channel.impl->id, ")"));
  return kj::READY_NOW;
}


kj::Promise<void> Writer::save(SaveContext context) {
  KJ_LOG(INFO, "Writer::save: message received");
  if (_channel.impl->restorer) {
    KJ_IF_MAYBE(client, _channel.impl->writers.find(_id)) {
      return _channel.impl->restorer->save(*client, context.getResults().initSturdyRef(),
                                context.getResults().initUnsaveSR()).ignoreResult();
    }
  }
  return kj::READY_NOW;
}

kj::Promise<void> Writer::write(WriteContext context) {
  KJ_REQUIRE(!_closed, "Writer already closed.", _closed);

  auto v = context.getParams();
  auto &c = _channel;
  auto &b = c.impl->buffer;

  //don't accept any further writes if channel is supposed to be closed (now or when buffer is empty)
  if (c.impl->channelCanBeClosed || c.impl->channelShouldBeClosedOnEmptyBuffer) return kj::READY_NOW;

  // if we received a done, this writer can be removed
  if (v.isDone()) {
    KJ_LOG(INFO, "Writer::write: received done -> remove writer");
    //cout << "Writer::write: received done message id: " << id().cStr() << endl;
    c.closedWriter(id());
  } else if (!c.impl->blockingReadFulfillers.empty()) { // there's a reader waiting
    KJ_LOG(INFO, "Writer::write: unblock waiting reader");
    auto &&brf = c.impl->blockingReadFulfillers.back();
    brf->fulfill(v);
    c.impl->blockingReadFulfillers.pop_back();
  } else if (b.size() < c.impl->bufferSize) { // there space to store the message
    KJ_LOG(INFO, "Writer::write: no reader waiting and space in buffer -> storing message");
    b.push_front(capnp::clone(v));
  } else { // block until buffer has space
    KJ_LOG(INFO, "Writer::write: no reader waiting and no space in buffer -> block, waiting for reader");
    auto paf = kj::newPromiseAndFulfiller<void>();
    c.impl->blockingWriteFulfillers.push_front(kj::mv(paf.fulfiller));
    return paf.promise.then([context, this]() mutable {
      KJ_REQUIRE(!_closed, "promise_lambda: Writer already closed.", _closed);
      auto v = context.getParams();
      _channel.impl->buffer.push_front(capnp::clone(v));
      KJ_LOG(INFO, "Writer::write: promise_lambda: wrote value to buffer");
    }, [](kj::Exception &&e) {
      KJ_LOG(ERROR, "Writer::write: promise_lambda: error: ", e.getDescription().cStr());
    });
  }

  return kj::READY_NOW;
}

kj::Promise<void> Writer::writeIfSpace(WriteIfSpaceContext context) {
  KJ_REQUIRE(!_closed, "Writer already closed.", _closed);

  auto v = context.getParams();
  auto &c = _channel;
  auto &b = c.impl->buffer;

  //don't accept any further writes if channel is supposed to be closed (now or when buffer is empty)
  if (c.impl->channelCanBeClosed || c.impl->channelShouldBeClosedOnEmptyBuffer) return kj::READY_NOW;

  // if we received a done, this writer can be removed
  if (v.isDone()) {
    KJ_LOG(INFO, "Writer::writeIfSpace: received done -> remove writer");
    //cout << "Writer::write: received done message id: " << id().cStr() << endl;
    c.closedWriter(id());
    context.getResults().setSuccess(true);
  } else if (!c.impl->blockingReadFulfillers.empty()) { // there's a reader waiting
    KJ_LOG(INFO, "Writer::writeIfSpace: unblock waiting reader");
    auto &&brf = c.impl->blockingReadFulfillers.back();
    brf->fulfill(v);
    c.impl->blockingReadFulfillers.pop_back();
    context.getResults().setSuccess(true);
  } else if (b.size() < c.impl->bufferSize) { // there space to store the message
    KJ_LOG(INFO, "Writer::writeIfSpace: no reader waiting and space in buffer -> storing message");
    b.push_front(capnp::clone(v));
    context.getResults().setSuccess(true);
  } else { // block until buffer has space
    KJ_LOG(INFO, "Writer::writeIfSpace: no reader waiting and no space in buffer -> return success=false");
    context.getResults().setSuccess(false);
  }

  return kj::READY_NOW;
}

kj::Promise<void> Writer::close(CloseContext context) {
  KJ_LOG(INFO, "Writer::close: received close message id: ", id());
  _channel.closedWriter(id());
  return kj::READY_NOW;
}