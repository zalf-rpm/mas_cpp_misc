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

#include "ports.h"

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
#include <kj/encoding.h>

using namespace std;
using namespace mas::infrastructure::common;

struct Ports::Impl {
  Ports &self;
  kj::HashMap<int, Channel::ChanReader::Client> inPortCaps;
  kj::HashMap<kj::String, int> inPortName2Id;
  kj::HashMap<int, kj::String> inPortSRs;
  kj::HashMap<int, bool> inPortsConnected;
  kj::HashMap<int, Channel::ChanWriter::Client> outPortCaps;
  kj::HashMap<kj::String, int> outPortName2Id;
  kj::HashMap<int, kj::String> outPortSRs;
  kj::HashMap<int, bool> outPortsConnected;
  ConnectionManager &conMan;

  Impl(Ports &self, ConnectionManager& conMan,
    std::initializer_list<std::tuple<int, kj::StringPtr, kj::StringPtr>> inPorts,
    std::initializer_list<std::tuple<int, kj::StringPtr, kj::StringPtr>> outPorts)
    : self(self) , conMan(conMan) {
    for (auto &inPort : inPorts) {
      int portId = get<0>(inPort);
      this->inPortCaps.insert(portId, nullptr);
      this->inPortName2Id.insert(kj::str(get<1>(inPort)), portId);
      this->inPortSRs.insert(portId, kj::str(get<2>(inPort)));
      this->inPortsConnected.insert(portId, false);
    }
    for (auto &outPort : outPorts) {
      int portId = get<0>(outPort);
      this->outPortCaps.insert(portId, nullptr);
      this->outPortName2Id.insert(kj::str(get<1>(outPort)), portId);
      this->outPortSRs.insert(portId, kj::str(get<2>(outPort)));
      this->outPortsConnected.insert(portId, false);
    }
  }

  ~Impl() = default;

  void connect() {
    for (auto &e : inPortSRs) {
      kj::StringPtr sr = e.value;
      if (sr != nullptr && sr.size() > 0) {
        auto reader = conMan.tryConnectB(sr).castAs<Channel::ChanReader>();
        inPortCaps.upsert(e.key, kj::mv(reader));
        inPortsConnected.upsert(e.key, true);
      }
    }
    for (auto &e : outPortSRs) {
      kj::StringPtr sr = get<0>(e.value);
      if (sr != nullptr && sr.size() > 0) {
        auto writer = conMan.tryConnectB(sr).castAs<Channel::ChanWriter>();
        outPortCaps.upsert(e.key, kj::mv(writer));
        outPortsConnected.upsert(e.key, true);
      }
    }
  }

};

Ports::Ports(ConnectionManager& conMan,
             std::initializer_list<std::tuple<int, kj::StringPtr, kj::StringPtr>> inPorts,
             std::initializer_list<std::tuple<int, kj::StringPtr, kj::StringPtr>> outPorts,
             bool interactive)
: impl(kj::heap<Impl>(*this, conMan,
  inPorts, outPorts)) {}

void Ports::connect() {
  impl->connect();
}

Ports::Channel::ChanReader::Client Ports::in(int inPortId) {
  Channel::ChanReader::Client def(nullptr);
  return impl->inPortCaps.find(inPortId).orDefault(def);
}

bool Ports::inIsConnected(int inPortId) const {
  return impl->inPortsConnected.find(inPortId).orDefault(false);
}

void Ports::inSetDisconnected(int inPortId) {
  impl->inPortsConnected.upsert(inPortId, false);
}

Ports::Channel::ChanWriter::Client Ports::out(int outPortId) {
  Channel::ChanWriter::Client def(nullptr);
  return impl->outPortCaps.find(outPortId).orDefault(def);
}

bool Ports::outIsConnected(int outPortId) const {
  return impl->outPortsConnected.find(outPortId).orDefault(false);
}

kj::Promise<void> Ports::newInPort(NewInPortContext context) {
  auto params = context.getParams();
  if (params.hasName()) {
    KJ_IF_MAYBE(portId, impl->inPortName2Id.find(params.getName())) {
      if (params.hasReaderCap()) {
        impl->inPortCaps.upsert(*portId, params.getReaderCap());
        impl->inPortsConnected.upsert(*portId, true);
      }
      else impl->inPortsConnected.upsert(*portId, false);
      if (params.hasReaderSR()) impl->inPortSRs.upsert(*portId, kj::str(params.getReaderSR()));
    }
  }
  return kj::READY_NOW;
}

kj::Promise<void> Ports::newOutPort(NewOutPortContext context) {
  auto params = context.getParams();
  if (params.hasName()) {
    KJ_IF_MAYBE(portId, impl->outPortName2Id.find(params.getName())) {
      if (params.hasWriterCap()) {
        impl->outPortCaps.upsert(*portId, params.getWriterCap());
        impl->outPortsConnected.upsert(*portId, true);
      } else impl->outPortsConnected.upsert(*portId, false);
      if (params.hasWriterSR()) impl->outPortSRs.upsert(*portId, kj::str(params.getWriterSR()));
    }
  }
  return kj::READY_NOW;
}