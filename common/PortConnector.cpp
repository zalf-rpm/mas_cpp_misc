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

#include "PortConnector.h"

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

#include "toml/toml.hpp"

using namespace std;
using namespace mas::infrastructure::common;

struct PortConnector::Impl {
  PortConnector &self;
  kj::HashMap<int, Channel::ChanReader::Client> inPortCaps;
  kj::HashMap<kj::String, int> inPortName2Id;
  //kj::HashMap<int, kj::String> inPortSRs;
  kj::HashMap<int, bool> inPortsConnected;
  kj::HashMap<int, Channel::ChanWriter::Client> outPortCaps;
  kj::HashMap<int, kj::Vector<Channel::ChanWriter::Client>> outArrayPortCaps;
  kj::HashMap<int, kj::String> outPortId2Name;
  kj::HashMap<kj::String, int> outPortName2Id;
  //kj::HashMap<int, kj::String> outPortSRs;
  kj::HashMap<int, bool> outPortsConnected;
  kj::HashMap<int, kj::Vector<bool>> outArrayPortsConnected;
  ConnectionManager &conMan;
  kj::Own<kj::PromiseFulfiller<void>> necessaryPortsConnected;
  toml::table tomlConfig;

  Impl(PortConnector &self, ConnectionManager& conMan,
    std::map<int, kj::StringPtr> inPorts,
    std::map<int, kj::StringPtr> outPorts)
    : self(self), conMan(conMan) {
    for (auto [portId, portName] : inPorts) {
      this->inPortCaps.insert(portId, nullptr);
      this->inPortName2Id.insert(kj::str(portName), portId);
      //this->inPortSRs.insert(portId, kj::str(get<2>(inPort)));
      this->inPortsConnected.insert(portId, false);
    }
    for (auto [portId, portName] : outPorts) {
      this->outPortCaps.insert(portId, nullptr);
      this->outPortId2Name.insert(portId, kj::str(portName));
      this->outPortName2Id.insert(kj::str(portName), portId);
      //this->outPortSRs.insert(portId, kj::str(get<2>(outPort)));
      this->outPortsConnected.insert(portId, false);
    }
  }

  ~Impl() = default;

  // void connect() {
  //   for (auto &e : inPortSRs) {
  //     connectToSR(e.key, e.value, IN);
  //   }
  //   for (auto &e : outPortSRs) {
  //     connectToSR(e.key, e.value, OUT);
  //   }
  // }

  void connectFromPortInfos(kj::StringPtr portInfosReaderSR) {
    typedef mas::schema::fbp::Channel<mas::schema::fbp::PortInfos>::ChanReader PIReader;
    auto reader = conMan.tryConnectB(portInfosReaderSR).castAs<PIReader>();
    auto msg = reader.readRequest().send().wait(conMan.ioContext().waitScope);
    if (msg.isDone()) {
      return;
    } else if (msg.hasValue()) {
      if (msg.getValue().hasInPorts()) {
        for (auto nameAndSr : msg.getValue().getInPorts()) {
          if (nameAndSr.hasName() && nameAndSr.hasSr()) {
            auto key = nameAndSr.getName();
            KJ_IF_MAYBE(portId, inPortName2Id.find(key)) {
              connectToSR(*portId, nameAndSr.getSr(), IN);
            }
          }
        }
      }
      if (msg.getValue().hasOutPorts()){
        for (auto nameAndSr : msg.getValue().getOutPorts()) {
          KJ_IF_MAYBE(portId, outPortName2Id.find(nameAndSr.getName())) {
            if (nameAndSr.hasName()){
              if (nameAndSr.hasSrs()) {
                for (auto sr : nameAndSr.getSrs()) {
                  connectToSR(*portId, sr, ARRAY_OUT);
                }
              } else if (nameAndSr.hasSr()) {
                connectToSR(*portId, nameAndSr.getSr(), OUT);
              }
            }
          }
        }
      }
    }
  }

  enum PortType { IN, OUT, ARRAY_OUT };
  void connectToSR(int portId, schema::persistence::SturdyRef::Reader sr, PortType portType) {
    switch (portType) {
    case IN: {
      auto reader = conMan.tryConnectB(sr).castAs<Channel::ChanReader>();
      inPortCaps.upsert(portId, kj::mv(reader));
      inPortsConnected.upsert(portId, true);
      break;
    }
    case OUT:{
      auto writer = conMan.tryConnectB(sr).castAs<Channel::ChanWriter>();
      outPortCaps.upsert(portId, kj::mv(writer));
      outPortsConnected.upsert(portId, true);
      break;
    }
    case ARRAY_OUT:{
      auto writer = conMan.tryConnectB(sr).castAs<Channel::ChanWriter>();
      KJ_IF_MAYBE(cap_vec, outArrayPortCaps.find(portId)) {
        cap_vec->add(kj::mv(writer));
      } else {
        kj::Vector<Channel::ChanWriter::Client> vec;
        vec.add(kj::mv(writer));
        outArrayPortCaps.insert(portId, kj::mv(vec));
        kj::Vector<bool> vec2;
        vec2.add(true);
        outArrayPortsConnected.upsert(portId, kj::mv(vec2));
      }
      break;
    }
    }
  }
};

PortConnector::PortConnector(ConnectionManager &conMan, std::map<int, kj::StringPtr> inPorts,
  std::map<int, kj::StringPtr> outPorts)
  : impl(kj::heap<Impl>(*this, conMan, inPorts, outPorts)) {}

void PortConnector::connectFromPortInfos(kj::StringPtr portInfosReaderSR) {
  impl->connectFromPortInfos(portInfosReaderSR);
}

PortConnector::Channel::ChanReader::Client PortConnector::in(int inPortId) {
  Channel::ChanReader::Client def(nullptr);
  return impl->inPortCaps.find(inPortId).orDefault(def);
}

bool PortConnector::isInConnected(int inPortId) const {
  return impl->inPortsConnected.find(inPortId).orDefault(false);
}

void PortConnector::setInDisconnected(int inPortId) {
  impl->inPortsConnected.upsert(inPortId, false);
}

PortConnector::Channel::ChanWriter::Client PortConnector::out(int outPortId) {
  Channel::ChanWriter::Client def(nullptr);
  return impl->outPortCaps.find(outPortId).orDefault(def);
}

PortConnector::Channel::ChanWriter::Client PortConnector::arrOut(int outPortId, int portIndex) {
  Channel::ChanWriter::Client def(nullptr);
  KJ_IF_MAYBE(vec, impl->outArrayPortCaps.find(outPortId)) {
    if (portIndex < vec->size()) {
      return (*vec)[portIndex];
    }
  }
  return def;
}

bool PortConnector::isOutConnected(int outPortId) const {
  return impl->outPortsConnected.find(outPortId).orDefault(false);
}

bool PortConnector::isArrOutConnected(int outPortId, int portIndex) const {
  KJ_IF_MAYBE(vec, impl->outArrayPortsConnected.find(outPortId)) {
    if (portIndex < vec->size()) {
      return (*vec)[portIndex];
    }
  }
  return false;
}

void PortConnector::closeOutPorts() {
  for (auto [portId, writer] : impl->outPortCaps) {
    if (isOutConnected(portId)) {
      auto name = impl->outPortId2Name.find(portId);
      KJ_LOG(INFO, kj::str("closing ", name.orDefault(kj::str(portId)), " OUT port"));
      writer.closeRequest().send().wait(impl->conMan.ioContext().waitScope);
    }
  }
  for (auto& [portId, vecOfWriters] : impl->outArrayPortCaps) {
    int i = 0;
    auto name = impl->outPortId2Name.find(portId);
    for (auto &writer : vecOfWriters) {
      if (isArrOutConnected(portId, i)) {
        KJ_LOG(INFO, kj::str("closing ", name.orDefault(kj::str(portId)), " OUT port"));
        writer.closeRequest().send().wait(impl->conMan.ioContext().waitScope);
      }
      i++;
    }
  }
}