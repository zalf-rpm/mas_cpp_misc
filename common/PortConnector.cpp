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
  kj::HashMap<int, kj::String> inPortSRs;
  kj::HashMap<int, bool> inPortsConnected;
  kj::HashMap<int, Channel::ChanWriter::Client> outPortCaps;
  kj::HashMap<kj::String, int> outPortName2Id;
  kj::HashMap<int, kj::String> outPortSRs;
  kj::HashMap<int, bool> outPortsConnected;
  ConnectionManager &conMan;
  //mas::schema::fbp::PortCallbackRegistrar::Client portCallbackRegistrar{nullptr};
  //mas::schema::fbp::PortCallbackRegistrar::PortCallback::Client portCallback{nullptr};
  //kj::HashMap<int, std::vector<int>> andOrPortIds;
  kj::Own<kj::PromiseFulfiller<void>> necessaryPortsConnected;
  toml::table tomlConfig;

  Impl(PortConnector &self, ConnectionManager& conMan,
    std::initializer_list<std::tuple<int, kj::StringPtr, kj::StringPtr>> inPorts,
    std::initializer_list<std::tuple<int, kj::StringPtr, kj::StringPtr>> outPorts)
    : self(self), conMan(conMan) { //, portCallback(kj::heap<PortCallback>(*this)) {
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
      this->outPortName2Id.insert(kj::str(portName), portId);
      //this->outPortSRs.insert(portId, kj::str(get<2>(outPort)));
      this->outPortsConnected.insert(portId, false);
    }
  }

  ~Impl() = default;

  void connect() {
    for (auto &e : inPortSRs) {
      connectToSR(e.key, e.value, true);
    }
    for (auto &e : outPortSRs) {
      connectToSR(e.key, e.value, false);
    }
  }

  void connectFromConfig(kj::StringPtr configReaderSR) {
    typedef mas::schema::fbp::Channel<mas::schema::fbp::IIP>::ChanReader IIPChanReader;
    auto reader = conMan.tryConnectB(configReaderSR).castAs<IIPChanReader>();
    auto msg = reader.readRequest().send().wait(conMan.ioContext().waitScope);
    if (msg.isDone()) {
      //return kj::mv(newPortIds);
    } else if (msg.hasValue() && msg.getValue().hasContent()) {
      auto tomlST = msg.getValue().getContent().getAs<mas::schema::common::StructuredText>();
      if (tomlST.hasValue() && tomlST.getStructure().isToml()) {
        try {
          auto tomlConfigTxt = tomlST.getValue().cStr();
          KJ_LOG(INFO, "TOML configuration:\n", tomlConfigTxt);
          tomlConfig = toml::parse(tomlConfigTxt);
          const auto portsSection = tomlConfig["ports"];
          const auto inPortsSection = portsSection["in"];
          for (auto [portName, portTable] : *inPortsSection.as_table()) {
            auto key = portName;
            KJ_IF_MAYBE(portId, inPortName2Id.find(kj::StringPtr(key.data()))) {
              auto port = *portTable.as_table();
              auto sr = *port["sr"].as_string();
              if (!sr->empty()) {
                connectToSR(*portId, sr.get(), true);
              }
            }
          }
          const auto outPortsSection = portsSection["out"];
          for (auto [portName, portTable] : *outPortsSection.as_table()) {
            auto key = portName;
            KJ_IF_MAYBE(portId, outPortName2Id.find(kj::StringPtr(key.data()))) {
              auto port = *portTable.as_table();
              auto sr = *port["sr"].as_string();
              if (!sr->empty()) {
                connectToSR(*portId, sr.get(), false);
              }
            }
          }
        } catch (const toml::parse_error& err) {
          KJ_LOG(INFO, "Parsing TOML configuration failed. Error:\n", err.what(), "\nTOML:\n", tomlST.getValue().cStr());
        }
      }
    }
  }

  void connectToSR(int portId, kj::StringPtr sr, bool isInPort) {
    if (isInPort) {
      if (sr != nullptr && sr.size() > 0) {
        auto reader = conMan.tryConnectB(sr).castAs<Channel::ChanReader>();
        inPortCaps.upsert(portId, kj::mv(reader));
        inPortsConnected.upsert(portId, true);
      }
    } else {
      if (sr != nullptr && sr.size() > 0) {
        auto writer = conMan.tryConnectB(sr).castAs<Channel::ChanWriter>();
        outPortCaps.upsert(portId, kj::mv(writer));
        outPortsConnected.upsert(portId, true);
      }
    }
  }

  kj::Vector<int> connectInteractively(kj::StringPtr newPortInfoReaderSr,
    std::initializer_list<std::initializer_list<int>> andOrPortIdsInit) {
    kj::HashMap<int, std::vector<int>> andOrPortIds;
    int i = 0;
    for (auto l : andOrPortIdsInit) {
      andOrPortIds.insert(i++, l);
    }

    kj::Vector<int> newPortIds;

    auto portInfoReader = conMan.tryConnectB(newPortInfoReaderSr).castAs<mas::schema::fbp::Channel<mas::schema::fbp::NewPortInfo>::ChanReader>();
    while (true) {
      auto msg = portInfoReader.readRequest().send().wait(conMan.ioContext().waitScope);
      if (msg.isDone()) {
        return kj::mv(newPortIds);
      } else if (msg.hasValue()) {
        int newPortId = -1;
        auto newPortInfo = msg.getValue();
        if (newPortInfo.hasName()) {
          auto name = newPortInfo.getName();
          if (newPortInfo.hasInPortReaderCap() || newPortInfo.hasInPortReaderSR()){
            KJ_IF_MAYBE(portId, inPortName2Id.find(name)) {
              if (newPortInfo.hasInPortReaderCap()) {
                inPortCaps.upsert(*portId, newPortInfo.getInPortReaderCap());
                inPortsConnected.upsert(*portId, true);
                newPortId = *portId;
              } else if (newPortInfo.hasInPortReaderSR()) {
                auto sr = newPortInfo.getInPortReaderSR();
                inPortSRs.upsert(*portId, kj::str(sr));
                connectToSR(*portId, sr, true);
                newPortId = *portId;
              } else {
                inPortsConnected.upsert(*portId, false);
              }
            }
          } else if (newPortInfo.hasOutPortWriterCap() || newPortInfo.hasOutPortWriterSR()){
            KJ_IF_MAYBE(portId, outPortName2Id.find(name)) {
              if (newPortInfo.hasOutPortWriterCap()) {
                outPortCaps.upsert(*portId, newPortInfo.getOutPortWriterCap());
                outPortsConnected.upsert(*portId, true);
                newPortId = *portId;
              } else if (newPortInfo.hasOutPortWriterSR()) {
                auto sr = newPortInfo.getOutPortWriterSR();
                outPortSRs.upsert(*portId, kj::str(sr));
                connectToSR(*portId, sr, false);
                newPortId = *portId;
              } else {
                outPortsConnected.upsert(*portId, false);
              }
            }
          }
        }

        int removeIndex = -1;
        for (auto& e : andOrPortIds) {
          for (auto portId : e.value) {
            if (portId == newPortId) {
              removeIndex = e.key;
              newPortIds.add(newPortId);
              goto leave;
            }
          }
        }
        leave:
          if (removeIndex >= 0) {
            andOrPortIds.erase(removeIndex);
          }

        if (andOrPortIds.size() == 0) break;
      }
    }
    return kj::mv(newPortIds);
  }
};

// PortConnector::PortConnector(ConnectionManager& conMan,
//              std::initializer_list<std::tuple<int, kj::StringPtr, kj::StringPtr>> inPorts,
//              std::initializer_list<std::tuple<int, kj::StringPtr, kj::StringPtr>> outPorts)
// : impl(kj::heap<Impl>(*this, conMan,
//   inPorts, outPorts)) {}

PortConnector::PortConnector(ConnectionManager &conMan, std::map<int, kj::StringPtr> inPorts,
  std::map<int, kj::StringPtr> outPorts)
  : impl(kj::heap<Impl>(*this, conMan, inPorts, outPorts)) {}

void PortConnector::connectFromConfig(kj::StringPtr configReaderSR) {
  impl->connectFromConfig(configReaderSR);
}

// void PortConnector::connect() {
//   impl->connect();
// }

kj::Vector<int> PortConnector::connectInteractively(kj::StringPtr newPortInfoReaderSr,
    std::initializer_list<std::initializer_list<int>> andOrPortIds) {
  return impl->connectInteractively(newPortInfoReaderSr, andOrPortIds);
}

PortConnector::Channel::ChanReader::Client PortConnector::in(int inPortId) {
  Channel::ChanReader::Client def(nullptr);
  return impl->inPortCaps.find(inPortId).orDefault(def);
}

bool PortConnector::inIsConnected(int inPortId) const {
  return impl->inPortsConnected.find(inPortId).orDefault(false);
}

void PortConnector::inSetDisconnected(int inPortId) {
  impl->inPortsConnected.upsert(inPortId, false);
}

PortConnector::Channel::ChanWriter::Client PortConnector::out(int outPortId) {
  Channel::ChanWriter::Client def(nullptr);
  return impl->outPortCaps.find(outPortId).orDefault(def);
}

bool PortConnector::outIsConnected(int outPortId) const {
  return impl->outPortsConnected.find(outPortId).orDefault(false);
}
