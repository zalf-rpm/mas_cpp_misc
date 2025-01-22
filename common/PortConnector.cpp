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

using namespace std;
using namespace mas::infrastructure::common;

struct PortConnector::Impl {
//   class PortCallback final : public mas::schema::fbp::PortCallbackRegistrar::PortCallback::Server {
//   public:
//     explicit PortCallback(Impl& impl) : _impl(impl) {}
//
//     ~PortCallback() = default;
//
//     kj::Promise<void> newInPort(NewInPortContext context) override {
//       auto params = context.getParams();
//       if (params.hasName()) {
//         KJ_IF_MAYBE(portId, _impl.inPortName2Id.find(params.getName())) {
//           if (params.hasReaderCap()) {
//             _impl.inPortCaps.upsert(*portId, params.getReaderCap());
//             _impl.inPortsConnected.upsert(*portId, true);
//             _impl.checkNecessaryPortsConnected(*portId);
//           }
//           else {
//             _impl.inPortsConnected.upsert(*portId, false);
//           }
//           if (params.hasReaderSR()) _impl.inPortSRs.upsert(*portId, kj::str(params.getReaderSR()));
//         }
//       }
//       return kj::READY_NOW;
//     }
//
//     kj::Promise<void> newOutPort(NewOutPortContext context) override {
//       auto params = context.getParams();
//       if (params.hasName()) {
//         KJ_IF_MAYBE(portId, _impl.outPortName2Id.find(params.getName())) {
//           if (params.hasWriterCap()) {
//             _impl.outPortCaps.upsert(*portId, params.getWriterCap());
//             _impl.outPortsConnected.upsert(*portId, true);
//             _impl.checkNecessaryPortsConnected(*portId);
//           } else {
//             _impl.outPortsConnected.upsert(*portId, false);
//           }
//           if (params.hasWriterSR()) _impl.outPortSRs.upsert(*portId, kj::str(params.getWriterSR()));
//         }
//       }
//       return kj::READY_NOW;
//     }
//
//   private:
//     Impl& _impl;
//   };

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
        if (newPortInfo.isIn()) {
          auto in = newPortInfo.getIn();
          if (newPortInfo.hasName()) {
            KJ_IF_MAYBE(portId, inPortName2Id.find(newPortInfo.getName())) {
              if (in.hasReaderCap()) {
                inPortCaps.upsert(*portId, in.getReaderCap());
                inPortsConnected.upsert(*portId, true);
                newPortId = *portId;
              } else {
                inPortsConnected.upsert(*portId, false);
              }
              if (in.hasReaderSR()) inPortSRs.upsert(*portId, kj::str(in.getReaderSR()));
            }
          }
        } else if (newPortInfo.isOut()) {
          auto out = newPortInfo.getOut();
          if (newPortInfo.hasName()) {
            KJ_IF_MAYBE(portId, outPortName2Id.find(newPortInfo.getName())) {
              if (out.hasWriterCap()) {
                outPortCaps.upsert(*portId, out.getWriterCap());
                outPortsConnected.upsert(*portId, true);
                newPortId = *portId;
              } else {
                outPortsConnected.upsert(*portId, false);
              }
              if (out.hasWriterSR()) outPortSRs.upsert(*portId, kj::str(out.getWriterSR()));
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

PortConnector::PortConnector(ConnectionManager& conMan,
             std::initializer_list<std::tuple<int, kj::StringPtr, kj::StringPtr>> inPorts,
             std::initializer_list<std::tuple<int, kj::StringPtr, kj::StringPtr>> outPorts,
             bool interactive)
: impl(kj::heap<Impl>(*this, conMan,
  inPorts, outPorts)) {}

void PortConnector::connect() {
  impl->connect();
}

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
