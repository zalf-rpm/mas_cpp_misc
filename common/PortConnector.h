/* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/*
Authors:
Michael Berg <michael.berg-mohnicke@zalf.de>

Maintainers:
Currently maintained by the authors.

This file is part of the ZALF model and simulation infrastructure.
Copyright (C) Leibniz Centre for Agricultural Landscape Research (ZALF)
*/

#pragma once

#include <functional>
#include <string>
#include <deque>

#include <kj/debug.h>
#include <kj/common.h>
#include <kj/string.h>
#include <kj/vector.h>
#include <kj/map.h>
#include <kj/memory.h>
#include <kj/thread.h>
#include <kj/async.h>

#include <capnp/any.h>
#include <capnp/rpc-twoparty.h>

#include "toml/toml.hpp"
#include "common.capnp.h"
#include "fbp.capnp.h"
#include "rpc-connection-manager.h"

namespace mas::infrastructure::common {

class PortConnector {
public:
  PortConnector(ConnectionManager &conMan, std::map<int, kj::StringPtr> inPorts, std::map<int, kj::StringPtr> outPorts);

  ~PortConnector() = default;

  void connectFromPortInfos(kj::StringPtr portInfosReaderSR);

  typedef mas::schema::fbp::IP IP;
  typedef mas::schema::fbp::Channel<IP> Channel;
  Channel::ChanReader::Client in(int inPortId);
  bool isInConnected(int inPortId) const;
  void setInDisconnected(int inPortId);

  Channel::ChanWriter::Client out(int outPortId);
  Channel::ChanWriter::Client arrOut(int outPortId, int portIndex);
  bool isOutConnected(int outPortId) const;
  bool isArrOutConnected(int outPortId, int portIndex) const;
  void closeOutPorts();

  struct Impl;
private:
  kj::Own<Impl> impl;
};

KJ_DECLARE_NON_POLYMORPHIC(PortConnector::Impl)

} // namespace mas::infrastructure::common

