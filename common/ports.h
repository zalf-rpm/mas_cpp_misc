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

#include "common.h"
#include "restorer.h"
#include "common.capnp.h"
#include "fbp.capnp.h"
#include "rpc-connection-manager.h"
#include "channel.h"

namespace mas::infrastructure::common {

class Ports final : public mas::schema::fbp::PortCallbackRegistrar::PortCallback::Server {
public:
  Ports(ConnectionManager &conMan,
    std::initializer_list<std::tuple<int, kj::StringPtr, kj::StringPtr>> inPorts,
    std::initializer_list<std::tuple<int, kj::StringPtr, kj::StringPtr>> outPorts, bool interactive = false);

  ~Ports() = default;

  void connect();

  typedef mas::schema::fbp::IP IP;
  typedef mas::schema::fbp::Channel<IP> Channel;
  Channel::ChanReader::Client in(int inPortId);
  bool inIsConnected(int inPortId) const;
  void inSetDisconnected(int inPortId);

  Channel::ChanWriter::Client out(int outPortId);
  bool outIsConnected(int outPortId) const;

  kj::Promise<void> newInPort(NewInPortContext context) override;

  kj::Promise<void> newOutPort(NewOutPortContext context) override;

  struct Impl;
private:
  kj::Own<Impl> impl;
};

KJ_DECLARE_NON_POLYMORPHIC(Ports::Impl)

} // namespace mas::infrastructure::common

