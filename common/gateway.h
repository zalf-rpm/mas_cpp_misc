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

#include <kj/array.h>
#include <kj/async.h>
#include <kj/function.h>
#include <kj/string.h>
#include <kj/timer.h>

#include "restorer.h"
#include "common.capnp.h"
#include "persistence.capnp.h"

namespace mas::infrastructure::common {

class Gateway final : public mas::schema::persistence::Gateway::Server
{
public:
  explicit Gateway(kj::Timer& timer, kj::StringPtr name, kj::StringPtr description, uint32_t secsKeepAliveTimeout);
  ~Gateway();

  kj::Promise<void> info(InfoContext context) override;

  // restore @0 (srToken :Text) -> (cap :Capability);
  kj::Promise<void> restore(RestoreContext context) override;

  // register @0 (cap :Capability) -> RegResults;
  // struct RegResults {
  //   sturdyRef               @0 :SturdyRef;
  //   heartbeat               @1 :Heartbeat;
  //   secsHeartbeatInterval   @2 :UInt32;
  // }
  kj::Promise<void> register_(RegisterContext context) override;

  void setRestorer(Restorer* r, mas::schema::persistence::Restorer::Client client);

  kj::Promise<void> garbageCollectMappings(bool runOnce = false);

private:
  struct Impl;
  kj::Own<Impl> impl;
};

} // namespace mas::infrastructure::common
