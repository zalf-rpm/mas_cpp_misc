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

#include <cstdint>

#include <kj/async.h>
#include <kj/common.h>
#include <kj/debug.h>
#include <kj/function.h>
#include <kj/main.h>
#include <kj/memory.h>
#include <kj/string.h>
#include <kj/vector.h>

#include "gateway.h"
#include "persistence.capnp.h"
#include "restorable-service-main.h"

#include "common.capnp.h"

namespace mas::infrastructure::common {

class GatewayMain : public RestorableServiceMain {
public:
  explicit GatewayMain(kj::ProcessContext &context)
      : RestorableServiceMain(context, "Gateway v0.1",
                              "Offers a Gateway for internal services to be accessed from outside world.") {}

  kj::MainBuilder::Validity setSecsKeepAliveTimeout(kj::StringPtr name) {
    secsKeepAliveTimeout = kj::max(0, name.parseAs<uint32_t>());
    return true;
  }

  kj::MainBuilder::Validity startService() {
    KJ_LOG(INFO, "starting host-port-resolver service");

    auto ownedGateway = kj::heap<Gateway>(ioContext.provider->getTimer(), name, description, secsKeepAliveTimeout);
    auto gateway = ownedGateway.get();
    mas::schema::persistence::Gateway::Client gatewayClient = kj::mv(ownedGateway);
    KJ_LOG(INFO, "created gateway");

    startRestorerSetup(gatewayClient, true);
    gateway->setRestorer(restorer, restorerClient);

    // Run forever, using regularly cleaning mappings, accepting connections and handling requests.
    auto gcmProm = gateway->garbageCollectMappings();
    gcmProm
        .then([]() { return kj::NEVER_DONE; },
              [](auto &&ex) {
                KJ_LOG(INFO, ex);
                return kj::NEVER_DONE;
              })
        .wait(ioContext.waitScope);
    KJ_LOG(INFO, "stopped gateway service");
    return true;
  }

  kj::MainFunc getMain() {
    return addRestorableServiceOptions()
        .callAfterParsing(KJ_BIND_METHOD(*this, startService))
        .addOptionWithArg({'t', "secs_keep_alive_timeout"}, KJ_BIND_METHOD(*this, setSecsKeepAliveTimeout),
                          "<secs_keep_alive_timeout (default: 600s (= 10min))>",
                          "Set timeout in seconds before an service mapping will be removed.")
        .build();
  }

  uint32_t secsKeepAliveTimeout{600};
};

} // namespace mas::infrastructure::common

KJ_MAIN(mas::infrastructure::common::GatewayMain)
