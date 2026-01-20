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

#include "gateway.h"
#include "persistence.capnp.h"

#include <cstdint>
#include <kj/array.h>
#include <kj/map.h>
#include <kj/memory.h>
#include <kj/time.h>
#include <kj/timer.h>
#include <kj/vector.h>
#include <random>
#include <vector>

#include <kj/async.h>
#include <kj/common.h>
#include <kj/debug.h>
#include <kj/encoding.h>
#include <kj/string.h>
#include <kj/thread.h>
#include <kj/tuple.h>
#define KJ_MVCAP(var) var = kj::mv(var)

#include <capnp/capability.h>
#include <capnp/compat/json.h>
#include <capnp/message.h>

#include "restorer.h"

#include "sole.hpp"

using namespace mas::infrastructure::common;

namespace {

kj::String deterministicUuid(kj::StringPtr seed) {
  std::vector<uint32_t> seedData;
  seedData.reserve(seed.size());
  for (char ch : seed) {
    seedData.push_back(static_cast<unsigned char>(ch));
  }
  if (seedData.empty()) {
    seedData.push_back(0);
  }

  std::seed_seq seq(seedData.begin(), seedData.end());
  std::mt19937_64 rng(seq);
  uint64_t ab = rng();
  uint64_t cd = rng();

  // Set RFC 4122 variant + version 4 bits for a UUID-like format.
  ab = (ab & 0xFFFFFFFFFFFF0FFFULL) | 0x0000000000004000ULL;
  cd = (cd & 0x3FFFFFFFFFFFFFFFULL) | 0x8000000000000000ULL;

  return kj::str(sole::rebuild(ab, cd).str());
}

} // namespace

struct Gateway::Impl {

  struct Heartbeat final : public mas::schema::persistence::Heartbeat::Server {
    kj::String capId;
    Gateway::Impl &gwImpl;

    Heartbeat(Gateway::Impl &gwImpl, kj::StringPtr capId) : gwImpl(gwImpl), capId(kj::str(capId)) {}

    virtual ~Heartbeat() = default;

    kj::Promise<void> beat(BeatContext context) override {
      gwImpl.keepAlive(capId);
      return kj::READY_NOW;
    }
  };

  Gateway &self;
  Restorer *restorerPtr{nullptr};
  mas::schema::persistence::Restorer::Client restorerClient{nullptr};
  kj::Timer &timer;
  kj::String id;
  kj::String name{kj::str("Gateway")};
  kj::String description;
  kj::Duration secsKeepAliveTimeout{10 * 60 * kj::SECONDS};
  uint16_t count{0};

  kj::HashMap<kj::String, kj::Tuple<uint8_t, mas::schema::persistence::Persistent::ReleaseSturdyRef::Client>>
      id2CountAndUnsaveCap;
  // the mapping from id to keep alive count (which should never get 0) and unsaveCap

  Impl(Gateway &self, kj::Timer &timer, kj::StringPtr name, kj::StringPtr description, uint32_t secsKeepAliveTimeout)
      : self(self), timer(timer), id(kj::str(sole::uuid4().str())), name(kj::str(name)),
        description(kj::str(description)), secsKeepAliveTimeout(secsKeepAliveTimeout * kj::SECONDS) {}

  ~Impl() = default;

  kj::Promise<void> garbageCollectMappings(bool runOnce = false) {
    KJ_DBG("garbageCollectMappings", runOnce, count++);
    kj::Vector<kj::String> toBeGarbageCollectedKeys;
    for (auto &entry : id2CountAndUnsaveCap) {
      auto aliveCount = kj::get<0>(entry.value);
      if (aliveCount == 0)
        toBeGarbageCollectedKeys.add(kj::str(entry.key));
      else if (aliveCount > 0)
        kj::get<0>(entry.value) = aliveCount - 1;
    }

    auto proms = kj::heapArrayBuilder<kj::Promise<bool>>(toBeGarbageCollectedKeys.size() + (runOnce ? 0 : 1));
    for (kj::StringPtr key : toBeGarbageCollectedKeys) {
      KJ_IF_MAYBE (val, id2CountAndUnsaveCap.find(key)) {
        auto unsaveCap = kj::get<1>(*val);
        proms.add(unsaveCap.releaseRequest().send().then([](auto &&resp) { return resp.getSuccess(); }));
        KJ_DBG("removed mapping", key);
      }
      id2CountAndUnsaveCap.erase(key);
    }

    if (runOnce)
      return kj::joinPromises(proms.finish()).ignoreResult();
    proms.add(timer.afterDelay(secsKeepAliveTimeout * 3).then([]() { return false; }));
    return kj::joinPromises(proms.finish()).ignoreResult().then([this]() { return garbageCollectMappings(); });
  }

  void keepAlive(kj::StringPtr capId) {
    KJ_IF_MAYBE (hp, id2CountAndUnsaveCap.find(capId)) {
      kj::get<0>(*hp) = 1;
    }
  }

  kj::Promise<bool> addAndStoreMapping(kj::StringPtr capId,
                                       schema::persistence::Persistent::ReleaseSturdyRef::Client unsaveCap) {
    auto updateFunc = [](auto &existingValue, auto &&newValue) { existingValue = kj::mv(newValue); };
    // id2HostPort.upsert(kj::str(capId), kj::tuple(kj::str(host), port, 1), updateFunc);
    id2CountAndUnsaveCap.upsert(kj::str(capId), kj::tuple(1, kj::mv(unsaveCap)), updateFunc);
    KJ_DBG("added mapping", capId);
    return true;
  }
};

Gateway::Gateway(kj::Timer &timer, kj::StringPtr name, kj::StringPtr description, uint32_t secsKeepAliveTimeout)
    : impl(kj::heap<Impl>(*this, timer, name, description, secsKeepAliveTimeout)) {}

Gateway::~Gateway() = default;

kj::Promise<void> Gateway::info(InfoContext context) {
  KJ_LOG(INFO, "info message received");
  auto rs = context.getResults();
  rs.setId(impl->id);
  rs.setName(impl->name);
  rs.setDescription(impl->description);
  return kj::READY_NOW;
}

kj::Promise<void> Gateway::restore(RestoreContext context) {
  auto req = impl->restorerClient.restoreRequest();
  auto params = context.getParams();
  req.setLocalRef(params.getLocalRef());
  if (params.hasSealedBy())
    req.setSealedBy(params.getSealedBy());
  return req.send().then([context](auto &&resp) mutable { context.getResults().setCap(resp.getCap()); });
}

kj::Promise<void> Gateway::register_(RegisterContext context) {
  auto params = context.getParams();
  if (params.hasCap()) {
    auto cap = params.getCap();
    const bool hasSeed = params.hasSecretSeed() && params.getSecretSeed().size() > 0;
    kj::String capId = hasSeed ? deterministicUuid(params.getSecretSeed()) : kj::str(sole::uuid4().str());

    kj::Promise<void> preRelease = kj::READY_NOW;
    if (hasSeed) {
      KJ_IF_MAYBE (existing, impl->id2CountAndUnsaveCap.find(capId)) {
        auto oldUnsaveCap = kj::get<1>(*existing);
        impl->id2CountAndUnsaveCap.erase(capId);
        preRelease = oldUnsaveCap.releaseRequest().send().ignoreResult();
      }
    }

    return preRelease.then([this, context, cap, KJ_MVCAP(capId)]() mutable {
      capnp::MallocMessageBuilder msg;
      auto srb = context.getResults().initSturdyRef();
      auto unsaveSrb = msg.initRoot<mas::schema::persistence::SturdyRef>();
      return impl->restorerPtr->save(cap, srb, unsaveSrb, capId)
          .then([this, context, KJ_MVCAP(capId)](auto &&unsaveCap) mutable {
            return impl->addAndStoreMapping(capId, unsaveCap)
                .then([this, context, KJ_MVCAP(capId)](bool success) mutable {
                  auto res = context.getResults();
                  if (success) {
                    auto hb = kj::heap<Impl::Heartbeat>(*impl.get(), capId);
                    res.setHeartbeat(kj::mv(hb));
                    res.setSecsHeartbeatInterval(impl->secsKeepAliveTimeout / kj::SECONDS);
                  }
                });
          });
    });
  }
  return kj::READY_NOW;
}

void Gateway::setRestorer(Restorer *restorer, mas::schema::persistence::Restorer::Client client) {
  impl->restorerPtr = restorer;
  impl->restorerClient = client;
}

kj::Promise<void> Gateway::garbageCollectMappings(bool runOnce) { return impl->garbageCollectMappings(runOnce); }
