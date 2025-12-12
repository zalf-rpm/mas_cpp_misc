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

#include <iostream>

#include <kj/debug.h>
#include <kj/common.h>
#include <kj/main.h>
#include <kj/string.h>
#include <kj/tuple.h>
#include <kj/vector.h>

#include "channel.h"
#include "common.h"
#include "restorable-service-main.h"
#include "sole.hpp"

#include "common.capnp.h"
#include "fbp.capnp.h"

namespace mas { 
namespace infrastructure { 
namespace common {

class ChannelMain : public RestorableServiceMain
{
public:
  ChannelMain(kj::ProcessContext& context) 
  : RestorableServiceMain(context, "Channel v0.2", "Service to offer multiple channels.")
  {}

  kj::MainBuilder::Validity setNoOfChannels(kj::StringPtr no) {
    noOfChannels = std::max(1, std::stoi(no.cStr()));
    return true;
  }

  kj::MainBuilder::Validity setBufferSize(kj::StringPtr size) {
    bufferSize = std::max(1, std::stoi(size.cStr()));
    return true;
  }

  kj::MainBuilder::Validity setExitTimeout(kj::StringPtr timeoutInSeconds) {
    exitTimeout = std::max(1, std::stoi(timeoutInSeconds.cStr()));
    return true;
  }


  kj::MainBuilder::Validity setNoOfReaderWriterPairs(kj::StringPtr no) {
    noOfReadersPerChannel = noOfWritersPerChannel = static_cast<uint8_t>(std::max(1UL, std::min(std::stoul(no.cStr()), 255UL)));
    return true;
  }

  kj::MainBuilder::Validity setNoOfReaders(kj::StringPtr no) {
    noOfReadersPerChannel = static_cast<uint8_t>(std::max(1UL, std::min(std::stoul(no.cStr()), 255UL)));
    return true;
  }

  kj::MainBuilder::Validity setNoOfWriters(kj::StringPtr no) {
    noOfWritersPerChannel = static_cast<uint8_t>(std::max(1UL, std::min(std::stoul(no.cStr()), 255UL)));
    return true;
  }

  kj::MainBuilder::Validity setReaderSrts(kj::StringPtr name) {
    for(auto& per_channel : splitString(name, "+")) {
      kj::Vector<kj::String> srtsPerChannel;
      for(auto& srt : splitString(per_channel, ",")) {
        srtsPerChannel.add(kj::str(srt));
      }
      readerSrts.add(kj::mv(srtsPerChannel));
    }
    return true;
  }

  kj::MainBuilder::Validity setWriterSrts(kj::StringPtr name) {
    for(auto& per_channel : splitString(name, "+")) {
      kj::Vector<kj::String> srtsPerChannel;
      for(auto& srt : splitString(per_channel, ",")) {
        srtsPerChannel.add(kj::str(srt));
      }
      writerSrts.add(kj::mv(srtsPerChannel));
    }
    return true;
  }

  kj::MainBuilder::Validity startChannel() {
    for (auto c = 0; c < noOfChannels; c++) {
      auto &rsrts = c < readerSrts.size() ? readerSrts[c] : readerSrts.add(kj::Vector<kj::String>());
      for (auto i = 0; i < noOfReadersPerChannel; i++) {
        if (i >= rsrts.size()) {
          rsrts.add(kj::str(sole::uuid4().str()));
        }
      }
      auto &wsrts = c < writerSrts.size() ? writerSrts[c] : writerSrts.add(kj::Vector<kj::String>());
      for (auto i = 0; i < noOfWritersPerChannel; i++) {
        if (i >= wsrts.size()) {
          wsrts.add(kj::str(sole::uuid4().str()));
        }
      }
    }

    startRestorerSetup(nullptr);

    KJ_LOG(INFO, "starting channel(s)");

    for (auto i = 0; i < noOfChannels; i++) {
      auto ownedChannel = kj::heap<Channel>(name, description, bufferSize);
      auto channel = ownedChannel.get();
      AnyPointerChannel::Client channelClient = kj::mv(ownedChannel);
      KJ_LOG(INFO, "created channel");

      channel->setRestorer(restorer);

      using SI = mas::schema::fbp::Channel<capnp::AnyPointer>::StartupInfo;
      using P = mas::schema::common::Pair<capnp::Text, SI>;
      using SIC = mas::schema::fbp::Channel<P>;

      kj::Maybe<SI::Builder> startupInfo;
      kj::Maybe<capnp::Request<SIC::Msg, SIC::ChanWriter::WriteResults>> infoReq;
      KJ_IF_MAYBE(anyOut, startupInfoWriterClient){
        auto out = anyOut->castAs<SIC::ChanWriter>();
        infoReq = out.writeRequest();
        KJ_IF_MAYBE(req, infoReq){
          auto p = req->initValue();
          p.setFst(startupInfoWriterSRId);
          auto info = p.initSnd();
          info.setBufferSize(bufferSize);
          info.setChannel(channelClient);

          restorer->save(channelClient, info.initChannelSR(), nullptr, nullptr, nullptr, false).wait(ioContext.waitScope);
          auto channelSRUrl = restorer->sturdyRefStr(info.getChannelSR().getLocalRef().getText());
          if(outputSturdyRefs && channelSRUrl.size() > 0) std::cout << "channelSR=" << channelSRUrl.cStr() << std::endl;

          info.initReaders(readerSrts.size());
          info.initReaderSRs(readerSrts.size());
          info.initWriters(writerSrts.size());
          info.initWriterSRs(writerSrts.size());
          startupInfo = info;
        }
      } else {
        auto channelSR = restorer->saveStr(channelClient, nullptr, nullptr, false).wait(ioContext.waitScope).sturdyRef;
        if(outputSturdyRefs && channelSR.size() > 0) std::cout << "channelSR=" << channelSR.cStr() << std::endl;
      }

      auto &rsrts = readerSrts[i];
      for(auto k = 0; k < noOfReadersPerChannel; k++){
        const auto& srt = rsrts[k];
        auto reader = channelClient.readerRequest().send().wait(ioContext.waitScope).getR();
        KJ_IF_MAYBE(info, startupInfo){
          restorer->save(reader, info->getReaderSRs()[k], nullptr, nullptr, nullptr, false).wait(ioContext.waitScope);
          auto readerSRUrl = restorer->sturdyRefStr(info->getReaderSRs()[k].getLocalRef().getText());
          if(outputSturdyRefs && readerSRUrl.size() > 0) std::cout << "\treaderSR=" << readerSRUrl.cStr() << std::endl;
          info->getReaders().set(k, kj::mv(reader));
        } else {
          auto readerSR = restorer->saveStr(reader, srt, nullptr, false, nullptr, false).wait(ioContext.waitScope).sturdyRef;
          if(outputSturdyRefs && readerSR.size() > 0) std::cout << "\treaderSR=" << readerSR.cStr() << std::endl;
        }
      }
      auto &wsrts = writerSrts[i];
      for(auto k = 0; k < noOfWritersPerChannel; k++){
        const auto& srt = wsrts[k];
        auto writer = channelClient.writerRequest().send().wait(ioContext.waitScope).getW();
        KJ_IF_MAYBE(info, startupInfo){
          restorer->save(writer, info->getWriterSRs()[k], nullptr, nullptr, nullptr, false).wait(ioContext.waitScope);
          auto writerSRUrl = restorer->sturdyRefStr(info->getWriterSRs()[k].getLocalRef().getText());
          if(outputSturdyRefs && writerSRUrl.size() > 0) std::cout << "\twriterSR=" << writerSRUrl.cStr() << std::endl;
          info->getWriters().set(k, kj::mv(writer));
        } else {
          auto writerSR = restorer->saveStr(writer, srt, nullptr, false, nullptr, false).wait(ioContext.waitScope).sturdyRef;
          if(outputSturdyRefs && writerSR.size() > 0) std::cout << "\twriterSR=" << writerSR.cStr() << std::endl;
        }
      }

      KJ_IF_MAYBE(req, infoReq){
        req->send().wait(ioContext.waitScope);
      }

      auto cd = kj::heap<ChannelData>(kj::mv(channelClient), channel);
      allChannelData.add(kj::mv(cd));
      channels.add(*allChannelData.back());
    }

    // Run forever, accepting connections and handling requests.
    kj::Timer& timer = ioContext.provider->getTimer();
    while (true) {
      timer.afterDelay(exitTimeout * kj::SECONDS).wait(ioContext.waitScope);
      if (channels.empty()) break;
      for (auto& cd : channels) {
        if (cd.channel->canBeClosed()) channels.remove(cd);
      }

    }
    KJ_LOG(INFO, "stopped channels vat");
    return true;
  }

  kj::MainFunc getMain()
  {
    return addRestorableServiceOptions()
      .addOptionWithArg({'#', "no_of_channels"}, KJ_BIND_METHOD(*this, setNoOfChannels),
                        "<no_of_channels=1>", "Set the number of channels to start.")
      .addOptionWithArg({'b', "buffer_size"}, KJ_BIND_METHOD(*this, setBufferSize),
                        "<buffer_size=1>", "Set buffer size of channel.")
      .addOptionWithArg({'c', "create"}, KJ_BIND_METHOD(*this, setNoOfReaderWriterPairs),
                        "<number_of_reader_writer_pairs (default: 1)>",
                        "Create number of reader/writer pairs per channel.")
      .addOptionWithArg({'R', "no_of_readers"}, KJ_BIND_METHOD(*this, setNoOfReaders),
                        "<number_of_readers (default: 1)>",
                        "Create this number of readers per channel.")
      .addOptionWithArg({'W', "no_of_writers"}, KJ_BIND_METHOD(*this, setNoOfWriters),
                        "<number_of_writers (default: 1)>",
                        "Create number of readers per channel.")
      .addOptionWithArg({'r', "reader_srts"}, KJ_BIND_METHOD(*this, setReaderSrts),
                        "<Sturdy_ref_token_1_Channel_1,[Sturdy_ref_token_2_Channel_1],...+Sturdy_ref_token_1_Channel_2,[Sturdy_ref_token_2_Channel_2],...>",
                        "Create readers for given sturdy ref tokens per channel.")
      .addOptionWithArg({'w', "writer_srts"}, KJ_BIND_METHOD(*this, setWriterSrts),
                        "<Sturdy_ref_token_1_Channel_1,[Sturdy_ref_token_2_Channel_1],...+Sturdy_ref_token_1_Channel_2,[Sturdy_ref_token_2_Channel_2],...>",
                        "Create writers for given sturdy ref tokens per channel.")
    .addOptionWithArg({'t', "exit_timeout"}, KJ_BIND_METHOD(*this, setExitTimeout),
                        "<exit_timeout=3>", "Exit channel vat after 'exit_timeout' (must be >= 1) seconds after last channel closed.")
      .callAfterParsing(KJ_BIND_METHOD(*this, startChannel))
      .build();
  }

private:
  uint64_t bufferSize{1};
  uint64_t exitTimeout{3};
  uint64_t noOfChannels{1};
  uint8_t noOfReadersPerChannel{1};
  uint8_t noOfWritersPerChannel{1};
  struct ChannelData {
    AnyPointerChannel::Client client{nullptr};
    Channel* channel;
    kj::ListLink<ChannelData> link;
    ChannelData(AnyPointerChannel::Client client, Channel* channel)
    : client(kj::mv(client)), channel(channel) {}
  };
  kj::List<ChannelData, &ChannelData::link> channels;
  kj::Vector<kj::Own<ChannelData>> allChannelData;
  kj::Vector<kj::Vector<kj::String>> readerSrts;
  kj::Vector<kj::Vector<kj::String>> writerSrts;
};

} // namespace common
} // namespace infrastructure
} // namespace mas

KJ_MAIN(mas::infrastructure::common::ChannelMain)
