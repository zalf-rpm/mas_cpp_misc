/* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/*
Authors: 
Michael Berg <michael.berg@zalf.de>

Maintainers: 
Currently maintained by the authors.

This file is part of the util library used by models created at the Institute of 
Landscape Systems Analysis at the ZALF.
Copyright (C) Leibniz Centre for Agricultural Landscape Research (ZALF)
*/

#include "climate-common.h"

#include <iostream>
#include <sstream>
#include <algorithm>
#include <cassert>
#include <limits>
#include <utility>

#include "tools/algorithms.h"

using namespace std;
using namespace Climate;
using namespace Tools;

string Climate::availableClimateData2CLMDBColName(AvailableClimateData col) {
  switch (col) {
    case day:
      return "tag";
    case month:
      return "monat";
    case year:
      return "jahr";
    case tmin:
      return "tn";
    case tavg:
      return "tm";
    case tmax:
      return "tx";
    case precip:
      return "rr_corr";
    case precipOrig:
      return "rr";
    case globrad:
      return "0";
    case relhumid:
      return "rf";
    case airpress:
      return "pp";
    case sunhours:
      return "sd";
    case cloudamount:
      return "nn";
    case vaporpress:
      return "dd";
    case wind:
      return "ff";
    default:;
  }
  return "error";
}

string Climate::availableClimateData2WerexColName(AvailableClimateData col) {
  switch (col) {
    case day:
      return "tag";
    case month:
      return "mo";
    case year:
      return "jahr";
    case tmin:
      return "tmin";
    case tavg:
      return "tmit";
    case tmax:
      return "tmax";
    case precip:
      return "rr";
    case precipOrig:
      return "rr";
    case globrad:
      return "0";
    case relhumid:
      return "rf";
    case airpress:
      return "pp";
    case sunhours:
      return "sd";
    case cloudamount:
      return "nn";
    case vaporpress:
      return "dd";
    case wind:
      return "ff";
    default:;
  }
  return "error";
}

string Climate::availableClimateData2WettRegDBColName(AvailableClimateData col) {
  switch (col) {
    case day:
      return "tag";
    case month:
      return "mo";
    case year:
      return "jahr";
    case tmin:
      return "tmin";
    case tavg:
      return "tmit";
    case tmax:
      return "tmax";
    case precip:
      return "nied";
    case precipOrig:
      return "nied";
    case globrad:
      return "stra";
    case relhumid:
      return "relf";
    case airpress:
      return "ludr";
    case sunhours:
      return "sonn";
    case cloudamount:
      return "bewo";
    case vaporpress:
      return "dadr";
    case wind:
      return "wind";
    default:;
  }
  return "error";
}

pair<string, int>
Climate::availableClimateData2CarbiocialDBColNameAndScaleFactor(AvailableClimateData col) {
  switch (col) {
    case day:
      return make_pair("day", 0);
    case month:
      return make_pair("month", 0);
    case year:
      return make_pair("year", 0);
    case tmin:
      return make_pair("t_min_times_10", 10);
    case tavg:
      return make_pair("t_avg_times_10", 10);
    case tmax:
      return make_pair("t_max_times_10", 10);
    case precip:
      return make_pair("precipitation_mm_times_10", 10);
    case precipOrig:
      return make_pair("precipitation_mm_times_10", 10);
    case globrad:
      return make_pair("global_radiation_mj_per_m2_times_100", 100);
    case relhumid:
      return make_pair("relative_humidity_percent_times_10", 10);
      //case airpress: return make_pair("ludr", 0);
      //case sunhours: return make_pair("sonn", 0);
      //case cloudamount: return make_pair("bewo", 0);
      //case vaporpress: return make_pair("dadr", 0);
    case wind:
      return make_pair("windspeed_m_per_s_times_10", 10);
    default:;
  }
  return make_pair("error", 0);
}

string
Climate::availableClimateData2UserSqliteDBColNameAndScaleFactor(AvailableClimateData col) {
  switch (col) {
    case day:
      return "day";
    case month:
      return "month";
    case year:
      return "year";
    case tmin:
      return "t_min";
    case tavg:
      return "t_avg";
    case tmax:
      return "t_max";
    case precip:
      return "precipitation_mm";
    case precipOrig:
      return "precipitation_mm";
    case globrad:
      return "global_radiation_mj_per_m2";
    case relhumid:
      return "relative_humidity_percent";
      //case airpress: return "ludr";
      //case sunhours: return "sonn";
      //case cloudamount: return "bewo";
      //case vaporpress: return "dadr";
    case wind:
      return "windspeed_m_per_s";
    default:;
  }
  return "error";
}

std::string Climate::availableClimateData2Name(AvailableClimateData col) {
  switch (col) {
    case day:
      return "Tag";
    case month:
      return "Monat";
    case year:
      return "Jahr";
    case tmin:
      return "Tmin";
    case tavg:
      return "Tmit";
    case tmax:
      return "Tmax";
    case precip:
      return "Niederschlag";
    case precipOrig:
      return "Niederschlag (unkorrigiert)";
    case globrad:
      return "Globlstrahlung";
    case relhumid:
      return "relative Luftfeuchte";
    case airpress:
      return "Luftdruck";
    case sunhours:
      return "Sonnenscheindauer";
    case cloudamount:
      return "Bewölkungsgrad";
    case vaporpress:
      return "Dampfdruck";
    case wind:
      return "Windgeschwindigkeit";
    case co2:
      return "CO2";
    case et0:
      return "Reference-Evapotranspiration";
    default:;
  }
  return "unknown";
}

std::string Climate::availableClimateData2unit(AvailableClimateData col) {
  switch (col) {
    case day:
      return "d";
    case year:
      return "a";
    case tmin:
      return "°C";
    case tavg:
      return "°C";
    case tmax:
      return "°C";
    case precip:
      return "mm";
    case precipOrig:
      return "mm";
    case sunhours:
      return "h";
    case wind:
      return "m/s";
    case co2:
      return "ppm";
    case et0:
      return "mm";
    default:;
  }
  return "";
}

/*
const ACDV& acds()
{
	static ACDV v;
	static bool isInitialized = false;
  if(!isInitialized)
  {
		ACD t[] = { day, month, year, tmin, tavg, tmax, precip, precipOrig, globrad,
		            wind, sunhours, cloudamount, relhumid, airpress, vaporpress, co2 };
		v.insert(v.end(), t, t + availableClimateDataSize());
		isInitialized = true;
	}
	return v;
}
*/

YearRange Climate::snapToRaster(YearRange yr, int raster) {
  int from = yr.fromYear;
  int to = yr.toYear;

  int fmod = from % raster;
  if (fmod != 1)
    from = from + (fmod == 0 ? 0 : raster - fmod) + 1;
  int tmod = to % raster;
  if (tmod != 0)
    to = to - tmod;

  return {from, to};
}

DataAccessor::DataAccessor()
    : _data(new VVD), _acd2dataIndex(availableClimateDataSize(), -1) {}

DataAccessor::DataAccessor(Tools::Date startDate,
                           Tools::Date endDate)
    : _startDate(std::move(startDate)), _endDate(std::move(endDate)), _data(new VVD), _acd2dataIndex(availableClimateDataSize(), -1) {}

DataAccessor::DataAccessor(const DataAccessor &other)
    : _startDate(other._startDate), _endDate(other._endDate), _data(other._data), _acd2dataIndex(other._acd2dataIndex),
      _fromStep(other._fromStep), _numberOfSteps(other._numberOfSteps), _tamp(other._tamp), _tav(other._tav) {}

DataAccessor::DataAccessor(DataAccessor &&other) noexcept
    : _startDate(kj::mv(other._startDate)), _endDate(kj::mv(other._endDate)), _data(kj::mv(other._data)),
      _acd2dataIndex(kj::mv(other._acd2dataIndex)), _fromStep(other._fromStep), _numberOfSteps(other._numberOfSteps),
_tamp(other._tamp), _tav(other._tav) {
  other._fromStep = 0;
  other._numberOfSteps = 0;
}

DataAccessor& DataAccessor::operator=(const DataAccessor &other) {
  if (this == &other) return *this;

  _startDate = other._startDate;
  _endDate = other._endDate;
  _data = other._data;
  _acd2dataIndex = other._acd2dataIndex;
  _fromStep = other._fromStep;
  _numberOfSteps = other._numberOfSteps;
  _tamp = other._tamp;
  _tav = other._tav;

  return *this;
}

DataAccessor& DataAccessor::operator=(DataAccessor &&other) noexcept {
  if (this == &other) return *this;

  _startDate = kj::mv(other._startDate);
  _endDate = kj::mv(other._endDate);
  _data = kj::mv(other._data);
  _acd2dataIndex = kj::mv(other._acd2dataIndex);
  _fromStep = other._fromStep;
  other._fromStep = 0;
  _numberOfSteps = other._numberOfSteps;
  other._numberOfSteps = 0;
  _tamp = other._tamp;
  _tav = other._tav;

  return *this;
}

DataAccessor::DataAccessor(json11::Json j) {
  merge(j);
}

Errors DataAccessor::merge(json11::Json j) {
  Errors res = Json11Serializable::merge(j);
  for (const auto &acd2data: j["data"].object_items()) {
    addOrReplaceClimateData(ACD(stoi(acd2data.first)), double_vector(acd2data.second));
  }

  try {
    set_iso_date_value(_startDate, j, "startDate");
  } catch (const std::exception &e) {
    res.appendError(kj::str("Climate data: Couldn't parse startDate as iso-date: ", j["startDate"].dump(), "! Exception: ", e.what()).cStr());
  }
  try {
    set_iso_date_value(_endDate, j, "endDate");
  } catch (const std::exception &e) {
    res.appendError(kj::str("Climate data: Couldn't parse endDate as iso-date: ", j["endDate"].dump(), "! Exception: ", e.what()).cStr());
  }

  set_double_value(_tamp, j, "tamp");
  set_double_value(_tav, j, "tav");

  return res;
}

json11::Json DataAccessor::to_json() const {
  J11Object data;
  int acd = 0;
  for (auto i: _acd2dataIndex) {
    if (i >= 0) {
      data[to_string(acd)] = toPrimJsonArray(dataAsVector(ACD(acd)));
    }
    acd++;
  }

  return J11Object
      {{"type",      "DataAccessor"},
       {"data",      data},
       {"startDate", startDate().toIsoDateString()},
       {"endDate",   endDate().toIsoDateString()},
        {"tamp", _tamp},
        {"tav", _tav},
      };
}

double DataAccessor::dataForTimestep(AvailableClimateData acd,
                                     size_t stepNo,
                                     double def) const {
  short cacheIndex = _acd2dataIndex.at(int(acd));
  return cacheIndex < 0 ? def : _data->at(cacheIndex).at(_fromStep + stepNo);
}

Maybe<double> DataAccessor::dataForTimestepM(AvailableClimateData acd,
                                             size_t stepNo) const {
  short cacheIndex = _acd2dataIndex.at(int(acd));
  return cacheIndex < 0 ? Maybe<double>() : _data->at(cacheIndex).at(_fromStep + stepNo);
}

map<Climate::ACD, double>
DataAccessor::allDataForStep(size_t stepNo,
                             double latitude) const {
  map<ACD, double> m;

  for (size_t k = 0, size = _acd2dataIndex.size(); k < size; k++) {
    ACD acd = ACD(int(k));
    auto v = dataForTimestepM(acd, stepNo);
    if (v.isValue())
      m[acd] = v.value();
    else if (acd == globrad) {
      auto v2 = dataForTimestepM(sunhours, stepNo);
      if (v2.isValue())
        m[acd] = Tools::sunshine2globalRadiation(dateForStep(stepNo).julianDay(),
                                                 v2.value(),
                                                 latitude,
                                                 true);
    }
  }

  return m;
}

vector<double> DataAccessor::dataAsVector(AvailableClimateData acd) const {
  short cacheIndex = _acd2dataIndex.at(int(acd));
  return cacheIndex < 0
         ? vector<double>()
         : vector<double>(_data->at(cacheIndex).begin() + _fromStep,
                          _data->at(cacheIndex).begin() + _fromStep + noOfStepsPossible());
}

DataAccessor DataAccessor::cloneForRange(size_t fromStep,
                                         size_t numberOfSteps) const {
  //cout << "cloneForRange fromStep: " << fromStep <<
  //" numberOfSteps: " << numberOfSteps << endl;
  if (!isValid()
      || fromStep > noOfStepsPossible()
      || (fromStep + numberOfSteps) > noOfStepsPossible())
    return {}; //numberOfSteps = fromStep = 0;

  DataAccessor clone(*this);
  clone._fromStep += fromStep;
  clone._numberOfSteps = numberOfSteps;
  clone._startDate = clone._startDate + (uint64_t) clone._fromStep;
  clone._endDate = clone._startDate + (uint64_t) numberOfSteps - 1;
  return clone;
}

void DataAccessor::addClimateData(AvailableClimateData acd,
                                  const vector<double> &data) {
  if (!_data->empty())
    assert(_numberOfSteps = data.size());

  _data->push_back(data);
  _acd2dataIndex[int(acd)] = short(_data->size() - 1);
  _numberOfSteps = _data->empty() ? 0 : _data->front().size();
}

void DataAccessor::addClimateData(AvailableClimateData acd,
                                  const vector<double> &&data) {
  if (!_data->empty())
    assert(_numberOfSteps = data.size());

  _data->push_back(data);
  _acd2dataIndex[int(acd)] = short(_data->size() - 1);
  _numberOfSteps = _data->empty() ? 0 : _data->front().size();
}

void DataAccessor::mergeClimateData(DataAccessor other,
                                    bool replaceOverlappingData) {
  if (!other.isValid())
    return;

  if (!isValid()) {
    (*this) = kj::mv(other);
    return;
  }

  auto range = [](const DataAccessor& da) {
    return "(" + da.startDate().toIsoDateString() + " - " + da.endDate().toIsoDateString() + ")";
  };
  // the names used in CSV headers (tmin, precip, co2, ...)
  auto acdName = [](size_t i) {
    static const auto names = [] {
      std::map<ACD, std::string> m;
      for (const auto& p : name2acd()) m.emplace(p.second, p.first);
      return m;
    }();
    auto it = names.find(ACD(i));
    return it == names.end() ? availableClimateData2Name(ACD(i)) : it->second;
  };

  if ((startDate() > other.endDate() + 1) || (other.startDate() > endDate() + 1)) {
    cout
        << "Can't prepend or append climate data because together data don't give a continuous timeseries. "
        << "current: " << range(*this) << " new: " << range(other) << ". "
        << "New climate data won't be appended or prepended." << endl;
    return;
  }

  // Every climate element of the result has to cover its whole time range. So
  // the range can only grow beyond this' range if other has all of this'
  // climate elements (to fill the added days); else only the part of other
  // overlapping this' range is used.
  std::string missingInOther;
  for (size_t i = 0; i < _acd2dataIndex.size(); i++) {
    if (_acd2dataIndex[i] >= 0 && other._acd2dataIndex[i] < 0)
      missingInOther += (missingInOther.empty() ? "" : ", ") + acdName(i);
  }
  auto newStart = startDate();
  auto newEnd = endDate();
  bool otherExtendsRange = other.startDate() < startDate() || other.endDate() > endDate();
  if (otherExtendsRange) {
    if (missingInOther.empty()) {
      if (other.startDate() < newStart) newStart = other.startDate();
      if (other.endDate() > newEnd) newEnd = other.endDate();
    } else {
      cout
          << "Can't extend climate data " << range(*this) << " with " << range(other)
          << ", because the new data lack " << missingInOther << ". "
          << "Only the new data within " << range(*this) << " will be used." << endl;
    }
  }

  const size_t n = size_t(newEnd - newStart) + 1;
  // the days [from, to] of the result other has data for
  auto oFrom = other.startDate() > newStart ? other.startDate() : newStart;
  auto oTo = other.endDate() < newEnd ? other.endDate() : newEnd;
  const size_t oFromPos = size_t(oFrom - newStart), oToPos = size_t(oTo - newStart);
  const size_t thisFromPos = size_t(startDate() - newStart), thisToPos = size_t(endDate() - newStart);
  bool otherCoversResult = other.startDate() <= newStart && other.endDate() >= newEnd;

  // build the merged data anew instead of changing *_data in place: copies of
  // this DataAccessor (e.g. from cloneForRange) share it
  auto data = std::make_shared<VVD>();
  std::vector<short> acd2dataIndex(_acd2dataIndex.size(), -1);
  for (size_t i = 0; i < _acd2dataIndex.size(); i++) {
    short ti = _acd2dataIndex[i];
    short oi = other._acd2dataIndex[i];
    if (ti < 0 && oi < 0) continue;

    if (ti < 0 && !otherCoversResult) {
      cout
          << "Can't add climate element " << acdName(i) << " from " << range(other)
          << ", because it doesn't cover the whole time range (" << newStart.toIsoDateString() << " - "
          << newEnd.toIsoDateString() << ")." << endl;
      continue;
    }

    std::vector<double> v(n);
    if (ti >= 0) {
      const auto& td = (*_data)[ti];
      std::copy_n(td.begin() + long(_fromStep), thisToPos - thisFromPos + 1, v.begin() + long(thisFromPos));
    }
    if (oi >= 0) {
      const auto& od = (*other._data)[oi];
      auto oData = od.begin() + long(other._fromStep + size_t(oFrom - other.startDate()));
      for (size_t pos = oFromPos; pos <= oToPos; pos++, oData++) {
        bool inThisRange = ti >= 0 && pos >= thisFromPos && pos <= thisToPos;
        if (!inThisRange || replaceOverlappingData) v[pos] = *oData;
      }
    }
    data->push_back(kj::mv(v));
    acd2dataIndex[i] = short(data->size() - 1);
  }

  _data = kj::mv(data);
  _acd2dataIndex = kj::mv(acd2dataIndex);
  _fromStep = 0;
  _numberOfSteps = n;
  _startDate = newStart;
  _endDate = newEnd;
}

void DataAccessor::addOrReplaceClimateData(AvailableClimateData acd,
                                           const vector<double> &data) {
  int index = _acd2dataIndex[int(acd)];
  if (index < 0) {
    addClimateData(acd, data);
  } else {
    if (!_data->empty())
      assert(_numberOfSteps = data.size());

    (*_data)[index] = data;
  }
}

std::pair<double, double> DataAccessor::getTAMPandTAV() {
  if (static_cast<int>(_tamp) == -9999 || static_cast<int>(_tav) == -9999) {
    auto tt = calcTAMPandTAV();
    if (static_cast<int>(_tamp) == -9999) _tamp = tt.first;
    if (static_cast<int>(_tav) == -9999) _tav = tt.second;
  }
  return make_pair(_tamp, _tav);
}

void DataAccessor::setTAMPandTAV(double tamp, double tav) {
  _tamp = tamp;
  _tav = tav;
}

std::pair<double, double> DataAccessor::calcTAMPandTAV() const {
  map<int, vector<double>> month2avgs;
  for(int i = 1; i < 13; i++) month2avgs[i] = vector<double>();
  double sum = 0;
  int count = 0;
  int prevMonth = 0;
  for (size_t i = 0; i < noOfStepsPossible(); i++) {
    auto d = dateForStep(i);
    auto currentMonth = d.month();
    if (prevMonth == 0) prevMonth = currentMonth;
    if (currentMonth != prevMonth) {
      if (count > 0) month2avgs[prevMonth].push_back(sum / count);
      else month2avgs[prevMonth].push_back(0);
      sum = 0;
      count = 0;
      prevMonth = currentMonth;
    }
    sum += dataForTimestep(tavg, i);
    count++;
  }
  vector<double> monthlyAvgs;
  for(int i = 1; i < 13; i++){
    monthlyAvgs.push_back(average(month2avgs[i]));
  }
  auto mm = minMax(monthlyAvgs);
  return make_pair(mm.second - mm.first, average(monthlyAvgs));
}

