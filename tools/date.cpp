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

#include "date.h"

#include <algorithms.h>
#include <sstream>
#include <cmath>
#include <cassert>
#include <iostream>

using namespace Tools;
using namespace std;

const std::vector<uint8_t> *Date::_dim() {
  static vector<uint8_t> dim{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  return &dim;
}

const std::vector<uint8_t> *Date::_ldim() {
  static vector<uint8_t> dim{0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  return &dim;
}

//const uint Date::defaultRelativeBaseYear = 2000;

//! default constructor
Date::Date(bool useLeapYears)
    : _useLeapYears(useLeapYears) {
  _daysInMonth = isLeapYear() && _useLeapYears ? _ldim() : _dim();
}

Date::Date(const string &isoDateString,
           bool useLeapYears)
    : _useLeapYears(useLeapYears) {
  Date d = fromIsoDateString(isoDateString);
  _d = d.day();
  _m = d.month();
  _y = d.year();
  _daysInMonth = isLeapYear() && _useLeapYears ? _ldim() : _dim();
}

/*!
 * construct a date with a named month
 * @param day
 * @param month
 * @param year
 * @param useLeapYears date object will support leap yesrs
 */
/*
Date::Date(uint day, 
           Month month, 
           int year,
           bool createValidDate,
           bool useLeapYears)
  : _d(day)
  , _m(month)
  , _y(year)
  , _useLeapYears(useLeapYears)
{
  _daysInMonth = isLeapYear() && _useLeapYears ? _ldim() : _dim();
  auto dim = daysInMonth(month);
  if(createValidDate)
  {
    if(day == 0)
      _d = 1;
    else if(day > dim)
      _d = dim;
  }
  else if(day > dim || day == 0)
  {
    _d = _m = _y = 0;
  }
}
*/

/*!
 * construct a date from number values
 * @param day
 * @param month
 * @param year
 * @param useLeapYears date object will support leap years
 */
Date::Date(uint8_t day,
           uint8_t month,
           uint16_t year,
           bool isRelativeDate,
           bool createValidDate,
           bool useLeapYears)
    : _d(day), _m(month), _y(year), _useLeapYears(useLeapYears), _isRelativeDate(isRelativeDate) {
  _daysInMonth = isLeapYear() && _useLeapYears ? _ldim() : _dim();
  auto dim = daysInMonth(month);
  if (createValidDate) {
    if (day == 0) _d = 1;
    else if (day > dim) _d = dim;

    if (month == 0) _m = 1;
    else if (month > 12) _m = 12;
  } else if (month > 12 || month == 0 || day > dim || day == 0) {
    _d = _m = 0;
    _y = 0;
  }
}

#ifdef CAPNPROTO_SERIALIZATION_SUPPORT

void Date::deserialize(mas::schema::common::Date::Reader reader) {
  _d = reader.getDay();
  _m = reader.getMonth();
  _y = reader.getYear();
}

void Date::serialize(mas::schema::common::Date::Builder builder) const {
  builder.setDay(_d);
  builder.setMonth(_m);
  builder.setYear(_y);
}

#endif

Date Date::relativeDate(uint8_t day,
                        uint8_t month,
                        uint16_t deltaYears,
                        bool useLeapYears) {
  return {day, month, deltaYears, true, false, useLeapYears};
}

Date Date::fromIsoDateString(const std::string &isoDateString,
                             bool useLeapYears) {
  if (isoDateString.size() == 10) {
    auto year = (uint16_t) stoul(isoDateString.substr(0, 4));
    auto month = (uint8_t) stoul(isoDateString.substr(5, 2));
    auto day = (uint8_t) stoul(isoDateString.substr(8, 2));
    //cout << day << "." << month << "." << year << endl;

    return year < 100
           ? relativeDate(day, month, year, useLeapYears)
           : Date(day, month, year, false, false, useLeapYears);
  }
  return {};
}

Date Date::fromPatternDateString(const std::string &dateString, const std::string &pattern,
                                 bool useLeapYears) {
  std::string year_str;
  std::string month_str;
  std::string day_str;
  bool isDOY = false;
  for (int i = 0; i < pattern.size() && i < dateString.size(); i++){
    switch (pattern[i]) {
      case 'Y':
      case 'y':
        if (isDOY) day_str.push_back(dateString[i]);
        else year_str.push_back(dateString[i]);
        break;
      case 'M':
      case 'm':
        month_str.push_back(dateString[i]);
        break;
      case 'D':
      case 'd':
        day_str.push_back(dateString[i]);
        break;
      case 'O':
      case 'o':
        day_str.push_back(dateString[i]);
        isDOY = true;
        break;
    }
  }

  if (isDOY) {
    auto year = (uint16_t) stoul(year_str);
    auto doy = (uint16_t) stoul(day_str);
    //cout << day << "." << month << "." << year << endl;
    return julianDate(doy, year, year < 100, useLeapYears);
  } else {
    auto year = (uint16_t) stoul(year_str);
    auto month = (uint8_t) stoul(month_str);
    auto day = (uint8_t) stoul(day_str);
    //cout << day << "." << month << "." << year << endl;

    return year < 100
           ? relativeDate(day, month, year, useLeapYears)
           : Date(day, month, year, false, false, useLeapYears);
  }
}

Date::Date(Date &&other) noexcept
: _daysInMonth(other._daysInMonth), _d(other._d), _m(other._m), _y(other._y), _useLeapYears(other._useLeapYears),
      _isRelativeDate(other._isRelativeDate) {
  other._daysInMonth = nullptr;
  other._d = 0;
  other._m = 0;
  other._y = 0;
  other._useLeapYears = DEFAULT_USE_LEAP_YEARS;
  other._isRelativeDate = false;
}


/*!
 * assignment operator, copies the argument
 * @param other the other date
 * @return this, with the copied argument
 */
Date &Date::operator=(const Date &other) {
  if (this == &other) return *this;

  _daysInMonth = other._daysInMonth;
  _d = other.day();
  _m = other.month();
  _y = other.year();
  _useLeapYears = other.useLeapYears();
  _isRelativeDate = other._isRelativeDate;

  return *this;
}

Date &Date::operator=(Date &&other) noexcept {
  if (this == &other) return *this;

  _daysInMonth = other._daysInMonth;
  other._daysInMonth = nullptr;
  _d = other._d;
  other._d = 0;
  _m = other._m;
  other._m = 0;
  _y = other._y;
  other._y = 0;
  _useLeapYears = other._useLeapYears;
  other._useLeapYears = DEFAULT_USE_LEAP_YEARS;
  _isRelativeDate = other._isRelativeDate;
  other._isRelativeDate = false;

  return *this;
}


/*!
 * @param toDate
 * @return number of days to argument date, excluding the argument date
 * (thus 01.01.2000 to 01.01.2000 = 0 days)
 */
int Date::numberOfDaysTo(const Date &toDate) const {
  assert(useLeapYears() == toDate.useLeapYears());
  assert((isRelativeDate() && toDate.isRelativeDate())
         || (!isRelativeDate() && !toDate.isRelativeDate()));

  Date from(*this), to(toDate);
  bool reverse = from > to;
  if (reverse)
    from = toDate, to = *this;

  int nods = 0; //number of days
  if (from.year() == to.year() && from.month() == to.month()) nods += to.day() - from.day();
  else {
    for (int y = from.year(); y <= to.year(); y++) {
      int startMonth = 1, endMonth = 12;
      if (y == from.year()) {
        startMonth = from.month() + 1;
        nods += from.daysInMonth(from.month()) - from.day();
      }
      if (y == to.year()) {
        endMonth = to.month() - 1;
        nods += to.day();
      }
      Date cy(1, 1, y, false, false, useLeapYears()); //current year, needed to count months in leap years correctly
      for (int m = startMonth; m >= 1 && m <= 12 && m <= endMonth; m++) nods += cy.daysInMonth(m);
    }
  }

  return nods * (reverse ? -1 : 1);
}

Date Date::withDay(uint8_t d, bool createValidDate) {
  Date t(*this);
  t.setDay(d, createValidDate);
  return t;
}

void Date::setDay(uint8_t day, bool createValidDate) {
  _d = day;
  if (createValidDate) {
    if (_d == 0) _d = 1;
    else if (_d > daysInMonth()) _d = daysInMonth();
  }
}

Date Date::withMonth(uint8_t m, bool createValidDate) {
  Date t(*this);
  t.setMonth(m, createValidDate);
  return t;
}


void Date::setMonth(uint8_t month, bool createValidDate) {
  _m = month;
  if (createValidDate) {
    if (_m == 0) _m = 1;
    else if (_m > 12) _m = 12;
  }
}


Date Date::withYear(uint16_t y) {
  Date t(*this);
  t.setYear(y);
  return t;
}

/*!
 * compare two dates
 * @param other the other date
 * @return true if 'this' date lies before the argument date
 */
bool Date::operator<(const Date &other) const {
  bool t = day() < other.day();
  //month might be <=
  if (t) {
    t = month() <= other.month();
    if (t) return year() <= other.year(); //year might be <=
    else return year() < other.year(); //year has to be <
  } else { //month has to be <
    t = month() < other.month();
    if (t) return year() <= other.year(); //year might be <=
    else return year() < other.year(); //year has to be <
  }

  return false;
}

Date Date::withAddedYears(int years) const {
  Date d(*this);
  d.setYear(year() + years);
  return d;
}

std::string Date::toIsoDateString(const std::string &wrapInto) const {
  ostringstream s;
  int y = year();// -(isRelativeDate() ? relativeBaseYear() : 0);
  int d = day();
  int m = month();

  s << wrapInto;
  if (isRelativeDate()) {
    if (y < 10) s << "000";
    else if (y < 100) s << "00";
    else if (y < 1000) s << "0";
  }
  s << y
    << "-" << (m < 10 ? "0" : "") << m
    << "-" << (d < 10 ? "0" : "") << d << wrapInto;
  return s.str();
}


/*!
 * @param separator to use for delimit days, month, year
 * @return general string representation
 */
std::string Date::toString(const std::string &separator,
                           bool skipYear) const {
  ostringstream s;
  s << (day() < 10 ? "0" : "") << day()
    << separator << (month() < 10 ? "0" : "") << month();
  if (!skipYear) {
    if (isRelativeDate()) {
      uint16_t deltaYears = year();// -_relativeBaseYear;
      s << separator << "year"
        << (deltaYears > 0 ? "+" : "");
      if (deltaYears != 0) s << deltaYears;
    } else s << separator << year();
  }
  return s.str();
}

/*!
 * @param days
 * @return a copy of 'this' date 'days' before
 */
Date Date::operator-(uint64_t days) const {
  Date cd(*this); //current date
  uint64_t ds = days; //days

  bool isRelativeDate = cd.isRelativeDate();

  while (true) {
    if (cd.day() <= ds) {
      ds -= cd.day();

      if (cd.month() == 1) cd = Date(31, 12, cd.year() - 1, isRelativeDate, false, useLeapYears());
      else
        cd = Date(cd.daysInMonth(cd.month() - 1),
                  cd.month() - 1,
                  cd.year(),
                  isRelativeDate,
                  false,
                  useLeapYears());
    } else {
      assert(ds <= 31);
      cd.setDay(cd.day() - (uint8_t) ds);
      break;
    }
  }

  return cd;
}

/*!
 * postfix --
 * decrement 'this' date by one day
 * @param dummy to distiguish prefix from postfix
 * @return the old 'this' before decrementation
 */
Date Date::operator--(int) {
  Date d = *this;
  operator-=(1);
  return d;
}

/*!
 * @param days
 * @return date 'days' ahead of 'this' date
 */
Date Date::operator+(uint64_t days) const {
  Date cd(*this); //current date
  uint64_t ds = days; //days

  bool isRelativeDate = cd.isRelativeDate();
  //uint relativeBaseYear = cd.relativeBaseYear();

  while (true) {
    uint8_t delta = cd.daysInMonth(cd.month()) - cd.day() + 1;
    if (delta <= ds) {
      ds -= delta;

      if (cd.month() == 12) cd = Date(1, 1, cd.year() + 1, isRelativeDate, false, useLeapYears());
      else cd = Date(1, cd.month() + 1, cd.year(), isRelativeDate, false, useLeapYears());
    } else {
      assert(ds <= 31);
      cd.setDay(cd.day() + (uint8_t) ds);
      break;
    }
  }

  return cd;
}

/*!
 * postfix ++
 * increment 'this' date by one day
 * @param dummy to distinguish prefix from postfix
 * @return old 'this' before incrementation
 */
Date Date::operator++(int) {
  Date d = *this;
  operator+=(1);
  return d;
}

bool Date::isLeapYear() const {
  return _y % 4 == 0 && (_y % 100 != 0 || _y % 400 == 0);
}

Date Date::toAbsoluteDate(uint16_t absYear, bool ignoreDeltaYears) const {
  return Date(day(), month(), ignoreDeltaYears ? absYear : absYear + year(),
              false, useLeapYears());
}

DayLengths Tools::dayLengths(double latitude, double julianDay) {
  DayLengths dls;

  // Calculation of declination - old DEC
  double declination = -23.4 * cos(2.0 * M_PI * ((julianDay + 10.0) / 365.0));

  //old SINLD
  double declSin = sin(declination * M_PI / 180.0) * sin(latitude * M_PI / 180.0);
  //old COSLD
  double declCos = cos(declination * M_PI / 180.0) * cos(latitude * M_PI / 180.0);

  // Calculation of the atmospheric day lenght -> old DL
  double astroDayLength = declSin / declCos;
  astroDayLength = bound(-1.0, astroDayLength, 1.0); // The argument of asin must be in the range of -1 to 1
  dls.astronomicDayLength = 12.0 * (M_PI + 2.0 * asin(astroDayLength)) / M_PI;

  // Calculation of the effective day length = old DLE
  double edlHelper = (-sin(8.0 * M_PI / 180.0) + declSin) / declCos;

  if ((edlHelper < -1.0) || (edlHelper > 1.0)) {
    dls.effectiveDayLength = 0.01;
  } else {
    dls.effectiveDayLength = 12.0 * (M_PI + 2.0 * asin(edlHelper)) / M_PI;
  }
  /*edlHelper = bound(-1.0, edlHelper, 1.0);
  dls.effectiveDayLength = 12.0 * (M_PI + 2.0 * asin(edlHelper)) / PI;*/

  // old DLP
  double photoDayLength = (-sin(-6.0 * M_PI / 180.0) + declSin) / declCos;
  photoDayLength = bound(-1.0, photoDayLength, 1.0); // The argument of asin must be in the range of -1 to 1
  dls.photoperiodicDaylength = 12.0 * (M_PI + 2.0 * asin(photoDayLength)) / M_PI;

  return dls;
}

//! function testing the date class
void Tools::testDate() {
  assert(Date(1, 1, 2001).numberOfDaysTo(Date(2, 1, 2001)) == 1);
  assert(Date(1, 1, 2001).numberOfDaysTo(Date(1, 1, 2001)) == 0);
  assert(Date(1, 1, 2001).numberOfDaysTo(Date(1, 2, 2001)) == 31);

  assert(Date(1, 1, 2001) == Date(1, 1, 2001));
  assert(Date(1, 1, 2001) > Date(31, 12, 2000));
  assert(Date(1, 1, 2001) < Date(2, 1, 2001));
  assert(Date(1, 1, 2001) >= Date(1, 1, 2001));
  assert(Date(1, 1, 2001) >= Date(31, 12, 2000));
  assert(Date(1, 1, 2001) <= Date(1, 1, 2001));
  assert(Date(1, 1, 2001) <= Date(2, 1, 2001));

  Date t(5, 3, 200, true);
  assert(t - 5 == Date(29, 2, 2008));
  assert(t - 10 == Date(24, 2, 2008));
  assert(t - 20 == Date(14, 2, 2008));
  assert(t - 30 == Date(4, 2, 2008));
  assert(t - 100 == Date(26, 11, 2007));
  assert(t - 300 == Date(10, 5, 2007));
  assert(t - 400 == Date(30, 1, 2007));
  assert(t - 1000 == Date(9, 6, 2005));

  t = Date(25, 2, 2008);
  assert(t + 5 == Date(1, 3, 2008));
  assert(t + 10 == Date(6, 3, 2008));
  assert(t + 20 == Date(16, 3, 2008));
  assert(t + 30 == Date(26, 3, 2008));
  assert(t + 100 == Date(4, 6, 2008));
  assert(t + 300 == Date(21, 12, 2008));
  assert(t + 400 == Date(31, 3, 2009));
  assert(t + 1000 == Date(21, 11, 2010));
}
