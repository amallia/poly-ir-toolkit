// Copyright (c) 2010, Roman Khmelichek
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//  1. Redistributions of source code must retain the above copyright notice,
//     this list of conditions and the following disclaimer.
//  2. Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//  3. Neither the name of Roman Khmelichek nor the names of its contributors
//     may be used to endorse or promote products derived from this software
//     without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE AUTHOR "AS IS" AND ANY EXPRESS OR IMPLIED
// WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
// EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
// OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
// OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
// ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

//==============================================================================================================================================================
// Author(s): Roman Khmelichek
//
// The key/value store is simply implemented as a vector.
//==============================================================================================================================================================

#include "key_value_store.h"

#include <cctype>
#include <climits>
#include <cmath>
#include <cstdlib>

#include <fstream>
using namespace std;

string KeyValueStore::GetValue(const string& key) const {
  return GetKeyValuePair(key).second;
}

KeyValueStore::KeyValuePair KeyValueStore::GetKeyValuePair(const string& key) const {
  for (vector<KeyValuePair>::const_iterator itr = key_value_store_.begin(); itr != key_value_store_.end(); ++itr) {
    if (key == itr->first) {
      return *itr;
    }
  }

  return KeyValuePair();
}

KeyValueStore::KeyValueResult<string> KeyValueStore::GetStringValue(const string& key) const {
  KeyValueResult<string>::StatusCode status_code = KeyValueResult<string>::kOk;
  KeyValuePair key_value_pair = GetKeyValuePair(key);

  if (!key_value_pair.first.empty()) {
    if (key_value_pair.second.empty()) {
      status_code = KeyValueResult<string>::kNoValue;
    }
  } else {
    status_code = KeyValueResult<string>::kNoKey;
  }

  return KeyValueResult<string> (status_code, loaded_filename_, key_value_pair.first, key_value_pair.second, key_value_pair.second);
}

KeyValueStore::KeyValueResult<bool> KeyValueStore::GetBooleanValue(const string& key) const {
  KeyValueResult<bool>::StatusCode status_code = KeyValueResult<bool>::kOk;
  KeyValuePair key_value_pair = GetKeyValuePair(key);
  bool value = true;

  if (!key_value_pair.first.empty()) {
    if (!key_value_pair.second.empty()) {
      if (key_value_pair.second == "true") {
        value = true;
      } else if (key_value_pair.second == "false") {
        value = false;
      } else {
        status_code = KeyValueResult<bool>::kImproperBooleanValue;
      }
    } else {
      status_code = KeyValueResult<bool>::kNoValue;
    }
  } else {
    status_code = KeyValueResult<bool>::kNoKey;
  }

  return KeyValueResult<bool> (status_code, loaded_filename_, key_value_pair.first, key_value_pair.second, value);
}

KeyValueStore::KeyValueResult<long int> KeyValueStore::GetNumericalValue(const string& key) const {
  KeyValueResult<long int>::StatusCode status_code = KeyValueResult<long int>::kOk;
  KeyValuePair key_value_pair = GetKeyValuePair(key);
  long int value = 0;

  if (!key_value_pair.first.empty()) {
    if (!key_value_pair.second.empty()) {
      value = atol(key_value_pair.second.c_str());
      if (value == LONG_MAX || value == LONG_MIN) {
        status_code = KeyValueResult<long int>::kNumericalValueOutOfRange;
      }
    } else {
      status_code = KeyValueResult<long int>::kNoValue;
    }
  } else {
    status_code = KeyValueResult<long int>::kNoKey;
  }

  return KeyValueResult<long int> (status_code, loaded_filename_, key_value_pair.first, key_value_pair.second, value);
}

KeyValueStore::KeyValueResult<double> KeyValueStore::GetFloatingValue(const string& key) const {
  KeyValueResult<double>::StatusCode status_code = KeyValueResult<double>::kOk;
  KeyValuePair key_value_pair = GetKeyValuePair(key);
  double value = 0.0;

  if (!key_value_pair.first.empty()) {
    if (!key_value_pair.second.empty()) {
      value = atof(key_value_pair.second.c_str());
      if (value == HUGE_VAL || value == -HUGE_VAL) {
        status_code = KeyValueResult<double>::kFloatingValueOutOfRange;
      }
    } else {
      status_code = KeyValueResult<double>::kNoValue;
    }
  } else {
    status_code = KeyValueResult<double>::kNoKey;
  }

  return KeyValueResult<double> (status_code, loaded_filename_, key_value_pair.first, key_value_pair.second, value);
}

void KeyValueStore::AddKeyValuePair(const string& key, const string& value) {
  key_value_store_.push_back(make_pair(key, value));
}

KeyValueStore::Status KeyValueStore::LoadKeyValueStore(const char* filename) {
  loaded_filename_ = string(filename);

  ifstream key_value_stream(filename);
  if (!key_value_stream) {
    return Status(Status::kBadFileRead, 0);
  }

  string curr_key_value_str;
  int curr_line_num = 1;
  while (getline(key_value_stream, curr_key_value_str)) {
    string::iterator itr = curr_key_value_str.begin();

    // Skip any whitespace prior to the key.
    while (itr != curr_key_value_str.end() && isspace(*itr)) {
      ++itr;
    }

    // There is nothing on this line, except possibly a comment.
    if (itr == curr_key_value_str.end() || *itr == '#') {
      ++curr_line_num;
      continue;
    }

    // First non whitespace and non comment character indicates start of the key.
    string::iterator key_start = itr;

    // Everything that is non whitespace, not an equals character, and not a comment character is considered to be part of the key.
    while (itr != curr_key_value_str.end() && isspace(*itr) == false && *itr != '=' && *itr != '#') {
      ++itr;
    }

    if (key_start == itr) {
      return Status(Status::kNoKey, curr_line_num);
    } else if (*itr == '#') {
      return Status(Status::kNoValue, curr_line_num);
    }

    // We have the key.
    string key = string(key_start, itr);

    // Skip any whitespace after the key.
    while (itr != curr_key_value_str.end() && isspace(*itr)) {
      ++itr;
    }

    // Should have an equals character here.
    if (*itr != '=') {
      return Status(Status::kNoEquals, curr_line_num);
    } else {
      ++itr;
    }

    // Skip any whitespace prior to the value.
    while (itr != curr_key_value_str.end() && isspace(*itr)) {
      ++itr;
    }

    if (*itr == '#') {
      return Status(Status::kNoValue, curr_line_num);
    }

    // First non whitespace character indicates start of the value.
    string::iterator value_start = itr;

    // Everything that is non whitespace and not a comment character is considered to be part of the value.
    while (itr != curr_key_value_str.end() && isspace(*itr) == false && *itr != '#') {
      ++itr;
    }

    if (value_start == itr) {
      return Status(Status::kNoValue, curr_line_num);
    }

    // We have the value.
    string value = string(value_start, itr);

    key_value_store_.push_back(make_pair(key, value));
    ++curr_line_num;
  }

  key_value_stream.close();
  return Status(Status::kOk, 0);
}

KeyValueStore::Status KeyValueStore::WriteKeyValueStore(const char* filename) const {
  ofstream key_value_stream(filename);
  if (!key_value_stream) {
    return Status(Status::kBadFileWrite, 0);
  }

  for (vector<KeyValuePair>::const_iterator itr = key_value_store_.begin(); itr != key_value_store_.end(); ++itr) {
    key_value_stream << itr->first << " = " << itr->second << "\n";
  }

  key_value_stream.close();
  return Status(Status::kOk, 0);
}
