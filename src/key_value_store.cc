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

#include <fstream>
#include <iostream>
using namespace std;

string KeyValueStore::GetValue(const std::string& key) const {
  for (vector<KeyValuePair>::const_iterator itr = key_value_store_.begin(); itr != key_value_store_.end(); ++itr) {
    if (key == itr->first)
      return itr->second;
  }
  return string();
}

void KeyValueStore::AddKeyValuePair(const std::string& key, const std::string& value) {
  key_value_store_.push_back(make_pair(key, value));
}

KeyValueStore::Status KeyValueStore::LoadKeyValueStore(const char* filename) {
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
