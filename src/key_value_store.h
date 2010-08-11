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
// A class for reading and writing key/value pairs from files or just keeping them around in memory.
// The format of a key/value store on disk is simply:
// key = value
// where the key and value must be a single token not separated by whitespace. There must be one key/value pair per line. There can also optionally be comments,
// denoted by a pound character (#). Anything after the start of a comment is not considered part of the key/value pair.
// The keys must be unique. If there are pairs with duplicate keys, only the first pair will ever be found.
// The search time to find a value is proportional to the number of key/value pairs.
//==============================================================================================================================================================

#ifndef KEY_VALUE_STORE_H_
#define KEY_VALUE_STORE_H_

#include <string>
#include <utility>
#include <vector>

class KeyValueStore {
public:
  typedef std::pair<std::string, std::string> KeyValuePair;

  class Status {
  public:
    enum StatusCode {
      kOk, kBadFileRead, kBadFileWrite, kNoEquals, kNoKey, kNoValue
    };

    Status(StatusCode status_code, int line_num) :
      status_code_(status_code), line_num_(line_num) {
    }

    const char* GetStatusMessage() const {
      static const char* kStatusMessages[] = { "OK", "Could not open key value store file for reading", "Could not open key value store file for writing",
                                               "Did not find '=' in key value pair line", "No key found", "No value found" };
      return kStatusMessages[status_code_];
    }

    StatusCode status_code() const {
      return status_code_;
    }

    int line_num() const {
      return line_num_;
    }

  private:
    StatusCode status_code_;
    int line_num_;
  };

  template<class ValueT>
    class KeyValueResult {
    public:
      enum StatusCode {
        kOk, kNoKey, kNoValue, kImproperBooleanValue, kNumericalValueOutOfRange, kFloatingValueOutOfRange
      };

      KeyValueResult(StatusCode status_code, const std::string& filename, const std::string& key, const std::string& value, ValueT value_t) :
        status_code_(status_code), filename_(filename), key_(key), value_(value), value_t_(value_t) {
      }

      const char* GetStatusMessage() const {
        static const char* kStatusMessages[] = { "OK", "Key does not exist", "Value not specified", "Boolean value may only be one of 'true' or 'false'",
                                                 "Numerical value is out of range", "Floating value is out of range" };
        return kStatusMessages[status_code_];
      }

      std::string GetErrorMessage() const {
        return "Error trying to get key '" + key_ + "' with value '" + value_ + "': " + std::string(GetStatusMessage());
      }

      bool error() const {
        if (status_code_ != kOk)
          return true;

        return false;
      }

      StatusCode status_code() const {
        return status_code_;
      }

      const std::string& filename() const {
        return filename_;
      }

      const std::string& key() const {
        return key_;
      }

      const std::string& value() const {
        return value_;
      }

      const ValueT& value_t() const {
        return value_t_;
      }

    private:
      StatusCode status_code_;
      std::string filename_;
      std::string key_;
      std::string value_;
      ValueT value_t_;
    };

  std::string GetValue(const std::string& key) const;
  KeyValuePair GetKeyValuePair(const std::string& key) const;
  KeyValueResult<std::string> GetStringValue(const std::string& key) const;
  KeyValueResult<bool> GetBooleanValue(const std::string& key) const;
  KeyValueResult<long int> GetNumericalValue(const std::string& key) const;
  KeyValueResult<double> GetFloatingValue(const std::string& key) const;
  void AddKeyValuePair(const std::string& key, const std::string& value);
  Status WriteKeyValueStore(const char* filename) const;
  Status LoadKeyValueStore(const char* filename);

private:
  std::vector<KeyValuePair> key_value_store_;
  std::string loaded_filename_;
};

#endif /* KEY_VALUE_STORE_H_ */
