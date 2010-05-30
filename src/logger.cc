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

//==============================================================================
// Author(s): Roman Khmelichek
//
// Multiple Logger objects used by multiple threads logging to the same file
// descriptor are allowed; they will not cause race conditions, since each
// unique file descriptor maps to the same mutex lock. However, there should be
// no race conditions when initializing Logger objects!
//==============================================================================

#include "logger.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>

#include <unistd.h>
using namespace std;

namespace logger {

vector<pair<int, pthread_mutex_t*> > Logger::fd_lock_mapping_;

Logger::Logger(int fd, bool debug, void(*cleanup_handler)(void)) :
  kDebug(debug), fd_(fd), cleanup_handler_(cleanup_handler) {
  pthread_mutex_t* logger_lock = FindFdLock(fd_);
  if (logger_lock == NULL) {
    pthread_mutex_t* logger_lock = new pthread_mutex_t;
    pthread_mutex_init(logger_lock, NULL);
    Logger::fd_lock_mapping_.push_back(make_pair(fd_, logger_lock));
  }
}

Logger::~Logger() {
  vector<std::pair<int, pthread_mutex_t*> >& fd_lock_mapping =
      Logger::fd_lock_mapping_;
  for (size_t i = 0; i < fd_lock_mapping.size(); ++i) {
    pthread_mutex_destroy(fd_lock_mapping[i].second);
    delete fd_lock_mapping[i].second;
  }
}

void Logger::Log(const string& message, bool fatal_error) {
  pthread_mutex_t* logger_lock = FindFdLock(fd_);
  string timestamp = GetTimestamp(); //thread safe
  if (logger_lock != NULL) {
    pthread_mutex_lock(logger_lock);

    string write_message = timestamp + message + "\n";
    if (write(fd_, write_message.c_str(), write_message.length()) == -1) {
      string err_message = "Logger could not write() to fd " + Stringify(fd_);
      perror(err_message.c_str());
    }

    if (fatal_error) {
      const char kExitMessage[] = "Exiting.\n";
      if (write(fd_, kExitMessage, strlen(kExitMessage)) == -1) {
        string err_message = "Logger could not write() to fd "
            + Stringify(fd_);
        perror(err_message.c_str());
      }

      if (cleanup_handler_ != NULL)
        cleanup_handler_();

      // TODO: Maybe cleanup handler should be the one calling exit.
      // Also, might want to unlock the mutex right after we do the last write to the stream.
      exit(1);
    }
    pthread_mutex_unlock(logger_lock);
  } else {
    assert(false);
  }
}

void Logger::LogErrno(const string& message, int err_no, bool fatal_error) {
  const size_t kErrBufLen = 256;
  char errbuf[kErrBufLen];
  char* errbuf_alias = errbuf;

  // TODO: Version of strerror_r here is the GNU specific version. For some reason, the XSI standard version is not seen by the compiler.
  if ((errbuf_alias = strerror_r(err_no, errbuf_alias, kErrBufLen)) == NULL) {
    perror("Logger could not do strerror_r()");
    errbuf[0] = '\0';
  }

  string whole_message = message + ": " + string(errbuf_alias);
  Log(whole_message, fatal_error);
}

void Logger::DebugLog(const string& message) {
  if (kDebug)
    Log(message, false);
}

string Logger::GetTimestamp() {
  const char kTimeFormat[] = "[%Y-%m-%d %H:%M:%S] ";
  const int kTimeSize = 23;  // Custom fit for our time format.

  time_t rawtime;
  struct tm timestamp;
  char timestr[kTimeSize];

  time(&rawtime);
  if (!localtime_r(&rawtime, &timestamp)) {
    perror("Logger could not do localtime()");
  }
  strftime(timestr, kTimeSize, kTimeFormat, &timestamp);
  return timestr;
}

pthread_mutex_t* Logger::FindFdLock(int fd) {
  vector<std::pair<int, pthread_mutex_t*> >& fd_lock_mapping =
      Logger::fd_lock_mapping_;
  for (size_t i = 0; i < fd_lock_mapping.size(); ++i) {
    if (fd == fd_lock_mapping[i].first)
      return fd_lock_mapping[i].second;
  }
  return NULL;
}

} // namespace logger
