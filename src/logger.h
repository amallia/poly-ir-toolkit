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
// Class used for logging. Thread safe through pthread mutexes. Logger objects can be created in debug mode; in this case DebugLog() can be used to output
// messages, but these calls should be optimized away when a Logger is not created in debug mode.
//==============================================================================================================================================================

#ifndef LOGGER_H_
#define LOGGER_H_

#include <string>
#include <utility>
#include <vector>

#include <pthread.h>

namespace logger {

class Logger {
public:
  // Parameters are: valid file descriptor to log to, whether debug mode is
  // activated, and an optional cleanup function to call before exiting, which
  // could be NULL.
  Logger(int fd, bool debug, void(*cleanup_handler)(void));

  // Destroys the allocated mutexes.
  ~Logger();

  // Appends a timestamp to a string message and outputs it to a file
  // descriptor specified during the creation of this Logger.
  // Messages can be marked as fatal, which causes the program to call the
  // cleanup handler specified during construction, and then to exit.
  void Log(const std::string& message, bool fatal_error);

  // Same as Log(), but the message is 'message' followed by a colon and space,
  // the errno description and finally the newline character.
  void LogErrno(const std::string& message, int err_no, bool fatal_error);

  // Logs message when Logger is in debug mode. Logger can be put into debug
  // mode only at compile time, so these calls can be optimized away when not
  // doing debugging.
  void DebugLog(const std::string& message);

  // Returns a nicely formatted timestamp.
  std::string GetTimestamp();

private:
  pthread_mutex_t* FindFdLock(int fd);

  const bool kDebug;
  int fd_;
  void (*cleanup_handler_)(void);
  static std::vector<std::pair<int, pthread_mutex_t*> > fd_lock_mapping_;
};

} // namespace logger

#endif /* LOGGER_H_ */
