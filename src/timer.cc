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
//==============================================================================================================================================================

#include "timer.h"
using namespace std;

Timer::Timer() {
  start_time_std_ = clock();
  gettimeofday(&start_time_sys_, NULL);
}

double Timer::GetElapsedTime() {
  return GetElapsedTimeSys();
}

double Timer::GetElapsedTimeStd() {
  clock_t stop_time_std;
  stop_time_std = clock();
  double time_diff = static_cast<double> (stop_time_std - start_time_std_) / CLOCKS_PER_SEC;
  return time_diff;
}

double Timer::GetElapsedTimeSys() {
  timeval stop_time_sys, time_diff_sys;
  gettimeofday(&stop_time_sys, NULL);
  timersub(&stop_time_sys, &start_time_sys_, &time_diff_sys);
  double time_diff = time_diff_sys.tv_sec + time_diff_sys.tv_usec / 1000000.0;  // 10^6 usec per second.
  return time_diff;
}
