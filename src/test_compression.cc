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
// This explains the maximum compressed encoding of a 32-bit unsigned integer under each of the compression methods in the worst case. This is useful for
// allocating the correct amount of space for the compressed array. Assume that 'block_size' is a multiple of 32. Also assume that a word is 32 bits.
//
// Rice Coding and Turbo Rice Coding:
// Consider encoding (2^32 - 1). The binary remainder consists of the 31 least significant bits, followed by the unary portion which consists of '10'.
// So the maximum is 33 bits per integer. There is also a metadata header which takes up one word. So, the maximum compressed size of a block of integers is
// (('block_size' + 'block_size' / 32) + 1) words.
//
// PForDelta Coding:
// Consider encoding block of (2^32 - 1). Each one will be coded fully, so it will take 32 bits each. The max compressed block size is then ('block_size' + 1)
// words, including one word for the metadata header.
//
// S9 and S16 Coding:
// In the worst case, it will use one word to store one integer to be compressed. So, the max compressed block size is then just ('block_size'). There is also a
// limitation on the maximum integer that can be compressed with this method, which is (2**28 - 1), since each encoded word has a 4-bit metadata header.
// Important note: For S9 and S16, in this implementation, the output array provided for decompression should be an upper bound of a multiple of at least 28,
// of the total number of integers expected to be decompressed; this is because the last compressed word could be coded such that it has 28 integers, but not
// all of them are actually valid integers (there could be less than 28), so the output array buffer will still be accessed and those invalid integers decoded,
// so the output array could be accessed at most 28 integers ahead of the total number of integers expected to be decompressed.
//
// Variable Byte Coding:
// This implementation uses a maximum of 5 bytes to encode a single integer. It therefore has a limitation on the maximum integer that can be encoded, which is
// ((128**4) + (128**3) + (128**2) + (128**1)). In the worst case, encoding 'block_size' integers will result in a maximum compressed block size of
// ceil((5 * 'block_size') / 4) words, since there are 5 bytes per compressed integer and 4 bytes per word.
//
// Null Coding:
// Does not perform any compression, just copies the input to the output. So, the compressed output is simply 'block_size' words.
//==============================================================================================================================================================

#include "test_compression.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <stdint.h>

#include <algorithm>
#include <utility>
#include <iostream>

#include <strings.h>
#include <sys/time.h>

#include "compression_toolkit/pfor_coding.h"
#include "compression_toolkit/rice_coding.h"
#include "compression_toolkit/rice_coding2.h"
#include "compression_toolkit/s9_coding.h"
#include "compression_toolkit/s16_coding.h"
#include "compression_toolkit/vbyte_coding.h"
#include "compression_toolkit/null_coding.h"
using namespace std;

// Print unsigned integer in binary format with spaces separating each byte.
void PrintBinary(unsigned int num) {
  int arr[32];
  int i = 0;
  while (i++ < 32 || num / 2 != 0) {
    arr[i - 1] = num % 2;
    num /= 2;
  }

  for (int i = 31; i >= 0; i--) {
    printf("%d", arr[i]);
    if (i % 8 == 0)
      printf(" ");
  }
}

bool CodingBench(coding* coder, unsigned* compressor_in, int block_size, int num_blocks, int random_num_range, double* compressed_words,
                 double* time_elapsed_seconds) {
  coder->set_size(block_size);

  int max_compressed_size = 2 * block_size;  // Per block.
  unsigned* compressor_out = new unsigned[max_compressed_size * num_blocks];

  unsigned curr_compressed_offset = 0;
  unsigned curr_decompressed_offset = 0;
  unsigned total_compressed_words = 0;
  for (int i = 0; i < num_blocks; ++i) {
    total_compressed_words += coder->Compression(compressor_in + curr_decompressed_offset, compressor_out + curr_compressed_offset, block_size);
    curr_compressed_offset += max_compressed_size;
    curr_decompressed_offset += block_size;
  }

  // The '+ 1' in '(num_blocks + 1)' is necessary for S9/S16, which might decode 28 integers in the last compressed word
  // (but that compressed word actually corresponds to less than 28 integers), which could result in a seg fault if we don't allocate
  // a little (at least 28 words) extra space to the decompressed array.
  unsigned* decompressor_out = new unsigned[block_size * (num_blocks + 1)];

  *compressed_words = total_compressed_words;

  // We time this portion.
  struct timeval start_time;
  struct timeval end_time;

  gettimeofday(&start_time, NULL);
  curr_compressed_offset = 0;
  curr_decompressed_offset = 0;
  for (int i = 0; i < num_blocks; ++i) {
    coder->Decompression(compressor_out + curr_compressed_offset, decompressor_out + curr_decompressed_offset, block_size);
    curr_compressed_offset += max_compressed_size;
    curr_decompressed_offset += block_size;
  }
  gettimeofday(&end_time, NULL);

  struct timeval res;
  timersub(&end_time, &start_time, &res);
  *time_elapsed_seconds = (res.tv_sec + res.tv_usec / 1000000.0);  // 10^6 usec per second.

  bool status = true;
  // Verify that we can compress and decompress correctly.
  for (int i = 0; i < block_size * num_blocks; ++i) {
    if (compressor_in[i] != decompressor_out[i]) {
      status = false;
      break;
    }
  }

  delete[] compressor_out;
  delete[] decompressor_out;
  return status;
}

bool RunBench(coding* coder, const char* coder_str, unsigned* compressor_in, int block_size, int num_blocks, int random_num_range, double* compressed_words,
              double* time_elapsed_seconds) {
  bool status = CodingBench(coder, compressor_in, block_size, num_blocks, random_num_range, compressed_words, time_elapsed_seconds);
  if (!status) {
    printf("Test Failed (%s): block_size %d\n", coder_str, block_size);
  }
  return status;
}

void PrintStats(const char* coder_str, double time_elapsed_seconds, double compressed_words, int num_integers) {
  printf("%s: %f million integers per second (higher is better).\n", coder_str, ((num_integers / time_elapsed_seconds) / 1000000.0));
  printf("%s: %f average compression ratio (lower is better).\n", coder_str, (compressed_words / num_integers));
}

// TODO: Would be interesting to time compression and see how many million integers per second we can compress under each type of coder.
void TestCompression() {
  printf("Testing compression / decompression codes...This might take a while...\n");

  // Seed random number generator.
  srand(time(NULL));

  rice_coding rice_coder;
  rice_coding2 turbo_rice_coder;
  pfor_coding pfor_coder;  // Valid block sizes for PForDelta are 32, 64, 128, or 256.
  s9_coding s9_coder;
  s16_coding s16_coder;
  vbyte_coding vbyte_coder;
  null_coding null_coder;

  double rice_time_elapsed_seconds = 0, rice_compressed_words = 0;
  double turbo_rice_time_elapsed_seconds = 0, turbo_rice_compressed_words = 0;
  double pfor_time_elapsed_seconds = 0, pfor_compressed_words = 0;
  double s9_time_elapsed_seconds = 0, s9_compressed_words = 0;
  double s16_time_elapsed_seconds = 0, s16_compressed_words = 0;
  double vbyte_time_elapsed_seconds = 0, vbyte_compressed_words = 0;
  double null_time_elapsed_seconds = 0, null_compressed_words = 0;

  double num_integers_processed = 0;  // Keeps a count of the total number of integers compressed/decompressed.

  const int kMaxRandomNumRange = min(270549120, 268435455);  // Max allowed integer for varbyte implementation and for S9/16 implementation (take the min of all limitations).
  int random_num_range = 1;

  int kMaxBlockSize = 256;  // Maximum block size to use (limited by PForDelta).
  int kMaxNumBlocks = 32;  // Number of times we'll repeatedly encode/decode the current block size.
  int kNumTimes = 50;  // Number of times we'll repeat, this will use a different random number range each time.

  for (int num_times = 0; num_times < kNumTimes; ++num_times) {
    for (int block_size = 32; block_size <= kMaxBlockSize; block_size *= 2) {
      for (int num_blocks = 1; num_blocks <= kMaxNumBlocks; ++num_blocks) {
        unsigned* compressor_in = new unsigned[block_size * num_blocks];
        cout << "random_num_range: " << random_num_range << endl;
        for (int i = 0; i < (block_size * num_blocks); ++i) {
          compressor_in[i] = rand() % random_num_range;
        }

        printf("'block_size': %d, 'num_blocks': %d, 'random_num_range': %d\n", block_size, num_blocks, random_num_range);
        for (int curr_block_num = 0; curr_block_num < num_blocks; ++curr_block_num) {
          printf("Compressing the following array (read left to right):\n");
          for (int i = 0; i < block_size; i += 4) {
            printf("%10u, %10u, %10u, %10u", compressor_in[i], compressor_in[i + 1], compressor_in[i + 2], compressor_in[i + 3]);
            if (i < block_size - 4)
              printf(",");
            printf("\n");
          }
        }

        double time_elapsed_seconds;  // Time taken to decompress the 'num_blocks' * 'block_size' integers.
        double compressed_words;  // compressed size / uncompressed size.
        bool run_bench_status;

        run_bench_status = RunBench(&rice_coder, "Rice", compressor_in, block_size, num_blocks, random_num_range, &compressed_words, &time_elapsed_seconds);
        if (run_bench_status) {
          rice_time_elapsed_seconds += time_elapsed_seconds;
          rice_compressed_words += compressed_words;
        } else {
          PrintStats("Rice", rice_time_elapsed_seconds, rice_compressed_words, num_integers_processed);
          exit(1);
        }

        run_bench_status = RunBench(&turbo_rice_coder, "Turbo-Rice", compressor_in, block_size, num_blocks, random_num_range, &compressed_words,
                                    &time_elapsed_seconds);
        if (run_bench_status) {
          turbo_rice_time_elapsed_seconds += time_elapsed_seconds;
          turbo_rice_compressed_words += compressed_words;
        } else {
          PrintStats("Turbo-Rice", turbo_rice_time_elapsed_seconds, turbo_rice_compressed_words, num_integers_processed);
          exit(1);
        }

        run_bench_status = RunBench(&pfor_coder, "PForDelta", compressor_in, block_size, num_blocks, random_num_range, &compressed_words,
                                    &time_elapsed_seconds);
        if (run_bench_status) {
          pfor_time_elapsed_seconds += time_elapsed_seconds;
          pfor_compressed_words += compressed_words;
        } else {
          PrintStats("PForDelta", pfor_time_elapsed_seconds, pfor_compressed_words, num_integers_processed);
          exit(1);
        }

        run_bench_status = RunBench(&s9_coder, "S9", compressor_in, block_size, num_blocks, random_num_range, &compressed_words, &time_elapsed_seconds);
        if (run_bench_status) {
          s9_time_elapsed_seconds += time_elapsed_seconds;
          s9_compressed_words += compressed_words;
        } else {
          PrintStats("S9", s9_time_elapsed_seconds, s9_compressed_words, num_integers_processed);
          exit(1);
        }

        run_bench_status = RunBench(&s16_coder, "S16", compressor_in, block_size, num_blocks, random_num_range, &compressed_words, &time_elapsed_seconds);
        if (run_bench_status) {
          s16_time_elapsed_seconds += time_elapsed_seconds;
          s16_compressed_words += compressed_words;
        } else {
          PrintStats("S16", s16_time_elapsed_seconds, s16_compressed_words, num_integers_processed);
          exit(1);
        }

        run_bench_status = RunBench(&vbyte_coder, "VarByte", compressor_in, block_size, num_blocks, random_num_range, &compressed_words, &time_elapsed_seconds);
        if (run_bench_status) {
          vbyte_time_elapsed_seconds += time_elapsed_seconds;
          vbyte_compressed_words += compressed_words;
        } else {
          PrintStats("VarByte", vbyte_time_elapsed_seconds, vbyte_compressed_words, num_integers_processed);
          exit(1);
        }

        run_bench_status = RunBench(&null_coder, "Null", compressor_in, block_size, num_blocks, random_num_range, &compressed_words, &time_elapsed_seconds);
        if (run_bench_status) {
          null_time_elapsed_seconds += time_elapsed_seconds;
          null_compressed_words += compressed_words;
        } else {
          PrintStats("Null", null_time_elapsed_seconds, null_compressed_words, num_integers_processed);
          exit(1);
        }

        num_integers_processed += block_size * num_blocks;

        delete[] compressor_in;
      }
    }

    if (num_times < (kNumTimes / 2)) {
      random_num_range += 128;
      if (random_num_range > kMaxRandomNumRange) {
        random_num_range = kMaxRandomNumRange;
      }
    } else {
      random_num_range = rand() % kMaxRandomNumRange;
    }
  }

  printf("\nAll tests passed successfully. Statistics:\n");
  PrintStats("Rice", rice_time_elapsed_seconds, rice_compressed_words, num_integers_processed);
  printf("\n");
  PrintStats("Turbo-Rice", turbo_rice_time_elapsed_seconds, turbo_rice_compressed_words, num_integers_processed);
  printf("\n");
  PrintStats("PForDelta", pfor_time_elapsed_seconds, pfor_compressed_words, num_integers_processed);
  printf("\n");
  PrintStats("S9", s9_time_elapsed_seconds, s9_compressed_words, num_integers_processed);
  printf("\n");
  PrintStats("S16", s16_time_elapsed_seconds, s16_compressed_words, num_integers_processed);
  printf("\n");
  PrintStats("VarByte", vbyte_time_elapsed_seconds, vbyte_compressed_words, num_integers_processed);
  printf("\n");
  PrintStats("Null", null_time_elapsed_seconds, null_compressed_words, num_integers_processed);
}

// Outputs results of a compression coding for hand analysis.
void TestCoding(coding* coder) {
  const int kSize = 32;
  assert(kSize % 32 == 0);
  coder->set_size(kSize);

//  unsigned int in[kSize] = {4294967295, 4294967295, 4294967295, 4294967295,
//                            4294967295, 4294967295, 4294967295, 4294967295,
//                            4294967295, 4294967295, 4294967295, 4294967295,
//                            4294967295, 4294967295, 4294967295, 4294967295,
//                            4294967295, 4294967295, 4294967295, 4294967295,
//                            4294967295, 4294967295, 4294967295, 4294967295,
//                            4294967295, 4294967295, 4294967295, 4294967295,
//                            4294967295, 4294967295, 4294967295, 4294967295};

  unsigned int in[kSize] = {1, 0, 1, 0,
                            1, 0, 1, 0,
                            1, 0, 1, 0,
                            1, 0, 1, 0,
                            1, 0, 1, 0,
                            1, 0, 1, 0,
                            1, 0, 1, 0,
                            1, 0, 1, 0};

//  unsigned int in[kSize] = {0, 0, 0, 0,
//                            0, 0, 0, 0,
//                            0, 0, 0, 0,
//                            0, 0, 0, 0,
//                            0, 0, 0, 0,
//                            0, 0, 0, 0,
//                            0, 0, 0, 0,
//                            0, 0, 0, 0};

//  unsigned int in[kSize] = {17, 1023, 17, 17,
//                            17, 17, 17, 17,
//                            17, 17, 17, 17,
//                            17, 17, 1023, 17,
//                            17, 17, 17, 17,
//                            17, 17, 17, 17,
//                            17, 17, 17, 17,
//                            17, 17, 17, 17};

  unsigned int out[2 * kSize];
  printf("Compressing the following array (read left to right):\n");
  for (int i = 0; i < kSize; i += 4) {
    printf("%10u, %10u, %10u, %10u", in[i], in[i + 1], in[i + 2], in[i + 3]);
    if (i < kSize - 4)
      printf(",");
    printf("\n");
  }

  int compressed_words = coder->Compression(in, out, kSize);
  printf("\nCompressed to %d words with the following contents:\n", compressed_words);
  for (int i = 0; i < compressed_words; ++i) {
    printf("%-10u:\t", out[i]);
    PrintBinary(out[i]);
    printf("\n");
  }

  unsigned int decompressed[kSize];
  int decompressed_words = coder->Decompression(out, decompressed, kSize);
  printf("\nDecompressed %d words to the following content:\n", decompressed_words);
  for (int i = 0; i < kSize; ++i) {
    printf("%-10u:\t", decompressed[i]);
    PrintBinary(decompressed[i]);
    printf("\n");
  }
}

void TestCoder(char* coder_type) {
  if (strcasecmp(coder_type, "rice") == 0) {
    rice_coding rice_coder;
    TestCoding(&rice_coder);
  } else if (strcasecmp(coder_type, "turbo-rice") == 0) {
    rice_coding2 turbo_rice_coder;
    TestCoding(&turbo_rice_coder);
  } else if (strcasecmp(coder_type, "pfor") == 0) {
    pfor_coding pfor_coder;
    TestCoding(&pfor_coder);
  } else if (strcasecmp(coder_type, "s9") == 0) {
    s9_coding s9_coder;
    TestCoding(&s9_coder);
  } else if (strcasecmp(coder_type, "s16") == 0) {
    s16_coding s16_coder;
    TestCoding(&s16_coder);
  } else if (strcasecmp(coder_type, "vbyte") == 0) {
    vbyte_coding vbyte_coder;
    TestCoding(&vbyte_coder);
  } else if (strcasecmp(coder_type, "null") == 0) {
    null_coding null_coder;
    TestCoding(&null_coder);
  } else {
    fprintf(stderr, "No such coding.\nValid options are 'rice', 'turbo-rice', 'pfor', 's9', 's16', 'vbyte', or 'null'.\n");
  }
}
