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

#include "coding_policy.h"

#include <cassert>
#include <cstdlib>

#include <sstream>
#include <vector>

#include <strings.h>

#include "coding_methods.h"
#include "compression_toolkit/coding.h"
#include "compression_toolkit/pfor_coding.h"
#include "compression_toolkit/rice_coding.h"
#include "compression_toolkit/rice_coding2.h"
#include "compression_toolkit/s9_coding.h"
#include "compression_toolkit/s16_coding.h"
#include "compression_toolkit/vbyte_coding.h"
#include "compression_toolkit/null_coding.h"
#include "index_layout_parameters.h"
using namespace std;

coding* MapCoding(const char* coder_type) {
  if (strcasecmp(coder_type, coding_methods::kRiceCoding) == 0) {
    return new rice_coding();
  } else if (strcasecmp(coder_type, coding_methods::kTurboRiceCoding) == 0) {
    return new rice_coding2();
  } else if (strcasecmp(coder_type, coding_methods::kPForDeltaCoding) == 0) {
    return new pfor_coding();
  } else if (strcasecmp(coder_type, coding_methods::kS9Coding) == 0) {
    return new s9_coding();
  } else if (strcasecmp(coder_type, coding_methods::kS16Coding) == 0) {
    return new s16_coding();
  } else if (strcasecmp(coder_type, coding_methods::kVarByteCoding) == 0) {
    return new vbyte_coding();
  } else if (strcasecmp(coder_type, coding_methods::kNullCoding) == 0) {
    return new null_coding();
  } else {
    return NULL;
  }
}

bool StringExistsInArray(const char* str, const char** arr, int arr_len) {
  for (int i = 0; i < arr_len; ++i) {
    if (strcasecmp(arr[i], str) == 0) {
      return true;
    }
  }
  return false;
}

/**************************************************************************************************************************************************************
 * CodingPolicy
 *
 **************************************************************************************************************************************************************/
CodingPolicy::CodingPolicy(CodingProperty coding_property) :
  coding_property_(coding_property), primary_coder_(NULL), leftover_coder_(NULL), block_size_(0), min_padding_size_(0), primary_coder_is_blockwise_(false) {
}

CodingPolicy::~CodingPolicy() {
  delete primary_coder_;
  delete leftover_coder_;
}

CodingPolicy::Status CodingPolicy::LoadPolicy(const std::string& policy_str) {
  // Lists the subset of the available coding methods that are blockwise.
  const char* kBlockwiseCodingMethods[] = { coding_methods::kTurboRiceCoding, coding_methods::kPForDeltaCoding };
  int num_blockwise_codings = sizeof(kBlockwiseCodingMethods) / sizeof(*kBlockwiseCodingMethods);

  // Lists the subset of the available coding methods that are non-blockwise.
  const char* kNonBlockwiseCodingMethods[] = { coding_methods::kRiceCoding, coding_methods::kS9Coding, coding_methods::kS16Coding,
                                               coding_methods::kVarByteCoding, coding_methods::kNullCoding };
  int num_non_blockwise_codings = sizeof(kNonBlockwiseCodingMethods) / sizeof(*kNonBlockwiseCodingMethods);

  const char kDelim = ':';
  vector<string> tokens;

  stringstream iss(policy_str);
  std::string token;
  while (std::getline(iss, token, kDelim)) {
    tokens.push_back(token);
  }

  Status status;
  if (tokens.size() == 0) {
    // No coding policy specified.
    status = Status(Status::kNoCodingPolicySpecified);
  } else if (tokens.size() == 1) {
    // Should be a non-blockwise coding method.
    primary_coder_is_blockwise_ = false;

    const char* primary_coding = tokens[0].c_str();
    // Verify non-blockwise coding.
    if (StringExistsInArray(primary_coding, kNonBlockwiseCodingMethods, num_non_blockwise_codings)) {
      primary_coder_ = MapCoding(primary_coding);
      assert(primary_coder_ != NULL);
    } else {
      if (StringExistsInArray(primary_coding, kBlockwiseCodingMethods, num_blockwise_codings)) {
        status = Status(Status::kPrimaryCoderMustBeNonBlockwise);
      } else {
        status = Status(Status::kNoSuchCoderPrimary);
      }
    }
  } else if (tokens.size() == 4) {
    // Should be a blockwise coding method.
    primary_coder_is_blockwise_ = true;

    const char* primary_coding = tokens[0].c_str();
    // Verify blockwise coding.
    if (StringExistsInArray(primary_coding, kBlockwiseCodingMethods, num_blockwise_codings)) {
      primary_coder_ = MapCoding(primary_coding);
      assert(primary_coder_ != NULL);

      // Block size comes next. There are some restrictions on block size.
      block_size_ = atoi(tokens[1].c_str());
      if (block_size_ <= 0) {
        status = Status(Status::kBlockSizeNotPositive);
      }

      primary_coder_->set_size(block_size_);

      const char* leftover_coding = tokens[2].c_str();
      int num_non_blockwise_codings = sizeof(kNonBlockwiseCodingMethods) / sizeof(*kNonBlockwiseCodingMethods);
      // Verify non-blockwise coding.
      if (StringExistsInArray(leftover_coding, kNonBlockwiseCodingMethods, num_non_blockwise_codings)) {
        leftover_coder_ = MapCoding(leftover_coding);
        assert(leftover_coder_ != NULL);
        // Last thing is the minimum padding size.
        min_padding_size_ = atoi(tokens[3].c_str());
      } else {
        if (StringExistsInArray(leftover_coding, kBlockwiseCodingMethods, num_blockwise_codings)) {
          status = Status(Status::kLeftoverCoderMustBeNonBlockwise);
        } else {
          status = Status(Status::kNoSuchCoderLeftover);
        }
      }
    } else {
      if (StringExistsInArray(primary_coding, kNonBlockwiseCodingMethods, num_non_blockwise_codings)) {
        status = Status(Status::kPrimaryCoderMustBeBlockwise);
      } else {
        status = Status(Status::kNoSuchCoderPrimary);
      }
    }
  } else {
    status = Status(Status::kUnrecognizedCodingPolicy);
  }

  if (status.status_code() == Status::kOk) {
    return VerifyCodingPolicyMatchesCodingProperty();
  }

  return status;
}

CodingPolicy::Status CodingPolicy::VerifyCodingPolicyMatchesCodingProperty() {
  switch (coding_property_) {
    case kDocId:
    case kFrequency:
      if (block_size_ > 0 && block_size_ != CHUNK_SIZE) {
        if (coding_property_ == kDocId)
          return Status::kDocIdCoderBlockSizeMustMatchChunkSize;
        else if (coding_property_ == kFrequency)
          return Status::kFrequencyCoderBlockSizeMustMatchChunkSize;
      }
      break;
    case kPosition:
      if (block_size_ > 0 && block_size_ % (CHUNK_SIZE * MAX_FREQUENCY_PROPERTIES) != 0) {
        return Status::kPositionCoderBlockSizeMustBeMultipleOfMaxProperties;
      }
      break;
    case kBlockHeader:
      break;
  }

  return Status::kOk;
}

// The 'input' array size should be at least an upper multiple of 'block_size_'.
int CodingPolicy::Compress(uint32_t* input, uint32_t* output, int num_input_elements) const {
  assert(input != NULL);
  assert(output != NULL);
  assert(num_input_elements > 0);

  int compressed_len = 0;
  if (leftover_coder_ != NULL) {
    int num_whole_blocks = num_input_elements / block_size_;
    int encoded_offset = 0;
    int unencoded_offset = 0;
    while (num_whole_blocks-- > 0) {
      encoded_offset += primary_coder_->Compression(input + unencoded_offset, output + encoded_offset, block_size_);
      unencoded_offset += block_size_;
    }

    int left_to_encode = num_input_elements % block_size_;
    if (left_to_encode == 0) {
      // Nothing to do here.
    } else if (left_to_encode < min_padding_size_) {
      // Encode leftover portion with a non-blockwise coder.
      encoded_offset += leftover_coder_->Compression(input + unencoded_offset, output + encoded_offset, left_to_encode);
    } else {
      // Encode leftover portion with a blockwise coder, and pad it to the blocksize.
      // Assumption here is that the 'input' array size is at least an upper multiple of 'block_size_'.
      int pad_until = block_size_ * ((num_input_elements / block_size_) + 1);
      for (int i = num_input_elements; i < pad_until; ++i) {
        input[i] = 0;
      }
      encoded_offset += primary_coder_->Compression(input + unencoded_offset, output + encoded_offset, block_size_);
    }

    compressed_len = encoded_offset;
  } else {
    compressed_len = primary_coder_->Compression(input, output, num_input_elements);
  }

  return compressed_len;
}

// The 'output' array size should be at least an upper multiple of 'block_size_'.
int CodingPolicy::Decompress(uint32_t* input, uint32_t* output, int num_input_elements) const {
  assert(input != NULL);
  assert(output != NULL);
  assert(num_input_elements > 0);

  int compressed_len = 0;
  if (leftover_coder_ != NULL) {
    int num_whole_blocks = num_input_elements / block_size_;
    int encoded_offset = 0;
    int unencoded_offset = 0;
    while (num_whole_blocks-- > 0) {
      encoded_offset += primary_coder_->Decompression(input + encoded_offset, output + unencoded_offset, block_size_);
      unencoded_offset += block_size_;
    }

    int left_to_encode = num_input_elements % block_size_;
    if (left_to_encode == 0) {
      // Nothing to do here.
    } else if (left_to_encode < min_padding_size_) {
      // Decode leftover portion with a non-blockwise coder.
      encoded_offset += leftover_coder_->Decompression(input + encoded_offset, output + unencoded_offset, left_to_encode);
    } else {
      // Decode leftover portion with a blockwise coder, since it was padded to the blocksize.
      // Assumption here is that the 'output' array size is at least an upper multiple of 'block_size_'.
      encoded_offset += primary_coder_->Decompression(input + encoded_offset, output + unencoded_offset, block_size_);
    }

    compressed_len = encoded_offset;
  } else {
    compressed_len = primary_coder_->Decompression(input, output, num_input_elements);
  }

  return compressed_len;
}
