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

#ifndef COMPRESSION_POLICY_H_
#define COMPRESSION_POLICY_H_

#include <stdint.h>

#include <string>

/**************************************************************************************************************************************************************
 * CodingPolicy
 *
 **************************************************************************************************************************************************************/
class coding;
class CodingPolicy {
public:
  class Status {
  public:
    enum StatusCode {
      kOk,
      kUnrecognizedCodingPolicy,
      kNoCodingPolicySpecified,
      kBlockSizeNotPositive,
      kPrimaryCoderMustBeBlockwise,
      kPrimaryCoderMustBeNonBlockwise,
      kLeftoverCoderMustBeNonBlockwise,
      kNoSuchCoderPrimary,
      kNoSuchCoderLeftover,
      kDocIdCoderBlockSizeMustMatchChunkSize,
      kFrequencyCoderBlockSizeMustMatchChunkSize,
      kPositionCoderBlockSizeMustBeMultipleOfMaxProperties
    };

    Status() :
      status_code_(kOk) {
    }

    Status(StatusCode status_code) :
      status_code_(status_code) {
    }

    const char* GetStatusMessage() {
      static const char* kStatusMessages[] = {
                "OK",
                "Could not recognize coding policy",
                "No coding policy specified",
                "Coding policy block size must be positive",
                "Coding policy primary coder must be blockwise when a leftover coder is specified",
                "Coding policy primary coder must be non-blockwise when no leftover coder is specified",
                "Coding policy leftover coder must be non-blockwise",
                "No such primary coder available",
                "No such leftover coder available",
                "A blockwise docID coding policy must have the block size match the chunk size (defined in 'index_layout_parameters.h')",
                "A blockwise frequency coding policy must have the block size match the chunk size (defined in 'index_layout_parameters.h')",
                "A blockwise position coding policy must have the block size be a multiple of the chunk size multiplied by the max frequency properties (defined in 'index_layout_parameters.h')" };

      return kStatusMessages[status_code_];
    }

    StatusCode status_code() const {
      return status_code_;
    }

  private:
    StatusCode status_code_;
  };

  // Describes what we're going to be coding.
  enum CodingProperty {
    kDocId, kFrequency, kPosition, kBlockHeader
  };

  CodingPolicy(CodingProperty coding_property);
  ~CodingPolicy();

  Status LoadPolicy(const std::string& policy_str);

  int Compress(uint32_t* input, uint32_t* output, int num_input_elements) const;

  int Decompress(uint32_t* input, uint32_t* output, int num_input_elements) const;

  coding* primary_coder() const {
    return primary_coder_;
  }

  coding* leftover_coder() const {
    return leftover_coder_;
  }

  int block_size() const {
    return block_size_;
  }

  int min_padding_size() const {
    return min_padding_size_;
  }

  bool primary_coder_is_blockwise() const {
    return primary_coder_is_blockwise_;
  }

private:
  Status VerifyCodingPolicyMatchesCodingProperty();

  CodingProperty coding_property_;
  coding* primary_coder_;
  coding* leftover_coder_;
  int block_size_;
  int min_padding_size_;
  bool primary_coder_is_blockwise_;
};

#endif /* COMPRESSION_POLICY_H_ */
