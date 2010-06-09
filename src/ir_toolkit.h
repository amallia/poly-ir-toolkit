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

#ifndef IR_TOOLKIT_H_
#define IR_TOOLKIT_H_

class IndexCollection;
IndexCollection& GetIndexCollection();

// Uncompresses data pointed by 'src' with length 'src_len' into 'dest'.
// 'dest' will be resized as necessary to completely decompress the 'src' data.
// Upon completion, 'dest_len' will hold the length of the decompressed data,
// 'dest' will point to the start of the decompressed data, and 'dest_size'
// will hold the complete size of the 'dest' buffer, since it could be resized.
int Uncompress(const unsigned char* src, int src_len, unsigned char** dest, int* dest_size, int* dest_len);

// Uses 'Uncompress()' to decompress the gzipped file with filename 'file_path'.
// 'dest' is a pointer to the buffer pointer where the file will be decompressed to; it may change if the allocated buffer was not large enough.
// 'dest_size' is a pointer to the size of the destination buffer; it may change if the allocated buffer was not large enough.
// 'dest_len' is a pointer to the length of the destination buffer; this function will store the number of valid bytes in the decompressed buffer in 'dest_len'.
void UncompressFile(const char* file_path, char** dest, int* dest_size, int* dest_len);

#endif /* IR_TOOLKIT_H_ */
