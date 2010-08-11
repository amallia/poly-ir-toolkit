CXXFLAGS =	-O3 -g -Wall
			#-DNDEBUG : defines NDEBUG to turn off assertions.
			#-pg : enables code profiling.
			#-Wvla : warns about variable length arrays.
			#-Winline : warns about functions declared inline but were not actually inlined.

OBJS =		src/cache_manager.o \
			src/coding_policy.o \
			src/coding_policy_helper.o \
			src/configuration.o \
			src/document_collection.o \
			src/document_map.o \
			src/globals.o \
			src/index_build.o \
			src/index_cat.o \
			src/index_configuration.o \
			src/index_diff.o \
			src/index_merge.o \
			src/index_reader.o \
			src/index_remapper.o \
			src/index_util.o \
			src/ir_toolkit.o \
			src/key_value_store.o \
			src/logger.o \
			src/parser.o \
 			src/parser_callback.o \
			src/posting_collection.o \
			src/query_processor.o \
			src/test_compression.o \
			src/timer.o \
			src/term_hash_table.o \
			src/uncompress_file.o \
			src/compression_toolkit/coding_factory.o \
			src/compression_toolkit/null_coding.o \
			src/compression_toolkit/pfor_coding.o \
			src/compression_toolkit/rice_coding.o \
			src/compression_toolkit/rice_coding2.o \
			src/compression_toolkit/s9_coding.o \
			src/compression_toolkit/s16_coding.o \
			src/compression_toolkit/vbyte_coding.o \
			src/compression_toolkit/unpack.o

LIBS =		-lpthread -lrt -lz
			#-lpthread : pthreads support.
			#-lrt : necessary for POSIX Asynchronous I/O support (AIO).
			#-lz : zlib support.
			#-static-libgcc -lc_p : profiling support for C library calls.
			#-lefence : electric fence memory debugger (only used for debugging).

TARGET =	irtk

$(TARGET):	$(OBJS)
	$(CXX) -o $(TARGET) $(OBJS) $(LIBS)
	#-pg : enables code profiling.

all:	$(TARGET)

clean:
	rm -f $(OBJS) $(TARGET)

clear:
	rm -f document_collections_doc_id_ranges index*.idx* index*.lex* index*.meta* index*.dmap*
