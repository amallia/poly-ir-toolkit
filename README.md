# poly-ir-toolkit
_Information retrieval toolkit for large document collections._

<p>Developed at the Web Exploration and Search Technology Lab at the <a href="http://poly.edu/">Polytechnic Institute of NYU</a>, under the advisement of <a href="http://cis.poly.edu/suel/">Professor Torsten Suel</a>, PolyIRTK provides tools for indexing and querying large document collections.</p>

<p>The aim of PolyIRTK is to act as a platform for research into new algorithms for compression/decompression, querying, and indexing techniques. It implements a number of techniques from recent research literature that improves on the efficiency of indexing and querying. PolyIRTK is also focused on query optimization and early termination algorithms with rank safe properties (that is, faster querying performance, with absolutely no loss in result quality).
Another goal of PolyIRTK is to be a search engine library for applications that require fast full text search.</p>

<h2>Features</h2>

<p>On the querying side:
  * BM25 document ranking
  * DAAT algorithms for AND/OR mode querying
  * WAND query optimization algorithm
  * MaxScore query optimization algorithm
  * MaxScore with block and chunk level score skipping query optimization algorithm
  * Early termination algorithm based on multi-layered indices with list score upperbounds
  * On disk index with LRU caching (cache size is configurable) or a completely in-memory index
  * Support for TREC experiments
  * ...and more</p>

<p>On the indexing side:
  * In-memory compression of intermediate postings with adjustable memory consumption
  * Writes out complete (fully queryable) indices (which are merged later)
  * Supports many fully configurable compression algorithms for docIDs, frequencies, positions, and the block header (Rice, Turbo-Rice, Variable Byte, S9, S16, PForDelta)
  * Allows docID remapping based on document URLs for better compression and significantly faster querying performance.
  * Allows indices to be layered (with several layering algorithms) by partial docID scores, used in various early termination algorithms
  * ...and more</p>
