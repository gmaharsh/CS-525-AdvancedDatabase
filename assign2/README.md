=========================
#   Problem Statement   #
=========================

You should implement a buffer manager in this assignment. The buffer manager
manages a fixed number of pages in memory that represent pages from a page
file managed by the storage manager implemented in assignment 1. The memory
pages managed by the buffer manager are called page frames or frames for short.
We call the combination of a page file and the page frames storing pages from
that file a Buffer Pool. The Buffer manager should be able to handle more than
one open buffer pool at the same time. However, there can only be one buffer
pool for each page file. Each buffer pool uses one page replacement strategy
that is determined when the buffer pool is initialized. You should at least
implement two replacement strategies FIFO and LRU.

=========================
# How To Run The Script #
=========================

With default test cases:
————————————————————————————
Compile : make assign2_1
Run : ./assign2_1


With additional test cases:
————————————————————————————
Compile : make assign2_2
Run : ./assign2_2


To revert:
————————————————————————————
On terminal : make clean


=========================
#         Logic         #
=========================

Functions :
——————————

The main purpose of these methods is to manage a set of fixed page frames (pages in memory)
inside the buffer pool. From the storage manager the page number is used to read the requested
page inside the pool i.e pinning. A pointer to a page is returned to a request if the page is
found inside the pool or else it is fetched from the disk. For this purpose the pool uses any
one of the two methods namely LRU and FIFO. Least Recently Used method swaps pages that were
rarely used in set of requests. First In First Out method uses first come first serve logic.
The page that was fetched first gets replaced first. Changes made in the frames are reflected
back to the disk when a page leaves the pool.


Buffer Pool Functions :
_____________________

	1. initBufferPool
		- used to create a buffer pool with fixed page frames taking count from numPages
		- all status values are set to 0 and pointers to NULL
		- makes sure existing files are only cached

	2. shutdownBufferPool
		- used to destroy a buffer pool
		- frees memory allocated to page frames
		- the needed pointers and mgmtData is loaded to variables and checked against pinStatus.
		- if pinStatus is not equal 0 error is returned. Otherwise pages are freed and buffersize is set to 0.

	3.forceFlushPool
		- used for writing all dirty pages back to memory
		- mgmtData is loaded and all pages that are marked dirty are written to disk by running loop till buffersize.
		  the written pages are now reset to "not dirty" i.e 0 values and writecount is incremented.

Page Management Functions :
_________________________

	1. pinPage
		- for pinning the respective page
		- stores the page's page number and data in temporary variables
		- if bufferSize is equal to maxBufferSize one of the replacment strategy is used for pinning.
		  Else pages are pinned by using readBlock() and ensureCapacity() function from storage_manager.c file.

	2. unpinPage
		- for unpinning the specified page
		- the page number is used for guidance and the check made is pageframe[i].Pagenumber == page->pageNum
		  if TRUE the pinStaus is changed and fixCount is decremented.

	3. markDirty
		- used to a mark a page dirty
		- easier for write back identifications
		- if the pageframe[i].Pagenumber == page->pageNum check is TRUE the dirty flag is changed to 1.

	4. forcePage
		- used to write the current page back to memory
		- pageframe[i].Pagenumber == page->pageNum check again is made for all pages under bufferSize.
		- if TRUE the openPageFile() and writeBlock() of storage_mgr.c file are used to write pages to disk
		  and the dirty flag is changed to 0.

Statistics Functions :
____________________

	1. getFrameContents
		- returns an array of page numbers
		- the ith element corresponds to the number of page stored in ith page frame
		- array pageNumbers[i] is given values pageFrame[i].Pagenumber or NO_PAGE

	2. getDirtyFlags
		- returns an arrays of boolean values
		- TRUE corresponds to pages that are dirty
		- ith element corresponds to ith page frame
		- array dirtyBits[i] is given bool values based on dirty flag variable values

	3. getFixCounts
		- returns an array of ints
		- ith element is the fix count of the page stored in ith frame
		- array fixCounts[i] is given values 0 or fixcounts using ternary check

	4. getNumReadIO
		- returns the number of pages in a pool that were read from the disk
		- keeps tracks of all pages read since pool initialization

	5. getNumWriteIO
		- returns the number of pages written to the page file
		- keeps track of all pages written since pool initialization


==========================
#   Additional Features  #
==========================

Additional Test Cases :
——————————————————————

In addition to the default test cases, we have implemented test cases for all the remaining
functions in the test_assign2_2.c . The instructions to run these test cases are provided
above.

No memory leaks :
——————————————————————

The additional test cases are implemented and tested with valgrind for no memory leaks.
