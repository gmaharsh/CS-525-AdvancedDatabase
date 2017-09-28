
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "buffer_mgr.h"
#include "storage_mgr.h"

typedef struct Frame {
        SM_PageHandle content;
        int dirty;
        int pinStatus;
        int freeStat;
        int fixCount;
        PageNumber PageNumber;
        void *qPointer ;
} Frame;

typedef struct Queue {
        int count;
        int position;
        Frame *framePointer;
        int pNo;
} Queue;
int bufferSize= 0;
int maxBufferSize;
int queueSize;
int isBufferFull= 0;
int currentQueueSize;
int writeCount = 0;
int readCount = 0;

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData){

        queueSize = maxBufferSize = numPages;
        currentQueueSize = 0;
        writeCount= 0;
        readCount =0;

        bm->pageFile = (char *)pageFileName;
        bm->numPages = numPages;
        bm->strategy = strategy;


        int i;

        Frame *pageFrame = malloc(sizeof(Frame) * numPages);
        Queue *queueFrame = malloc(sizeof(Queue) * numPages);

        for(i=0; i<numPages; i++) {
                pageFrame[i].content = (SM_PageHandle)malloc(PAGE_SIZE);
                pageFrame[i].dirty = 0;
                pageFrame[i].pinStatus = 0;
                pageFrame[i].freeStat = 0;
                pageFrame[i].PageNumber = -1;
                pageFrame[i].fixCount = 0;

                queueFrame[i].count = 0;
                queueFrame[i].position = 0;
                queueFrame[i].framePointer = NULL;
                queueFrame[i].pNo = -1;
        }

        pageFrame[0].qPointer = queueFrame;
        bm->mgmtData = pageFrame;
        return RC_OK;
}

Frame * returnPagePointer(BM_BufferPool *const bm,BM_PageHandle *const page){
        Frame *pageFrames = (Frame *)bm->mgmtData;
        for(int i =0; i<maxBufferSize; i++) {
                if(page->pageNum == pageFrames[i].PageNumber) {
                        return &pageFrames[i];
                        break;
                }
        }
}

int maxQueue(Queue *q){
        int max = -1;
        for(int i = 0; i < currentQueueSize; i++) {
                if(q[i].position > max) {
                        max = q[i].position;
                }
        }
        return max;
}

Queue LRU(BM_BufferPool *const bm, BM_PageHandle *const page,int pageNum){
        Frame *pageFrames = (Frame *)bm->mgmtData;
        Queue *queue = (Queue *)pageFrames[0].qPointer;

        for(int i =0; i <currentQueueSize; i++) {
                if(pageFrames[i].PageNumber == pageNum) {
                        //pageFrames[i].fixCount += 1;
                        queue[i].position = 1;
                        for(int j=0; j<currentQueueSize; j++) {
                                if(j != i)
                                        queue[j].position++;
                        }

                        return queue[i];
                }
        }

        if(currentQueueSize < bm->numPages) {

                if(currentQueueSize == 0) {

                        queue[0].position = 1;
                        queue[0].framePointer = &pageFrames[0];
                        //printf("%d\n",queue[0].position);
                        currentQueueSize++;

                        return queue[0];
                }
                else{
                        for(int i =0; i<maxBufferSize; i++) {

                                if(queue[i].framePointer == NULL) {
                                        queue[i].position = 1;
                                        for(int j=0; j<maxBufferSize; j++) {
                                                if(j != i)
                                                        queue[j].position +=1;
                                        }
                                        queue[i].framePointer = returnPagePointer(bm,page);
                                        //printf("%d\n",queue[i].position);
                                        currentQueueSize++;
                                        return queue[i];
                                }

                        }
                }

        }else if(currentQueueSize == queueSize ) {
                //printf("lol %d",currentQueueSize);

                for(int i =0; i<currentQueueSize; i++) {
                      //printf("%d %d %d\n",queue[i].position,pageFrames[i].PageNumber,pageFrames[i].fixCount);
                        if(queue[i].position == maxQueue(queue)) {
                                if(pageFrames[i].fixCount == 0) {
                                        queue[i].position = 1;
                                        for(int j=0; j<currentQueueSize; j++) {
                                                if(j != i)
                                                        queue[j].position++;
                                                //printf("%d",queue[j].position);
                                        }

                                        SM_FileHandle fh;
                                        if(pageFrames[i].dirty ==1) {
                                                openPageFile(bm->pageFile, &fh);
                                                ensureCapacity(pageFrames[i].PageNumber,&fh);
                                                writeBlock(pageFrames[i].PageNumber, &fh, pageFrames[i].content);
                                                writeCount++;
                                        }
                                        pageFrames[i].pinStatus = 1;
                                        pageFrames[i].PageNumber = pageNum;
                                        //printf("%d",pf->PageNumber);
                                        pageFrames[i].freeStat = 1;
                                        pageFrames[i].dirty = 0;
                                        pageFrames[i].fixCount = 0;
                                        openPageFile(bm->pageFile,&fh);
                                        //pageFrames[i].content  = (SM_PageHandle)malloc(PAGE_SIZE);
                                        ensureCapacity(pageFrames[i].PageNumber, &fh);
                                        readBlock(pageFrames[i].PageNumber,&fh,pageFrames[i].content);
                                        readCount++;
                                        //printf(" %d %d %d %s\n",a.position,pf->PageNumber,maxBufferSize,pf->content);
                                        page->data  = pageFrames[i].content;
                                        page->pageNum = pageFrames[i].PageNumber;


                                        return queue[i];
                                }
                        }
                }

                for(int i =0; i<currentQueueSize; i++) {
                        int temp = currentQueueSize-1;
                        if(queue[i].position == temp && pageFrames[i].fixCount == 0) {
                                queue[i].position = 1;
                                for(int j=0; j<currentQueueSize; j++) {
                                        if(j != i)
                                                queue[j].position++;

                                }

                                SM_FileHandle fh;
                                if(pageFrames[i].dirty ==1) {
                                        openPageFile(bm->pageFile, &fh);
                                        ensureCapacity(pageFrames[i].PageNumber,&fh);
                                        writeBlock(pageFrames[i].PageNumber, &fh, pageFrames[i].content);
                                        writeCount++;
                                }
                                pageFrames[i].pinStatus = 1;
                                pageFrames[i].PageNumber = pageNum;
                                //printf("%d",pf->PageNumber);
                                pageFrames[i].freeStat = 1;
                                pageFrames[i].dirty = 0;
                                pageFrames[i].fixCount = 0;
                                openPageFile(bm->pageFile,&fh);
                                //pageFrames[i].content  = (SM_PageHandle)malloc(PAGE_SIZE);
                                ensureCapacity(pageFrames[i].PageNumber, &fh);
                                readBlock(pageFrames[i].PageNumber,&fh,pageFrames[i].content);
                                readCount++;
                                //printf(" %d %d %d %s\n",a.position,pf->PageNumber,maxBufferSize,pf->content);
                                page->data  = pageFrames[i].content;
                                page->pageNum = pageFrames[i].PageNumber;


                                return queue[i];
                        }
                }
        }
}

Queue FIFO(BM_BufferPool *const bm, BM_PageHandle *const page,int pageNum){
        Frame *pageFrames = (Frame *)bm->mgmtData;
        Queue *queue = (Queue *)pageFrames[0].qPointer;

        if(currentQueueSize < bm->numPages) {

                if(currentQueueSize == 0) {

                        queue[0].position = 1;
                        queue[0].framePointer = &pageFrames[0];
                        queue[0].pNo = pageNum;
                        //printf("%d\n",queue[0].position);
                        currentQueueSize++;

                        return queue[0];
                }
                else{
                        for(int i =0; i<maxBufferSize; i++) {

                                if(queue[i].framePointer == NULL) {
                                        queue[i].position = 1;
                                        queue[i].pNo = pageNum;
                                        for(int j=0; j<maxBufferSize; j++) {
                                                if(j != i)
                                                        queue[j].position +=1;
                                        }
                                        queue[i].framePointer = returnPagePointer(bm,page);
                                        //printf("%d\n",queue[i].position);
                                        currentQueueSize++;
                                        return queue[i];
                                }
                        }
                }
        }
        else if(currentQueueSize == queueSize ) {
                //printf("lol %d",currentQueueSize);

                for(int i =0; i<currentQueueSize; i++) {

                        if(pageFrames[i].PageNumber == pageNum) {
                                pageFrames[i].fixCount += 1;
                                return queue[i];
                        }

                        if(queue[i].position == currentQueueSize) {
                                if(pageFrames[i].fixCount == 0) {

                                        queue[i].position = 1;
                                        for(int j=0; j<currentQueueSize; j++) {
                                                if(j != i)
                                                        queue[j].position++;
                                                //printf("%d",queue[j].position);
                                        }

                                        SM_FileHandle fh;
                                        if(pageFrames[i].dirty ==1) {
                                                openPageFile(bm->pageFile, &fh);
                                                ensureCapacity(pageFrames[i].PageNumber,&fh);
                                                writeBlock(pageFrames[i].PageNumber, &fh, pageFrames[i].content);
                                                writeCount++;
                                        }
                                        pageFrames[i].pinStatus = 1;
                                        pageFrames[i].PageNumber = pageNum;
                                        //printf("%d",pf->PageNumber);
                                        pageFrames[i].freeStat = 1;
                                        pageFrames[i].dirty = 0;
                                        pageFrames[i].fixCount = 0;
                                        openPageFile(bm->pageFile,&fh);

                                        //pageFrames[i].content  = (SM_PageHandle)malloc(PAGE_SIZE);
                                        ensureCapacity(pageFrames[i].PageNumber, &fh);
                                        readBlock(pageFrames[i].PageNumber,&fh,pageFrames[i].content);
                                        readCount++;

                                        page->data  = pageFrames[i].content;
                                        page->pageNum = pageFrames[i].PageNumber;


                                        return queue[i];
                                }
                        }
                }

                for(int i =0; i<currentQueueSize; i++) {
                        int temp = currentQueueSize-1;
                        if(queue[i].position == temp && pageFrames[i].fixCount == 0) {
                                queue[i].position = 1;
                                for(int j=0; j<currentQueueSize; j++) {
                                        if(j != i)
                                                queue[j].position++;

                                }

                                SM_FileHandle fh;
                                if(pageFrames[i].dirty ==1) {
                                        openPageFile(bm->pageFile, &fh);
                                        ensureCapacity(pageFrames[i].PageNumber,&fh);
                                        writeBlock(pageFrames[i].PageNumber, &fh, pageFrames[i].content);
                                        writeCount++;
                                }
                                pageFrames[i].pinStatus = 1;
                                pageFrames[i].PageNumber = pageNum;

                                pageFrames[i].freeStat = 1;
                                pageFrames[i].dirty = 0;
                                pageFrames[i].fixCount = 0;
                                openPageFile(bm->pageFile,&fh);

                                //pageFrames[i].content  = (SM_PageHandle)malloc(PAGE_SIZE);
                                ensureCapacity(pageFrames[i].PageNumber, &fh);
                                readBlock(pageFrames[i].PageNumber,&fh,pageFrames[i].content);
                                readCount++;

                                page->data  = pageFrames[i].content;
                                page->pageNum = pageFrames[i].PageNumber;


                                return queue[i];
                        }
                }
        }
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){


        int size = bm->numPages;
        Frame *pageFrame = (Frame *)bm->mgmtData;
        //Queue a;
        if(bufferSize == maxBufferSize) {
                //printf("s %d",bufferSize);
                isBufferFull = 1;
                if(bm->strategy == RS_FIFO)
                        FIFO(bm,page,pageNum);
                else
                        LRU(bm,page,pageNum);
                return RC_OK;
        }
        else if(isBufferFull == 0) {
                //printf("a %d",bufferSize);
                if(bufferSize == 0) {
                        if(bm->strategy == RS_FIFO)
                                FIFO(bm,page,pageNum);
                        else
                                LRU(bm,page,pageNum);

                        SM_FileHandle fh;
                        //printf("lol");
                        pageFrame[0].pinStatus = 1;
                        pageFrame[0].PageNumber = pageNum;

                        openPageFile(bm->pageFile,&fh);
                        pageFrame[0].freeStat = 1;

                        //pageFrame[0].content  = (SM_PageHandle)malloc(PAGE_SIZE);

                        ensureCapacity(pageNum, &fh);
                        readBlock(pageNum,&fh,pageFrame[0].content);
                        readCount++;

                        page->data  = pageFrame[0].content;
                        page->pageNum = pageFrame[0].PageNumber;


                        bufferSize++;
                        return RC_OK;

                }
                else{

                        for(int i=1; i<size; i++) {
                                if(pageFrame[i].PageNumber == pageNum) {
                                        if(bm->strategy == RS_FIFO)
                                                FIFO(bm,page,pageNum);
                                        else
                                                LRU(bm,page,pageNum);
                                        //printf("%d %d\n",a.position,pageFrame[i].PageNumber);
                                        pageFrame[i].pinStatus = 1;
                                        pageFrame[i].freeStat = 1;
                                        page->pageNum = pageFrame[i].PageNumber;
                                        //pageFrame[i].PageNumber = pageNum;
                                        page->data  = pageFrame[i].content;

                                        //page->pageNum = pageNum;
                                        return RC_OK;
                                }
                        }
                        for(int i=1; i<size; i++) {
                                if(pageFrame[i].freeStat == 0) {
                                        SM_FileHandle fh;
                                        if(bm->strategy == RS_FIFO)
                                                FIFO(bm,page,pageNum);
                                        else
                                                LRU(bm,page,pageNum);

                                        pageFrame[i].freeStat = 1;
                                        pageFrame[i].pinStatus = 1;
                                        pageFrame[i].PageNumber = pageNum;

                                        openPageFile(bm->pageFile,&fh);

                                        //pageFrame[i].content  = (SM_PageHandle)malloc(PAGE_SIZE);

                                        ensureCapacity(pageNum, &fh);
                                        readBlock(pageNum,&fh,pageFrame[i].content);
                                        readCount++;
                                        //printf("data : %s %d\n",pageFrame[i].content,bufferSize);

                                        page->data  = pageFrame[i].content;
                                        page->pageNum = pageNum;
                                        bufferSize++;
                                        //free(pageFrame[i].content);
                                        return RC_OK;
                                }
                        }
                        return RC_OK;
                }
        }

}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){

        Frame *pageFrame = (Frame *)bm->mgmtData;
        int i;
        for(i=0; i<bufferSize; i++) {

                if(pageFrame[i].PageNumber == page->pageNum) {
                    //free(pageFrame[i].content);
                        //  printf("%d %d",pageFrame[i].PageNumber,page->pageNum);
                        pageFrame[i].pinStatus = 0;
                        if(pageFrame[i].fixCount> 0)
                                pageFrame[i].fixCount--;
                        else
                                pageFrame[i].fixCount=0;
                        //free(pageFrame[i].content);
                        pageFrame[1].pinStatus = 0;

                        return RC_OK;
                }
        }
        //return RC_OK;
}

RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){

        Frame *pageFrame = (Frame *)bm->mgmtData;
        int i;
        for(i=0; i<bufferSize; i++) {
                if(pageFrame[i].PageNumber == page->pageNum) {
                        pageFrame[i].dirty = 1;

                        //  printf("%s %s %d %d %d\n",pageFrame[i].content,page->data,pageFrame[i].dirty,page->pageNum,i);
                        return RC_OK;
                }
        }
        return RC_ERROR;
}


extern RC shutdownBufferPool(BM_BufferPool *const bm)
{
        Frame *pageFrame = (Frame *)bm->mgmtData;
        Queue *queue = pageFrame[0].qPointer;
        forceFlushPool(bm);

        int i;
        for(i = 0; i < bufferSize; i++)
        {

                if(pageFrame[i].pinStatus != 0)
                {
                        //printf("%d %d",i,pageFrame[1].pinStatus);
                        return RC_ERROR;
                }

                //free(bm->mgmtData);
        }
        for(i=0;i< maxBufferSize;i++){
          free(pageFrame[i].content);
        }
        //free(pageFrame[0].content);
        //pageFrame[0].qPointer= NULL;
        free(queue);
        free(pageFrame);

        bufferSize = 0;
        isBufferFull = 0;
        currentQueueSize=0;
        bm->mgmtData = NULL;

        return RC_OK;
}


extern RC forceFlushPool(BM_BufferPool *const bm)
{
        Frame *pageFrame = (Frame *)bm->mgmtData;

        int i;

        for(i = 0; i < bufferSize; i++)
        {
                if(pageFrame[i].dirty == 1)
                {
                        SM_FileHandle fh;

                        openPageFile(bm->pageFile, &fh);

                        ensureCapacity(pageFrame[i].PageNumber,&fh);

                        writeBlock(pageFrame[i].PageNumber, &fh, pageFrame[i].content);

                        pageFrame[i].dirty = 0;
                        writeCount++;
                }
        }
        return RC_OK;
}

extern RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
        Frame *pageFrame = (Frame *)bm->mgmtData;

        for(int i = 0; i < bufferSize; i++)
        {

                if(pageFrame[i].PageNumber == page->pageNum)
                {
                        SM_FileHandle fh;
                        openPageFile(bm->pageFile, &fh);
                        writeBlock(pageFrame[i].PageNumber, &fh, pageFrame[i].content);
                        writeCount++;
                        pageFrame[i].dirty= 0;
                }
        }
        return RC_OK;
}


extern PageNumber *getFrameContents (BM_BufferPool *const bm)
{

        PageNumber *pageNumbers = malloc(sizeof(PageNumber) * bufferSize);
        Frame *pageFrame = (Frame *) bm->mgmtData;

        int i = 0;

        while(i < maxBufferSize) {
                pageNumbers[i] = (pageFrame[i].PageNumber != -1) ? pageFrame[i].PageNumber : NO_PAGE;
                //printf("%d\n",pageNumbers[i]);
                i++;
        }
        return pageNumbers;
        free(pageNumbers);
}

extern bool *getDirtyFlags (BM_BufferPool *const bm)
{
        bool *dirtyBits = malloc(sizeof(bool) * bufferSize);
        Frame *pageFrame = (Frame *)bm->mgmtData;

        int i;
        for(i = 0; i < maxBufferSize; i++)
        {
                dirtyBits[i] = (pageFrame[i].dirty == 1) ? true : false;
        }
        return dirtyBits;
        free(dirtyBits);
}

extern int *getFixCounts (BM_BufferPool *const bm)
{
        int *fixCounts = malloc(sizeof(int) * bufferSize);
        Frame *pageFrame= (Frame *)bm->mgmtData;

        int i = 0;
        while(i < maxBufferSize)
        {
                fixCounts[i] = (pageFrame[i].fixCount != -1) ? pageFrame[i].fixCount : 0;
                i++;
        }
        return fixCounts;
          free(fixCounts);
}

extern int getNumReadIO (BM_BufferPool *const bm)
{
        return readCount;
}

extern int getNumWriteIO (BM_BufferPool *const bm)
{
        return writeCount;
}
