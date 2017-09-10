#include<stdio.h>
#include<stdlib.h>
#include "storage_mgr.h"
#include<string.h>


FILE *page;

extern void initStorageManager (void){
	page = NULL;
}



extern RC createPageFile(char *fileName){
	page = fopen(fileName,"w+");

	if(page == NULL){
		return RC_FILE_NOT_FOUND;
	}
	else {
		SM_PageHandle emptyBlock = (SM_PageHandle)calloc(PAGE_SIZE,sizeof(char));

		if(fwrite(emptyBlock,sizeof(char),PAGE_SIZE,page) >= PAGE_SIZE) {
			fclose(page);
	    free(emptyBlock);
      return RC_OK;
		}

	}


}

extern RC openPageFile(char *fileName, SM_FileHandle *fHandle){
	page = fopen(fileName,"r");
	if(page == NULL){
		return RC_FILE_NOT_FOUND;
	}else{
		fHandle->fileName = fileName;
		fseek(page,0L,SEEK_END);
		int size = ftell(page);
		rewind(page);
		size = size/ PAGE_SIZE;
		fHandle->totalNumPages = size;
		fHandle->mgmtInfo = page;
		fHandle->curPagePos = 0;

		return RC_OK;
	}

}

extern RC closePageFile(SM_FileHandle *fHandle){
	int testVal = fclose(fHandle->mgmtInfo);
	if(testVal == EOF){
		return RC_ERROR;
	}
	return RC_OK;
}
extern RC destroyPageFile(char *fileName){
	int testVal  = remove(fileName);
	if(testVal == 0){
		return RC_OK;
	}
}

extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){


	if(fHandle->mgmtInfo == NULL){
		return RC_FILE_NOT_FOUND;
	}
	if(pageNum > fHandle->totalNumPages && pageNum < 0 ){

		return RC_READ_NON_EXISTING_PAGE;
	}

	int val = fseek(fHandle->mgmtInfo, pageNum*PAGE_SIZE,SEEK_SET);
	if(val != 0){
		return RC_ERROR;
	}

	fread(memPage,sizeof(char),PAGE_SIZE,fHandle->mgmtInfo);
	fHandle->curPagePos = pageNum;
	return RC_OK;

}


extern int getBlockPos(SM_FileHandle *fHandle){
  int currentPagePosition;

  currentPagePosition = fHandle->curPagePos;
  return currentPagePosition;
}


extern RC readFirstBlock(SM_FileHandle *fHandle , SM_PageHandle memPage){
    readBlock(0,fHandle,memPage);
  }
extern RC readLastBlock(SM_FileHandle *fHandle , SM_PageHandle memPage){

  int totalPages = fHandle->totalNumPages;
  if(fHandle->mgmtInfo == NULL){
    return RC_FILE_NOT_FOUND;
  }

  readBlock(totalPages,fHandle,memPage);

}
extern RC readPreviousBlock(SM_FileHandle *fHandle , SM_PageHandle memPage){
  int currentPage = getBlockPos(fHandle);
  int previousPage = currentPage-1;
  readBlock(previousPage,fHandle,memPage);

}

extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
  int currentPage = getBlockPos(fHandle);
  readBlock(currentPage,fHandle,memPage);

}
extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

  int currentPage = getBlockPos(fHandle);
  int nextPage = currentPage+1;
  readBlock(nextPage,fHandle,memPage);
}
extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){

	if(pageNum > fHandle->totalNumPages || pageNum <0){
		return RC_WRITE_FAILED;
	}
  page = fopen(fHandle->fileName,"r+");
  if(page == NULL){
    return RC_FILE_NOT_FOUND;
  }
  int val = fseek(page, pageNum*PAGE_SIZE,SEEK_SET);
	if(val != 0){
		return RC_ERROR;
	}else{
    fwrite(memPage,sizeof(char),strlen(memPage),page);
		fHandle->curPagePos = pageNum;
		fclose(page);

		return RC_OK;
  }
}
extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
  int currentPosition = getBlockPos(fHandle);
	fHandle->totalNumPages += 1;
	writeBlock(currentPosition,fHandle,memPage);

}
extern RC appendEmptyBlock (SM_FileHandle *fHandle){
  page = fopen(fHandle->fileName,"r+");
  int totalPages = fHandle->totalNumPages;
  fHandle->totalNumPages += 1;

  fseek(page,totalPages*PAGE_SIZE,SEEK_SET);
  char c = 0 ;
  int i;
  for(i=0;i<PAGE_SIZE;i++){
    fwrite(&c,sizeof(c),1,page);
  }
}

extern RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle){
  
  int pagesToAdd = numberOfPages - fHandle->totalNumPages;
  if(pagesToAdd > 0){
    for(int i=0;i<pagesToAdd;i++)
        appendEmptyBlock(fHandle);
  }
  if(fHandle->totalNumPages == numberOfPages){
    return RC_OK;
  }
  //printf("%d",fHandle->totalNumPages);
}
