//including required header files
#include <stdio.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"

//defining structure
typedef struct Frame_Data
{
  SM_PageHandle data;
  int pageNumber;
  int isDirty;
  int clientcount;
  int freqCount;
  int sample;
}Frame_Data;


SM_FileHandle fh;
#define MAXVALUE 999999999

//Additional function declaration
void * allocateMemoryForBuffer(int size,int flag);
int sys_mtx_lock(void);
int sys_mtx_unlock(void);

//spin lock structure
struct t_spin_lock {
  int locked;
};

//global variable declaration
SM_FileHandle fh;
struct t_spin_lock t_lock;

//function definition for lock
int
sys_mtx_lock(void)
{
  while(true){
    if(t_lock.locked == 0){
    t_lock.locked=1;
    return 0;
  }
  }
}

//function definition for unlock
int
sys_mtx_unlock(void)
{
  if(t_lock.locked == 1){
    t_lock.locked=0;
    return 0;
  }
}

//function definition of allocate memory and it allocates memory based on the type required
void * allocateMemoryForBuffer(size, flag){
  void *memory_ptr=NULL;
  if(flag ==1){
  memory_ptr= (char *)calloc(size,sizeof(Frame_Data));//calloc is used so that it initialize the memmory value to zero
  return memory_ptr;
  }
  else if(flag==2){
    memory_ptr= (char *)calloc(size,sizeof(int));//calloc is used so that it initialize the memmory value to zero
    return memory_ptr;
  }
  else if(flag==3){
    memory_ptr= (char *)calloc(size,sizeof(bool));//calloc is used so that it initialize the memmory value to zero
    return memory_ptr;
  }
  else{
    return NULL;
  }
}

//function definition to initialise the buffer pool it sets all the value to the default value
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,const int numPages, ReplacementStrategy strategy,void *stratData)
{
  if(bm !=NULL &&  pageFileName !="" && strategy >=0 && numPages >0){
  sys_mtx_lock();
  Frame_Data *fd = allocateMemoryForBuffer(numPages, 1);
  if(fd == NULL){
    return RC_MEMORY_ERROR;
  else{
  }
  bm -> bufferpage=numPages;
  int i=0;
  int np=-1;
  int initialize=0;
  bm->pageFile = (char *)pageFileName;
  bm->numPages = numPages;
  bm->strategy = strategy;
  bm->mgmtData = fd;

  do{
    fd[i].data = NULL;
    fd[i].pageNumber = np;
    fd[i].clientcount = initialize;
    fd[i].freqCount = initialize;
    fd[i].isDirty = initialize;

    i=i+1;
  }while(i<bm -> bufferpage);

  bm->writecount = 0;
  sys_mtx_unlock();
  return RC_OK;
  }
}
else{
  return RC_INPUT_PARAMETER_NULL_IN_INITIALIZE;
}
}

//function definition of the shutdown buffer pool where it shut downs the buffer pool there by writing all the dirty pages
RC shutdownBufferPool(BM_BufferPool *const bm)
{
  //sys_mtx_lock();
  if(bm !=NULL){
  int result = forceFlushPool(bm);
  if(result != RC_OK){
    return RC_FORCE_POOL_FAILED;
  }
  int i;

  for(i=0;i< bm->bufferpage;i++)
  {
    if(((Frame_Data  *)bm->mgmtData)[i].clientcount != 0)
    return RC_PINNENPAGES_IN_BUFFER;
  }
  bm->mgmtData = NULL;
  //sys_mtx_unlock();
  return RC_OK;
}
else{
  return RC_INPUT_PARAMETER_NULL_IN_SHUTDOWNBUFFERPOOL;
}
}


//function definition for force flush pool...this will write all the pages in the buffer to disk if it is dirty
RC forceFlushPool(BM_BufferPool *const bm)
{
  if(bm !=NULL){
  sys_mtx_lock();
  int i;
  for(i=0;i<bm -> bufferpage;i++)
  {
    if((((Frame_Data  *)bm->mgmtData)[i].isDirty == 1) && (((Frame_Data  *)bm->mgmtData)[i].clientcount == 0))
    {
      writeBlock (((Frame_Data  *)bm->mgmtData)[i].pageNumber, &fh, ((Frame_Data  *)bm->mgmtData)[i].data);
      bm->writecount++;
      ((Frame_Data  *)bm->mgmtData)[i].isDirty = 0;
    }
  }
    sys_mtx_unlock();
  return RC_OK;
}
else{
  return RC_FORCE_POOL_NULL_PARAMETER;
}
}

//function definition for make dirty...this will mark the given page as dirty
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
  if(bm !=NULL && page !=NULL){
  sys_mtx_lock();
  int i;
  for(i=0;i<bm -> bufferpage;i++)
    {
      if(((Frame_Data  *)bm->mgmtData)[i].pageNumber == page->pageNum)
      {
        ((Frame_Data  *)bm->mgmtData)[i].isDirty = 1;
        sys_mtx_unlock();
        return RC_OK;
      }
    }
  sys_mtx_unlock();
  return RC_GENERAL_ERROR;
}
else{
  return RC_WRITE_DIRTY_PAGE_NULL_PARAMETER;
}
}

//function unpin page...this will unpin the page from the buffer
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
  if(bm != NULL && page != NULL)
  {
  sys_mtx_lock();
  int i;
  for(i=0;i<bm -> bufferpage;i++)
  {
    if(((Frame_Data  *)bm->mgmtData)[i].pageNumber == page->pageNum)
    {
      ((Frame_Data  *)bm->mgmtData)[i].clientcount--;
      break;
    }
  }
  sys_mtx_unlock();
  return RC_OK;
}
else{
  return RC_UNPIN_PAGE_NULL_PARAMETER;
}
}

//function definition for force page...this will write the dirty page to disc
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
  if(bm != NULL && page != NULL)
  {
  int result;
  result =writeDirtyPage(bm,page);
  if(result ==RC_OK){
  return RC_OK;
}
else{
  return RC_WRITE_DIRTY_PAGE_ERROR;
}
}
else{
  return RC_FORCE_PAGE_NULL_PARAMETER;
}
}

//function definition of write dirty page...it will write the given page to the disc if it is dirty
RC writeDirtyPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    if(bm !=NULL && page != NULL){
     sys_mtx_lock();
     int i;
     for(i=0;i<bm -> bufferpage;i++)
    {
    if(((Frame_Data  *)bm->mgmtData)[i].pageNumber == page->pageNum)
    {
     ensureCapacity(((Frame_Data  *)bm->mgmtData)[i].pageNumber, &fh);
     writeBlock (((Frame_Data  *)bm->mgmtData)[i].pageNumber, &fh, ((Frame_Data  *)bm->mgmtData)[i].data);
     bm->writecount++;
     ((Frame_Data  *)bm->mgmtData)[i].isDirty = 0;
      sys_mtx_unlock();
     return RC_OK;
    }
  }
  sys_mtx_unlock();
}
else{
  return RC_WRITE_DIRTY_PAGE_NULL_PARAMETER;
}
}




//function definition to PIN the page...for the given page num
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
  if(bm != NULL && page != NULL){
    if(bm->strategy==RS_FIFO){
        return fifoLogic(bm,page,pageNum);
    }
    else if(bm->strategy==RS_LRU){
        return lruLogic(bm,page,pageNum);
    }
  }
  else{
    return RC_PIN_PAGE_NULL_PARAMETER;
  }
}

//function definition to get the page which is not available in the buffer..so it is fetched from the disc
Frame_Data * notAvailableInBuffer(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){

  if(bm !=NULL && page !=NULL && pageNum >=0){
            sys_mtx_lock();
            Frame_Data *node = (Frame_Data *)malloc(sizeof(Frame_Data));
            int result;
            result = openPageFile (bm->pageFile, &fh);
            if(result != RC_OK){
              return NULL;
            }
            else{
            node->data = (SM_PageHandle) malloc(PAGE_SIZE);
            int readResult;
            readResult=readBlock(pageNum, &fh, node->data);
            if(readResult != RC_OK){
              return NULL;
            }
            else{
            node->pageNumber = pageNum;
            node->isDirty = 0;
            node->clientcount = 1;
            //printf("READ FILE HAPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPENED %d \n",bm->next);
            bm ->readcount++;
            bm ->count++;
            node->freqCount = bm ->count;
            page->pageNum = pageNum;
            page->data = node->data;
            sys_mtx_unlock();
            return node;
          }
          }
          }
          else{
            return NULL;
          }
}


//function definition for implementing LRU algo..
RC lruAlgorithm(BM_BufferPool *const bm, Frame_Data *node, int emptyBufferAvailable, int emptyBufferPageNumber)
{
  if(bm !=NULL && node != NULL){
  int i;
  int lessFrequentlyUsedPage;
  int minimum=MAXVALUE;
  if(emptyBufferPageNumber <= 0){
  for(i=0;i<bm -> bufferpage;i++)
  {
    if(((Frame_Data  *)bm->mgmtData)[i].clientcount == 0)
    {
      if(((Frame_Data  *)bm->mgmtData)[i].freqCount < minimum){
      lessFrequentlyUsedPage= i;
      minimum = ((Frame_Data  *)bm->mgmtData)[i].freqCount;
      }
    }
  }
    if(((Frame_Data  *)bm->mgmtData)[lessFrequentlyUsedPage].isDirty == 1) // checks if dirty page present and writes it to disk
    {
      writeBlock (((Frame_Data  *)bm->mgmtData)[lessFrequentlyUsedPage].pageNumber, &fh, ((Frame_Data  *)bm->mgmtData)[lessFrequentlyUsedPage].data);
      bm->writecount++;
    }
    ((Frame_Data  *)bm->mgmtData)[lessFrequentlyUsedPage].data = node->data;
    ((Frame_Data  *)bm->mgmtData)[lessFrequentlyUsedPage].pageNumber = node->pageNumber;
    ((Frame_Data  *)bm->mgmtData)[lessFrequentlyUsedPage].isDirty = node->isDirty;
    ((Frame_Data  *)bm->mgmtData)[lessFrequentlyUsedPage].clientcount = node->clientcount;
    ((Frame_Data  *)bm->mgmtData)[lessFrequentlyUsedPage].freqCount = node->freqCount;
  }
  else{
    ((Frame_Data  *)bm->mgmtData)[emptyBufferPageNumber].data = node->data;
    ((Frame_Data  *)bm->mgmtData)[emptyBufferPageNumber].pageNumber = node->pageNumber;
    ((Frame_Data  *)bm->mgmtData)[emptyBufferPageNumber].isDirty = node->isDirty;
    ((Frame_Data  *)bm->mgmtData)[emptyBufferPageNumber].clientcount = node->clientcount;
    ((Frame_Data  *)bm->mgmtData)[emptyBufferPageNumber].freqCount = node->freqCount;
  }
    return RC_OK;
}
else{
  return RC_LRU_ALGO_NULL_PARAMETER_ERROR;
}
}



//Function definition to implement LRU logic
RC lruLogic(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
  if(bm !=NULL && page !=NULL && pageNum >=0){
  int i;
  int flag = 0;
  int emptyBufferAvailable=0;
  int emptyBufferPageNumber=-1;
  for(i=0;i<bm -> bufferpage;i++)
  {
    if(((Frame_Data  *)bm->mgmtData)[i].pageNumber == -1)
    {
      emptyBufferAvailable=1;
      emptyBufferPageNumber=((Frame_Data  *)bm->mgmtData)[i].pageNumber;
      break;
    }
    else{
      emptyBufferAvailable=0;
    }
  }
      for(i=0;i<bm -> bufferpage;i++)
      {
        if(i==0){
        if(((Frame_Data  *)bm->mgmtData)[i].pageNumber == -1)
        {
         ((Frame_Data  *)bm->mgmtData)[i].data = (SM_PageHandle) malloc(PAGE_SIZE);
         readBlock(pageNum, &fh, ((Frame_Data  *)bm->mgmtData)[0].data);
         ((Frame_Data  *)bm->mgmtData)[i].pageNumber = pageNum;
         ((Frame_Data  *)bm->mgmtData)[i].clientcount=1;
         //printf("READ FILE HAPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPENED %d \n",bm->next);
         bm ->readcount = i;
         flag=i;
         page->pageNum = pageNum;
         page->data = ((Frame_Data  *)bm->mgmtData)[i].data;
         return RC_OK;
        }
      }
        if(((Frame_Data  *)bm->mgmtData)[i].pageNumber != -1)
        {
           if(((Frame_Data  *)bm->mgmtData)[i].pageNumber == pageNum)
           {
              ((Frame_Data  *)bm->mgmtData)[i].clientcount++;
              flag = 1;
              bm ->count++;
              ((Frame_Data  *)bm->mgmtData)[i].freqCount = bm ->count;
              page->pageNum = pageNum;
              page->data = ((Frame_Data  *)bm->mgmtData)[i].data;
              break;
           }
        }
        else
        {
            ((Frame_Data  *)bm->mgmtData)[i].data = (SM_PageHandle) malloc(PAGE_SIZE);
            readBlock(pageNum, &fh, ((Frame_Data  *)bm->mgmtData)[i].data);
            ((Frame_Data  *)bm->mgmtData)[i].pageNumber = pageNum;
            ((Frame_Data  *)bm->mgmtData)[i].clientcount = 1;
            ((Frame_Data  *)bm->mgmtData)[i].freqCount = bm ->count;
            page->pageNum = pageNum;
            page->data = ((Frame_Data  *)bm->mgmtData)[i].data;
            flag = 1;
            //printf("READ FILE HAPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPENED %d \n",bm->next);
            bm ->readcount++;
            bm ->count++;
            break;
        }
      }
      if(flag == 0)
      {
            Frame_Data *a =notAvailableInBuffer(bm,page,pageNum);
            if(a == NULL){
              return RC_NOT_AVAIL_IN_BUFFER_NULL_PARAMETER;
            }
            int result;
            result=lruAlgorithm(bm,a,emptyBufferAvailable,emptyBufferPageNumber);
            if(result != RC_OK){
              return RC_GENERAL_ERROR;
            }
      }
      return RC_OK;
    }
    else{
      return RC_LRU_LOGIC_NULL_PARAMETER;
    }
}

int fifoAlogorithm(BM_BufferPool *const bm, Frame_Data *node)
{
  if(bm != NULL && node != NULL){
  int i=0;
  int next = bm ->readcount%bm -> bufferpage;
  int emptyBufferAvailable=0;
  int emptyBufferPageNumber=-1;
  for(i=0;i<bm -> bufferpage;i++)
  {
    if(((Frame_Data  *)bm->mgmtData)[i].pageNumber == -1)
    {
      emptyBufferAvailable=1;
      emptyBufferPageNumber=((Frame_Data  *)bm->mgmtData)[i].pageNumber;
      break;
    }
    else{
      emptyBufferAvailable=0;
    }
  }
  while(i<bm -> bufferpage)
  {
    if(((Frame_Data  *)bm->mgmtData)[next].clientcount == 0)
	  {
      if(((Frame_Data  *)bm->mgmtData)[next].isDirty == 1)
	    {
        writeBlock (((Frame_Data  *)bm->mgmtData)[next].pageNumber, &fh, ((Frame_Data  *)bm->mgmtData)[next].data);
        bm -> writecount++;
	    }
    	 break;
	  }
	  else
	  {
       next++;
	     if(next%bm -> bufferpage == 0)
       next=0;
	  }
    i++;
  }
  return RC_OK;
}
  else{
    return RC_ERROR;
  }
}

RC fifoLogic(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
int i;
int flag = 0;
int emptyBufferAvailable=0;
int emptyBufferPageNumber=-1;
for(i=0;i<bm -> bufferpage;i++)
{
  if(((Frame_Data  *)bm->mgmtData)[i].pageNumber == -1)
  {
    emptyBufferAvailable=1;
    emptyBufferPageNumber=((Frame_Data  *)bm->mgmtData)[i].pageNumber;
    break;
  }
  else{
    emptyBufferAvailable=0;
  }
}

for(i=0;i<bm -> bufferpage;i++)
{
  if(((Frame_Data  *)bm->mgmtData)[i].pageNumber != -1)
  {
                 if(((Frame_Data  *)bm->mgmtData)[i].pageNumber == pageNum)
                 {
                    page->data = ((Frame_Data  *)bm->mgmtData)[i].data;
                    flag = 1;
                    break;
                 }
  }
}


  if(((Frame_Data  *)bm->mgmtData)[0].pageNumber == -1)
  {
   ((Frame_Data  *)bm->mgmtData)[0].data = (SM_PageHandle) malloc(PAGE_SIZE);
   readBlock(pageNum, &fh, ((Frame_Data  *)bm->mgmtData)[0].data);
   ((Frame_Data  *)bm->mgmtData)[0].pageNumber = pageNum;
   ((Frame_Data  *)bm->mgmtData)[0].clientcount++;
   page->data = ((Frame_Data  *)bm->mgmtData)[0].data;
   return RC_OK;
  }
   else
   {
      int i=0;
      int buffer_check = 0;
      while(i<bm -> bufferpage)
      {
        if(((Frame_Data  *)bm->mgmtData)[i].pageNumber != -1)
        {
	         if(((Frame_Data  *)bm->mgmtData)[i].pageNumber == pageNum)
	         {
              ((Frame_Data  *)bm->mgmtData)[i].clientcount++;
            	buffer_check = 1;
	            page->pageNum = pageNum;
	            page->data = ((Frame_Data  *)bm->mgmtData)[i].data;
              break;
	         }
        }
        else
        {
            openPageFile (bm->pageFile, &fh);
            ((Frame_Data  *)bm->mgmtData)[i].data = (SM_PageHandle) malloc(PAGE_SIZE);
            readBlock(pageNum, &fh, ((Frame_Data  *)bm->mgmtData)[i].data);
            ((Frame_Data  *)bm->mgmtData)[i].pageNumber = pageNum;
        	  page->data = ((Frame_Data  *)bm->mgmtData)[i].data;
        	  buffer_check = 1;
        	  break;
        }
        i++;
      }
      if(buffer_check == 0)
      {
            Frame_Data *fd = (Frame_Data *)malloc(sizeof(Frame_Data));
            openPageFile (bm->pageFile, &fh);
            fd->data = (SM_PageHandle) malloc(PAGE_SIZE);
            readBlock(pageNum, &fh, fd->data);
            fifoAlogorithm(bm,fd);
            }

      return RC_OK;
    }
    for(i=0;i<bm -> bufferpage;i++)
    {
      if(((Frame_Data  *)bm->mgmtData)[i].pageNumber == -1)
      {
        emptyBufferAvailable=1;
        emptyBufferPageNumber=((Frame_Data  *)bm->mgmtData)[i].pageNumber;
        break;
      }
      else{
        emptyBufferAvailable=0;
      }
    }

}

//function definition get the content of a frame/page
PageNumber *getFrameContents (BM_BufferPool *const bm)
{
  int val[bm -> bufferpage];
  PageNumber *pagenumber = allocateMemoryForBuffer(bm -> bufferpage, 2);
  int i;
  for(i=0;i<bm -> bufferpage;i++)
  {
    val[i] = ((Frame_Data  *)bm->mgmtData)[i].pageNumber;
  }
  for(i=0;i<bm -> bufferpage;i++){
    pagenumber[i]=val[i];
  }
  return pagenumber;
}

//function definition to get the list of dirty pages
bool *getDirtyFlags (BM_BufferPool *const bm)
{
  bool *dPage = allocateMemoryForBuffer(bm -> bufferpage, 3);
  int i;
  for(i=0;i<bm -> bufferpage;i++)
  {
    if(((Frame_Data  *)bm->mgmtData)[i].isDirty != 1)
    {
      dPage[i]= 0;
    }
    else
    {
      dPage[i]=1;
    }
  }
  return dPage;
}

//function definition to the current client count in the buffer pages
int *getFixCounts (BM_BufferPool *const bm)
{
  if(bm != NULL){
  int *clientCount = calloc(bm -> bufferpage,sizeof(int));
  int i;
  int val[bm -> bufferpage];
  for(i=0;i<bm -> bufferpage;i++)
  {
  val[i] = ((Frame_Data  *)bm->mgmtData)[i].clientcount;
  }
  for(i=0;i<bm -> bufferpage;i++)
  {
  clientCount[i] = val[i];
  }
  return clientCount;
}
else{
  return NULL;
}
}

//function definition to get the number of read io from disc
int getNumReadIO (BM_BufferPool *const bm)
{
  if(bm != NULL){
  bm ->readcount++;
  int readFromDisk=(bm ->readcount);
  return readFromDisk;
}
else{
  return RC_READ_COUNT_NULL_ERROR;
}
}

//function definition to get the number of write io to disc
int getNumWriteIO (BM_BufferPool *const bm)
{
  if(bm != NULL){
  int writeInMemory=bm->writecount;
  return writeInMemory;
  }
  else{
    return RC_WRITE_COUNT_NULL_ERROR;
  }
}
