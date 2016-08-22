#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dt.h"


#define tableAttributeCount 3

typedef struct bufferdata
{
    BM_PageHandle pageHandler;
    BM_BufferPool poolMgmt;
}bufferdata;

typedef struct tableStatistics
{
  int tuplesCount;
  int emptySlots;
}tableStatistics;

typedef struct scanner
{
    Expr *scanClause;
    RID tId;
} scanner;



int sizeOfTuple;
int attributeOffset;
char *r_Ptr;
bufferdata *bd;
Schema *sMem;
tableStatistics *ts;
SM_FileHandle fileHandler;
SM_PageHandle pHandler;
RID *tId;
scanner *scan_Ptr;



int allocatememoryfortableStatistics(){
  ts = ((tableStatistics *) malloc (sizeof(tableStatistics)));
  if(ts == NULL){
    return -1;
  }
  else{
    return 1;
  }
}


int bufferdatamemory(){
  bd = ((bufferdata *) malloc (sizeof(bufferdata)));
  if(bd == NULL){
    return -1;
  }
  else{
    return 1;
  }
}

int schemamemory(){
sMem= (Schema*) malloc(sizeof(Schema));
if(sMem == NULL){
  return -1;
}
else{
  return 1;
}
}

int initialiseSchemaMemory(){
  sMem->attrNames= (char**) malloc(sizeof(char*)*tableAttributeCount);
  if(sMem->attrNames == NULL){
    return -1;
  }
  else{
  sMem->dataTypes= (DataType*) malloc(sizeof(int)*tableAttributeCount);
  if(sMem->dataTypes == NULL){
    return -1;
  }
  else{
  sMem->typeLength= (int*) malloc(sizeof(int)*tableAttributeCount);
  if(sMem->typeLength == NULL){
    return -1;
  }
  else{
    return 1;
  }
}}
}

int allocateScannedMemory(){
  scan_Ptr = (scanner*) malloc(sizeof(scanner));
  if(scan_Ptr == NULL){
    return -1;
  }
}

int getNextFreeSpace(char *r_Ptr, int sizeOfTuple)
{
if(r_Ptr != NULL && sizeOfTuple >0){
int i,j = 0;
int numOfSlotsInABlock = floor(PAGE_SIZE/sizeOfTuple);
for(i=0;i<numOfSlotsInABlock;i++){
  int index =i * sizeOfTuple;
  char a =r_Ptr[index];
  if(r_Ptr[index] != '#'){
    j=1;
  }
  switch(j){
    case 1:
    return i;
    default:
    break;
  }
}
return -1;
}
else{
  return RC_RM_NULL_PARAMETER_ERROR_FROM_getNextFreeSpace;
}
}

int attributesinit(Schema *schema, int attrNum, int *final, int type)
{
  if(schema != NULL && type > 0 && final !=NULL && attrNum >=0 ){
  attributeOffset=1;
  int pos = 0;
  if (type ==1){
   for(pos = 0; pos < attrNum;pos++)
   if(schema->dataTypes[pos]==DT_INT){
    attributeOffset = attributeOffset + sizeof(int);
   }
   else if(schema->dataTypes[pos]==DT_STRING){
    attributeOffset = attributeOffset + schema->typeLength[pos];
   }
   else{
    return RC_OK;
   }
  *final = attributeOffset;
  return RC_OK;
}
else if(type ==2){
  attributeOffset=1;
  pos = 0;
  for(pos = 0; pos < attrNum;pos++){
  if(schema->dataTypes[pos]==DT_INT){
   attributeOffset = attributeOffset + sizeof(int);
  }
  else if(schema->dataTypes[pos]==DT_STRING){
   attributeOffset = attributeOffset + schema->typeLength[pos];
  }
  else if(schema->dataTypes[pos]==DT_FLOAT){
   attributeOffset = attributeOffset + sizeof(float);
  }
  else{
   return RC_OK;
  }
  *final = attributeOffset;
}
  return RC_OK;
}
}
else{
  return RC_RM_NULL_PARAMETER_ERROR_FROM_attributesinit;
}
}


RC initRecordManager (void *mgmtData)
{
  sizeOfTuple=-1;
  attributeOffset=-1;
  r_Ptr= NULL;
  bd=NULL;
  sMem=NULL;
  ts=NULL;
  tId=NULL;
  scan_Ptr=NULL;
return RC_OK;

}

RC shutdownRecordManager ()
{
int i=0;
if(bd !=NULL){
  free(bd);
}
else{
  i=1;
}
if(sMem !=NULL){
free(sMem);
}
else{
  i=1;
}
if(ts !=NULL){
free(ts);
}
else{
  i=1;
}
if(tId !=NULL){
free(tId);
}
else{
  i=1;
}
if(i==1){
  return RC_RM_MEMORY_LEAK;
}
return RC_OK;
}

char * incrementPointer(char *pointer, int incrementSize){
  if(pointer != NULL){
  while(incrementSize !=0){
  pointer =pointer+1;
  incrementSize--;
}
  return pointer;
}
else{
  return NULL;
}
}

void closeFile(SM_FileHandle *fHandle){
//close file logic needs to be implemented

}

char * allocateMemoryOfVariableSize(int size){
  if(size > 0){
  char *a = (char*)malloc(size);
  return a;
}
else{
  return NULL;
}
}

Value * initializeValueTypeStruct(int count){
  if(count >0){
  Value *temp;
  temp = (Value *) calloc(count,sizeof(Value));
  if(temp == NULL){
    return NULL;
  }
  return temp;
}
else{
  return NULL;
}
}


RC createTable (char *name, Schema *schema)
{
int bdResult,tsResult,bpResult;
bdResult = bufferdatamemory();
if(bdResult ==-1){
  return RC_ERROR;
}
else{
tsResult=allocatememoryfortableStatistics();
if(tsResult == -1){
  return RC_RM_ERROR;
}
else{
bpResult = initBufferPool(&bd->poolMgmt, name, 60, RS_FIFO, NULL);
if(bpResult != RC_OK){
  return RC_RM_ERROR;
}
char firstpagedata[PAGE_SIZE]="";
char *firstpagedatapointer = firstpagedata;
*(int*)firstpagedatapointer = 0;
firstpagedatapointer = incrementPointer(firstpagedatapointer,4);
if(firstpagedatapointer == NULL){
  return RC_RM_ERROR;
}
*(int*)firstpagedatapointer = 1;
firstpagedatapointer = incrementPointer(firstpagedatapointer,4);
if(firstpagedatapointer == NULL){
  return RC_RM_ERROR;
}                                                                                                                                                          // end of 'while' loop
*(int*)firstpagedatapointer = tableAttributeCount;
firstpagedatapointer = incrementPointer(firstpagedatapointer,4);
if(firstpagedatapointer == NULL){
  return RC_RM_ERROR;
}
int createResult,openResult,writeResult;
createResult= createPageFile(name);
if(createResult == RC_OK) {
openResult=openPageFile(name, &fileHandler);
if(openResult == RC_OK){
writeResult=  writeBlock(0, &fileHandler, firstpagedata);
if(writeResult ==RC_OK) {
  closeFile(&fileHandler);
return RC_OK;
}
else{
  return RC_ERROR;
}
}
else{
  return RC_ERROR;
}
}
else{
  return RC_ERROR;
}
}
}
}

RC openTable (RM_TableData *rel, char *name)
{
int memoryResult;
memoryResult= schemamemory();
if(memoryResult ==-1){
  return RC_RM_ERROR;
}
int openResult,pinpageResult;
openResult = openPageFile(rel->name, &fileHandler);
pinpageResult=pinPage(&bd->poolMgmt, &bd->pageHandler, 0);
if(pinpageResult != RC_OK){
  return RC_RM_ERROR;
}
pHandler = (char*) bd->pageHandler.data;
int tuplecount =*(int*)pHandler;
pHandler = incrementPointer(pHandler,4);
if(pHandler == NULL){
  return RC_RM_ERROR;
}
int emptyslotsinblock =*(int*)pHandler;
ts->emptySlots= emptyslotsinblock;
pHandler = incrementPointer(pHandler,4);
if(pHandler == NULL){
  return RC_RM_ERROR;
}
int numberofattributes =*(int*)pHandler;
pHandler = incrementPointer(pHandler,4);
if(pHandler == NULL){
  return RC_RM_ERROR;
}
int result= initialiseSchemaMemory();
if(result !=1){
  return RC_ERROR;
}

sMem->attrNames[0]=allocateMemoryOfVariableSize(5);
sMem->attrNames[1]=allocateMemoryOfVariableSize(5);
sMem->attrNames[2]=allocateMemoryOfVariableSize(5);

sMem->dataTypes[0]= 0;
sMem->dataTypes[1]= 0;
sMem->dataTypes[2]= 0;

rel->schema = sMem;
int unpinResult;
unpinResult = unpinPage(&bd->poolMgmt, &bd->pageHandler);
if(unpinResult ==RC_OK){
  return RC_OK;
}
}


RC closeTable (RM_TableData *rel)
{
if(rel!= NULL){
int closeresult, shutdownResult;
shutdownResult= shutdownBufferPool(&bd->poolMgmt);
if(shutdownResult == RC_OK){
  closeresult= closePageFile(&fileHandler);
  if(closeresult == RC_OK){
  free(sMem);
  free(bd);
  return RC_OK;
}
else{
  return RC_ERROR;
}
}
else{
  return RC_OK;
}
}
else{
  return RC_RM_NULL_PARAMETER_ERROR_FROM_closeTable;
}
}


RC deleteTable (char *name)
{
if(name != NULL){
int closeresult, shutdownResult,deleteResult;
shutdownResult= shutdownBufferPool(&bd->poolMgmt);
if(shutdownResult == RC_OK){
  closeresult= closePageFile(&fileHandler);
  if(closeresult == RC_OK){
  deleteResult = destroyPageFile(name);
  if(deleteResult !=RC_OK){
    return RC_ERROR;
  }
  free(sMem);
  free(bd);
  return RC_OK;
}
else{
  return RC_ERROR;
}
}
else{
  return RC_OK;
}
}
else{
  return RC_RM_NULL_PARAMETER_ERROR_FROM_deleteTable;
}
}

int getNumTuples (RM_TableData *rel)
{
 return(ts->tuplesCount);
}


RC insertRecord (RM_TableData *rel, Record *record)
{

if(rel == NULL && record == NULL){
  return RC_RM_NULL_PARAMETER_ERROR_FROM_insertRecord;
}

int nextavailablefreeslot=-1;
tId = &record->id;
int unpinPageResult;
int sizeOfTuple = 1;
int k;
for(k=0;k<tableAttributeCount;k++){
  switch(rel->schema->dataTypes[k]){
    case 0:
    sizeOfTuple = sizeOfTuple + sizeof(int);
    break;
    case 1:
    sizeOfTuple = sizeOfTuple + rel->schema->typeLength[k];
    break;
    case 2:
    sizeOfTuple = sizeOfTuple + sizeof(int);
    break;
    case 3:
    sizeOfTuple = sizeOfTuple + sizeof(int);
    break;
  }
}
int tempcount =tableAttributeCount;
while(tempcount >0){
  int sizecount = sizeof(int);
  tempcount--;
}
sizeOfTuple = getRecordSize(rel->schema);
if(sizeOfTuple <= 0){
return RC_RM_SIZE_ZERO_ERROR;
}
tId->page = ts->emptySlots;
int pinPageResult;
pinPageResult=pinPage(&bd->poolMgmt, &bd->pageHandler, tId->page);
if(pinPageResult != RC_OK){
  return RC_ERROR;
}
r_Ptr = (&bd->pageHandler)->data;
int i,j = 0;
int numOfSlotsInABlock = floor(PAGE_SIZE/sizeOfTuple);
if(numOfSlotsInABlock < 0){
  return RC_ERROR;
}
for(i=0;i<numOfSlotsInABlock;i++){
  int index =i * sizeOfTuple;
  char a =r_Ptr[index];
  if(r_Ptr[index] != '#'){
    j=1;
  }
  switch(j){
    case 1:
    nextavailablefreeslot= i;
    default:
    break;
  }
}

nextavailablefreeslot = getNextFreeSpace(r_Ptr, sizeOfTuple);
tId->slot = nextavailablefreeslot;
if(nextavailablefreeslot < 0){
  unpinPageResult=unpinPage(&bd->poolMgmt, &bd->pageHandler);
  if(unpinPageResult != RC_OK){
    return RC_ERROR;
  }
  else{
  while((nextavailablefreeslot)<0)
  {
  tId->page=tId->page+1;
  pinPageResult= pinPage(&bd->poolMgmt, &bd->pageHandler, tId->page);
  if(pinPageResult != RC_OK){
    return RC_ERROR;
  }
  r_Ptr = bd->pageHandler.data;
  int i,j = 0;
  numOfSlotsInABlock = floor(PAGE_SIZE/sizeOfTuple);
  for(i=0;i<numOfSlotsInABlock;i++){
    int index =i * sizeOfTuple;
    char a =r_Ptr[index];
    if(r_Ptr[index] != '#'){
      j=1;
    }
    switch(j){
      case 1:
      nextavailablefreeslot= i;
      default:
      break;
    }
  }
  nextavailablefreeslot=getNextFreeSpace(r_Ptr, sizeOfTuple);
  if(nextavailablefreeslot ==-1){
    return RC_RM_ERROR;
  }
  tId->slot = nextavailablefreeslot;
  ts->emptySlots =tId->page;
  }
}
}


int offset =tId->slot * sizeOfTuple;
r_Ptr = r_Ptr + offset;
*r_Ptr = '#';
r_Ptr=r_Ptr+1;
char *temp;
temp = memcpy(r_Ptr, record->data+1, sizeOfTuple);
if(temp == NULL){
 return RC_RM_MEMORY_LEAK;
}
int markDirtyResult;
markDirtyResult=markDirty(&bd->poolMgmt,&bd->pageHandler);
ts->tuplesCount++;
if(markDirtyResult != RC_OK){
  return RC_ERROR;
}
else{
unpinPageResult=unpinPage(&bd->poolMgmt, &bd->pageHandler);
if(unpinPageResult != RC_OK){
  return RC_ERROR;
}
else{
pinPageResult=pinPage(&bd->poolMgmt, &bd->pageHandler, 1);
if(pinPageResult != RC_OK){
  return RC_ERROR;
}
else{
  return RC_OK;
}
}
}
}

RC deleteRecord (RM_TableData *rel, RID id)
{
  if(rel == NULL && id.slot < 0 && id.page <0){
    return RC_RM_NULL_PARAMETER_ERROR_FROM_deleteRecord;
  }
  char *name;
  int openfileResult,pinpageResult,markdirtyResult,forcepageResult,unpinpageresult;
  char *memsetResult;
  openfileResult=    openPageFile(rel->name, &fileHandler);
  if(openfileResult != RC_OK){
    return RC_OK;
  }
  else{
      pinpageResult=pinPage(&bd->poolMgmt, &bd->pageHandler,id.page);
      if(pinpageResult != RC_OK){
        return RC_ERROR;
      }
      else{
      char *data;
      data = (&bd->pageHandler)->data;

      int sizeOfTuple = 1;
      int k;
      for(k=0;k<tableAttributeCount;k++){
        switch(rel->schema->dataTypes[k]){
          case 0:
          sizeOfTuple = sizeOfTuple + sizeof(int);
          break;
          case 1:
          sizeOfTuple = sizeOfTuple + rel->schema->typeLength[k];
          break;
          case 2:
          sizeOfTuple = sizeOfTuple + sizeof(int);
          break;
          case 3:
          sizeOfTuple = sizeOfTuple + sizeof(int);
          break;
        }
      }
      //sizeOfTuple = getRecordSize(rel->schema);
      data = data + (id.slot * sizeOfTuple);
      *data ='0';
      memsetResult = memsetResult = memset(data,0, sizeOfTuple);
      if(memsetResult != data){
        return RC_ERROR;
      }
      else{
      markdirtyResult = markDirty(&bd->poolMgmt, &bd->pageHandler);
      ts->tuplesCount--;
      if(markdirtyResult != RC_OK){
        return RC_ERROR;
      }
      else{
      forcepageResult = forcePage(&bd->poolMgmt, &bd->pageHandler);
      if(forcepageResult != RC_OK){
        return RC_ERROR;
      }
      else{
      unpinpageresult =unpinPage(&bd->poolMgmt, &bd->pageHandler);
      if(unpinpageresult != RC_OK){
      }
        return RC_ERROR;
      else{
      return RC_OK;
    }
    }
    }
    }
    }
    }
}

RC updateRecord (RM_TableData *rel, Record *record)
{

if(rel == NULL && record == NULL){
  return RC_RM_NULL_PARAMETER_ERROR_FROM_updateRecord;
}

int emptySlotChar =1;
int offset, recordsize,markdirtyResult,unpinpageResult,pinpageResult;
pinpageResult=pinPage(&bd->poolMgmt, &bd->pageHandler,(&record->id)->page);
if(pinpageResult != RC_OK){
  return RC_ERROR;
}
else{
char *datapointer, *memmovepointer,*newdataAfterIsEmptySlot,forcepageResult;
datapointer = bd->pageHandler.data;

int sizeOfTuple = 1;
int k;
for(k=0;k<tableAttributeCount;k++){
  switch(rel->schema->dataTypes[k]){
    case 0:
    sizeOfTuple = sizeOfTuple + sizeof(int);
    break;
    case 1:
    sizeOfTuple = sizeOfTuple + rel->schema->typeLength[k];
    break;
    case 2:
    sizeOfTuple = sizeOfTuple + sizeof(int);
    break;
    case 3:
    sizeOfTuple = sizeOfTuple + sizeof(int);
    break;
  }
}

recordsize =sizeOfTuple;
recordsize = getRecordSize(rel->schema);
if(recordsize == -1 ){
  return RC_RM_SIZE_ZERO_ERROR;
}
offset =((&record->id)->slot * recordsize);
datapointer = datapointer + offset;
datapointer=datapointer+emptySlotChar;
newdataAfterIsEmptySlot=record->data + emptySlotChar;
memmovepointer = memmove(datapointer,newdataAfterIsEmptySlot, recordsize-1);
if(memmovepointer != datapointer){
  return RC_ERROR;
}
else{
markdirtyResult=markDirty(&bd->poolMgmt, &bd->pageHandler);
if(markdirtyResult != RC_OK){
  return RC_ERROR;
}
else{
unpinpageResult=unpinPage(&bd->poolMgmt, &bd->pageHandler);
if(unpinpageResult != RC_OK){
  return RC_ERROR;
}
else{
return RC_OK;
}
}
}
}
}


int algo(RM_TableData *rel, RID id, Record *record)
{
if(rel == NULL || record == NULL || id.slot < 0 ||  id.page <0){
  return RC_RM_NULL_PARAMETER_ERROR_FROM_recordSize;
}
char *ptrs;
Schema *ptr=rel->schema;
int totalbytes = getRecordSize(ptr);
int length = 1;
int count =0;
int i;
for(i=0;i<tableAttributeCount;i++){
  switch(rel->schema->dataTypes[i]){
    case 0:
    length = length + sizeof(int);
    count ++;
    break;
    case 1:
    length = length + rel->schema->typeLength[i];
    count ++;
    break;
    case 2:
    length = length + sizeof(int);
    count ++;
    break;
    case 3:
    length = length + sizeof(int);
    count ++;
    break;
    default:
    return -1;
    break;
  }
}
if(totalbytes <0){
  return RC_RM_SIZE_ZERO_ERROR;
}
ptrs = bd->pageHandler.data + ((id.slot)*totalbytes);
r_Ptr = (record->data) + 1;
char *temp;
if(totalbytes <0){
  return RC_RM_SIZE_ZERO_ERROR;
}
temp = memcpy(r_Ptr,ptrs + 1,totalbytes - 1);
if(temp == NULL){
  return RC_RM_MEMORY_LEAK;
}
return RC_OK;
}

RC getRecord (RM_TableData *rel, RID id, Record *record)
{
  if(rel == NULL || record == NULL || id.slot < 0 ||  id.page <0){
    return RC_RM_NULL_PARAMETER_ERROR_FROM_getRecord;
  }
pinPage(&bd->poolMgmt, &bd->pageHandler, id.page);
algo(rel,id,record);
int result = unpinPage(&bd->poolMgmt, &bd->pageHandler);
return RC_OK;
}



RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
  if(rel == NULL || scan == NULL || cond == NULL){
    return RC_RM_NULL_PARAMETER_ERROR_FROM_scanRecord;
  }
  int opentableResult, memoryAllocationResult;
  memoryAllocationResult= allocateScannedMemory();
  if(memoryAllocationResult ==-1){
    return RC_RM_MEMORY_LEAK;
  }
  scan_Ptr->scanClause = cond;
  (&scan_Ptr->tId)->page= 1;
  (&scan_Ptr->tId)->slot= 0;
  scan->mgmtData = scan_Ptr;
  scan->rel= rel;
  char *name;
  opentableResult =openTable(rel, name);
  if(opentableResult != RC_OK){
    return RC_ERROR;
  }
  else{
  return RC_OK;
}
}

int getNumberofTuples(RM_ScanHandle *scan){
  if(scan != NULL){
  int temp;
  temp = getRecordSize(scan->rel->schema);
  if(temp >0){
  temp = floor(PAGE_SIZE/sizeOfTuple);
  return temp;
}
else{
  return -1;
}
}
else{
  return RC_RM_NULL_PARAMETER_ERROR_FROM_getNumberofTuples;
}
}

RC next (RM_ScanHandle *scan, Record *record)
{
if(scan == NULL && record == NULL ){
  return RC_RM_NULL_PARAMETER_ERROR_FROM_next;
}
int i=0;
char *recordpointer;
int unpinpagesResult;
recordpointer = record->data;
if(*recordpointer == '#' ){
recordpointer = recordpointer + 1;
}
recordpointer = recordpointer + 1;
sizeOfTuple = getRecordSize(scan->rel->schema);
if(sizeOfTuple <0 ){
  return RC_RM_SIZE_ZERO_ERROR;
}
int sizeOfTuple = 1;
int k;
for(k=0;k<tableAttributeCount;k++){
  switch(scan->rel->schema->dataTypes[k]){
    case 0:
    sizeOfTuple = sizeOfTuple + sizeof(int);
    break;
    case 1:
    sizeOfTuple = sizeOfTuple + scan->rel->schema->typeLength[k];
    break;
    case 2:
    sizeOfTuple = sizeOfTuple + sizeof(int);
    break;
    case 3:
    sizeOfTuple = sizeOfTuple + sizeof(int);
    break;
  }
}

int numOfSlotsInABlock = getNumberofTuples(scan);
if(numOfSlotsInABlock ==-1){
  return RC_RM_ERROR;
}
Value *v=initializeValueTypeStruct(1);
if (v == NULL){
  return RC_RM_NULL_RETURN_PARAMETER_ERROR_FROM_next;
}
(&scan_Ptr->tId)->slot++;

for(i=0;i <= ts->tuplesCount;i++)
{
if(numOfSlotsInABlock == (&scan_Ptr->tId)->slot){
    (&scan_Ptr->tId)->page++;
    (&scan_Ptr->tId)->slot=1;
}
int pinpageResult;
pinpageResult = pinPage(&bd->poolMgmt, &bd->pageHandler, (&scan_Ptr->tId)->page);
if(pinpageResult != RC_OK){
  return RC_ERROR;
}
else{
char *pagedatapointer;
pagedatapointer = (&bd->pageHandler)->data;
pagedatapointer = pagedatapointer + ((&scan_Ptr->tId)->slot * sizeOfTuple);
if(*pagedatapointer == '#' ){
pagedatapointer = pagedatapointer + 1;
}
int tuplesCount=10;//testing
int conting=tuplesCount;
while(conting >0){
conting --;
(&record->id)->page =1;
}
(&record->id)->page = (&scan_Ptr->tId)->page;
(&record->id)->slot = (&scan_Ptr->tId)->slot;
char *memmovpointer;
memmovpointer = memmove(recordpointer,pagedatapointer,sizeOfTuple-1);
if(memmovpointer != recordpointer){
  unpinPage(&bd->poolMgmt, &bd->pageHandler);
  return RC_ERROR;
}
else{
int evalResult;
evalResult = evalExpr(record,(scan->rel)->schema, scan_Ptr->scanClause, &v);
if(evalResult != RC_OK){
  unpinpagesResult=unpinPage(&bd->poolMgmt, &bd->pageHandler);
  if(unpinpagesResult != RC_OK){
    return RC_RM_ERROR;
  }
  return RC_ERROR;
}
else{
(&scan_Ptr->tId)->slot++;
if(((&v->v)->boolV == 1))
{
unpinpagesResult=unpinPage(&bd->poolMgmt, &bd->pageHandler);
if(unpinpagesResult != RC_OK){
  return RC_RM_ERROR;
}
return RC_OK;
}
unpinpagesResult=unpinPage(&bd->poolMgmt, &bd->pageHandler);
if(unpinpagesResult != RC_OK){
  return RC_RM_ERROR;
}
}
}
}
}

unpinPage(&bd->poolMgmt, &bd->pageHandler);
return RC_RM_NO_MORE_TUPLES;
}


RC closeScan (RM_ScanHandle *scan)
{
  if(scan == NULL){
    return RC_RM_NULL_RETURN_PARAMETER_ERROR_FROM_closescan;
  }
  else{
scan->mgmtData= NULL;
scan->rel='\0';
return RC_OK;
}
}


int getRecordSize (Schema *schema)
{
if(schema == NULL){
  return -1;
}
int length = 1;
int count =0;
int i;
for(i=0;i<tableAttributeCount;i++){
  switch(schema->dataTypes[i]){
    case 0:
    length = length + sizeof(int);
    count ++;
    break;
    case 1:
    length = length + schema->typeLength[i];
    count ++;
    break;
    case 2:
    length = length + sizeof(int);
    count ++;
    break;
    case 3:
    length = length + sizeof(int);
    count ++;
    break;
    default:
    return -1;
    break;
  }
}
return length;
}

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
if(numAttr !=0 && attrNames !=NULL && dataTypes !=NULL && keySize >=0  ){
sMem= (Schema*) malloc(sizeof(Schema));
if(sMem == NULL){
  return NULL;
}
else{
sMem->attrNames= (char**) malloc(sizeof(char*)*tableAttributeCount);
if(sMem->attrNames == NULL){
  return NULL;
}
else{
sMem->attrNames = attrNames;
sMem->dataTypes= (DataType*) malloc(sizeof(int)*tableAttributeCount);
if(sMem->dataTypes == NULL){
  return NULL;
}
else{
sMem->dataTypes =dataTypes;
sMem->typeLength= (int*) malloc(sizeof(int)*tableAttributeCount);
if(sMem->typeLength == NULL){
  return NULL;
}
else{
sMem->numAttr = numAttr;
sMem->typeLength = typeLength;
return sMem;
}}}}
}
else{
  return NULL;
}
}

RC freeSchema (Schema *schema)
{
  if(schema != NULL){
    free(schema);
}
else{
schema = NULL;
return RC_OK;
}
}


RC createRecord(Record **record, Schema *schema)
{
if(record != NULL && schema != NULL){
int *size = schema->typeLength;
int val=0;
int pos;
char *ptr;
if(schema->dataTypes == NULL){
  return RC_RM_ERROR;
}
pos=*(schema->dataTypes);
switch(pos){
case 0:
   sizeOfTuple=sizeOfTuple + sizeof(int);
   break;
case 1:
   sizeOfTuple = sizeOfTuple + (*(size));
   break;
case 2:
    sizeOfTuple = sizeOfTuple + sizeof(float);
    break;
case 3:
    sizeOfTuple = sizeOfTuple + sizeof(_Bool);
    break;
default:
    return RC_INVALID_DATATYPE;
}
ptr = (char *)calloc(1,sizeof(int));
if(ptr == NULL){
  return RC_RM_ERROR;
}
*record = (Record *)calloc(1,sizeof(int));
if(record == NULL){
  return RC_RM_ERROR;
}
record[0]->data=ptr;
return RC_OK;
}
else{
  return RC_RM_NULL_RETURN_PARAMETER_ERROR_FROM_createRecord;
}

}

RC freeRecord (Record *record)
{
if(record !=NULL){
  free(record);
}
else{
record =NULL;
return RC_OK;
}
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **r_Ptr)
{

if(record != NULL && schema != NULL && attrNum >=0 && r_Ptr != NULL){
  if(attrNum == 1)
  {
  schema->dataTypes[attrNum] = 1;
  }
char *ptr;
float fValue;
bool bValue;
int stringLength;
if(schema == NULL){
  return RC_RM_Schema_Null;
}
attributeOffset = 0;
int attributeinitResult;
attributeinitResult = attributesinit(schema, attrNum, &attributeOffset, 1);
if(attributeinitResult != RC_OK){
  return RC_RM_getAttr_not_initialised;
}
ptr =NULL;
ptr = record->data;
ptr = ptr + attributeOffset;
int val;
Value *v;
v = (Value *) calloc(1,sizeof(Value));
if(v == NULL){
  return RC_RM_ERROR;
}
char *temp;
if(schema->dataTypes[attrNum] == -1){
  return RC_RM_ERROR;
}
switch(schema->dataTypes[attrNum]){
case 0:

temp= memcpy(&val,ptr, sizeof(int));
if(temp == NULL){
  return RC_RM_ERROR;
}
v->v.intV = val;
v->dt = 0;
break;
case 1:
v->dt = 1;
stringLength = schema->typeLength[attrNum];
v->v.stringV = (char *) calloc(1,sizeof(char));
if(v->v.stringV == NULL){
  return RC_RM_ERROR;
}
temp = strncpy(v->v.stringV, ptr, stringLength);
if(temp == NULL){
  return RC_RM_ERROR;
}
v->v.stringV[stringLength] = '\0';
break;
default:
    return RC_INVALID_DATATYPE;
}
*r_Ptr = v;
return RC_OK;
}
else{
  return RC_RM_NULL_RETURN_PARAMETER_ERROR_FROM_getAttr;
}
}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *r_Ptr)
{
if(record != NULL && schema != NULL && attrNum >= 0 && r_Ptr != NULL){
float fValue;
bool bValue;
char *data;
char *ptr;
char *temp;
attributeOffset = 0;
int result;
if(schema == NULL){
  return RC_RM_Schema_Null;
}
result= attributesinit(schema, attrNum, &attributeOffset , 2);
if(result != RC_OK){
  return RC_RM_ERROR;
}
ptr = record->data;
ptr = ptr + attributeOffset;
if(schema->dataTypes[attrNum] == -1){
  return RC_RM_ERROR;
}
switch(schema->dataTypes[attrNum]){
case 0:
    *(int *)ptr = r_Ptr->v.intV;
    break;
case 1:
sizeOfTuple = schema->typeLength[attrNum];
data = (char *) calloc(1,5);
if(data == NULL){
  return RC_RM_ERROR;
}
temp = strncpy(data, r_Ptr->v.stringV,5);
if(temp == NULL){
  return RC_RM_ERROR;
}
data[sizeOfTuple] = -1;
temp = strncpy(ptr, data, 5);
if(temp == NULL){
  return RC_RM_ERROR;
}
    break;
default:
  return RC_INVALID_DATATYPE;
}
return RC_OK;
}
else{
  return RC_RM_setAttr_not_initialised;
}
}
