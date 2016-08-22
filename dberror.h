#ifndef DBERROR_H
#define DBERROR_H

#include "stdio.h"

/* module wide constants */
#define PAGE_SIZE 4096

/* return code definitions */
typedef int RC;

#define RC_OK 0
#define RC_FILE_NOT_FOUND 1
#define RC_FILE_HANDLE_NOT_INIT 2
#define RC_WRITE_FAILED 3
#define RC_READ_NON_EXISTING_PAGE 4

#define RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE 200
#define RC_RM_EXPR_RESULT_IS_NOT_BOOLEAN 201
#define RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN 202
#define RC_RM_NO_MORE_TUPLES 203
#define RC_RM_NO_PRINT_FOR_DATATYPE 204
#define RC_RM_UNKOWN_DATATYPE 205

#define RC_IM_KEY_NOT_FOUND 300
#define RC_IM_KEY_ALREADY_EXISTS 301
#define RC_IM_N_TO_LAGE 302
#define RC_IM_NO_MORE_ENTRIES 303

#define RC_MEMORY_ERROR 501
#define RC_WRITE_ERROR 502
#define RC_NULL_ERROR 503
#define RC_READ_ERROR 504
#define RC_WRITE_NON_EXISTING_PAGE 505
#define RC_SEEK_FAILURE 506

#define RC_PINNENPAGES_IN_BUFFER 601
#define RC_INPUT_PARAMETER_NULL_IN_INITIALIZE 602
#define RC_INPUT_PARAMETER_NULL_IN_SHUTDOWNBUFFERPOOL 603
#define RC_FORCE_POOL_FAILED 604
#define RC_FORCE_POOL_NULL_PARAMETER 605
#define RC_WRITE_DIRTY_PAGE_NULL_PARAMETER 606
#define RC_UNPIN_PAGE_NULL_PARAMETER 607
#define RC_FORCE_PAGE_NULL_PARAMETER 608
#define RC_WRITE_DIRTY_PAGE_ERROR 609
#define RC_FIFO_ALGO_NULL_PARAMETER_ERROR 610
#define RC_LRU_ALGO_NULL_PARAMETER_ERROR 611
#define RC_NOT_AVAIL_IN_BUFFER_NULL_PARAMETER 612
#define RC_FIFO_LOGIC_NULL_PARAMETER 613
#define RC_LRU_LOGIC_NULL_PARAMETER 614
#define RC_PIN_PAGE_NULL_PARAMETER 615
#define RC_WRITE_COUNT_NULL_ERROR 616
#define RC_READ_COUNT_NULL_ERROR 617
#define RC_CLIENT_COUNT_NULL_ERROR 618
#define RC_GENERAL_ERROR 619
#define RC_ERROR 620
#define RC_ALGORITHM_NOT_IMPLEMENTED 621
#define RC_INVALID_DATATYPE 622


#define RC_RM_ERROR 699
#define RC_RM_NULL_PARAMETER_ERROR_FROM_getNextFreeSpace 700
#define RC_RM_NULL_PARAMETER_ERROR_FROM_attributesinit 701
#define RC_RM_MEMORY_LEAK 702
#define RC_RM_MEMORY_NOT_ALLOCATED_ERROR_IN_incrementPointer 703
#define RC_RM_NULL_PARAMETER_ERROR_FROM_closeTable 704
#define RC_RM_NULL_PARAMETER_ERROR_FROM_deleteTable 705
#define RC_RM_NULL_PARAMETER_ERROR_FROM_insertRecord 707
#define RC_RM_SIZE_ZERO_ERROR 708
#define RC_RM_NULL_PARAMETER_ERROR_FROM_deleteRecord 708
#define RC_RM_NULL_PARAMETER_ERROR_FROM_updateRecord 709
#define RC_RM_NULL_PARAMETER_ERROR_FROM_recordSize 710
#define RC_RM_NULL_PARAMETER_ERROR_FROM_getRecord 711
#define RC_RM_NULL_PARAMETER_ERROR_FROM_scanRecord 712
#define RC_RM_NULL_PARAMETER_ERROR_FROM_getNumberofTuples 713
#define RC_RM_NULL_PARAMETER_ERROR_FROM_next 714
#define RC_RM_NULL_RETURN_PARAMETER_ERROR_FROM_next 714
#define RC_RM_NULL_RETURN_PARAMETER_ERROR_FROM_createRecord 715
#define RC_RM_NULL_RETURN_PARAMETER_ERROR_FROM_closescan 716
#define RC_RM_NULL_RETURN_PARAMETER_ERROR_FROM_getAttr 718
#define RC_RM_getAttr_not_initialised 719
#define RC_RM_setAttr_not_initialised 720
#define RC_RM_Schema_Null 721


/* holder for error messages */
extern char *RC_message;

/* print a message to standard out describing the error */
extern void printError (RC error);
extern char *errorMessage (RC error);

#define THROW(rc,message) \
  do {			  \
    RC_message=message;	  \
    return rc;		  \
  } while (0)		  \

// check the return code and exit if it is an error
#define CHECK(code)							\
  do {									\
    int rc_internal = (code);						\
    if (rc_internal != RC_OK)						\
      {									\
	char *message = errorMessage(rc_internal);			\
	printf("[%s-L%i-%s] ERROR: Operation returned error: %s\n",__FILE__, __LINE__, __TIME__, message); \
	free(message);							\
	exit(1);							\
      }									\
  } while(0);


#endif
