Authors:
Course ID : CS525
Group 6:
1. Subakar Sundarababu Elango (A20345012)
2. Anshul Gupta (A20369831)




Motivation-To implement a Record Manager.

Goals:-
1.The record manager handles tables with fixed schema.
2.The Client can insert records,delete records,update records and scan through the records in a table.
3.A scan is basically a execution of statement where it return records which match a search condition.
4.The table here is stored in a page file and record manager can access the pages through buffer manager.

Data Structues used in this Assignment

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

typedef struct scanRecord
{
    Expr *scanCondition;
    RID tId;
} scanRecord;

BufferData is the data structure used for accessing buffer manager functions.
TableStatistics is the data structure used for accessing record manager functions like number of rows in a table and slots in a block where a tuple can be inserted.
ScanRecord is the data structure used for scanning through a table with the help of condition specified and tuple id for a particular record to be accessed.

Functions Implemented

1.InitRecordManager-Initializes the record manager and return error code if not initialized.

2.ShutDownRecordManager-It shuts down the record manager.

3.CreateTable-The function creates a new table.It uses the function createpagefile from storage manager to write into file and transfer that onto the disk.

4.OpenTable-It uses the function openpagefile from storage manager and pin page to make any changes to table data.

5.CloseTable-It uses the function closePageFile from storage manager to close the file.

6.DeleteTable-It deletes a table using storage manager function called destroy page file from storage manager.

7.GetNumOfTuples-The function returns number of rows in a table.

8.Insert Record-
a-The function inserts a row into the table.
b-Each attribute should be of particular datatype.
c-After the record is insert that block is marked dirty and sent to disk.
d-After that the page is unpinned.

9.DeleteRecord-
a-The function deletes a row in a table.
b-Then it uses force page to write dirty page into disk.
c-It reduces the tuple count.
d-Then upins the page.

10.UpdateRecord-
a.The function updates a tuple in a table.
b.It the pins the page which contains the table.
c.Then using function memmove the changes are made and that page is then marked dirty and moved into memory.
d.Then the page is unpinned from memory.

11.GetRecords-
a.The function is responsible for getting a tuple from a table.
b.The page is pinned into memory.
c.Then the particular record is located on the page.
d.Then the page is unpinned from memory.

12.StartScan-
a.It scans a table for a particular record as per the condition mentioned in expr and gets a corresponding tuple from table.
b.Then its open page file

13.Next-
a.The function takes the scan pointer to the next tuple in a table.It retrieves the tuples which satisfies the condition.
b.It retieves all tuples if NULL is passed in the condition.

14.CloseScan-
a.It stops the scanning process.
b.All resources are cleaned up and scan is closed.

15.GetRecordSize-
a.It calculates the total number of bytes in a tuple.

16.CreateSchema-
a.It creates a new schema for the table.

17.FreeSchema-
a.It frees the memory variables used in creation of schema.

18.CreateRecord-
a.It allocates memory for each attribute of a record.
b.It creates a structure of record.

19.FreeRecord-
a.It frees all memory variables of a record.

20.GetAttribute
a.It returns the each field of a record.

21.SetAttribute
a.It sets the each field of a record.




