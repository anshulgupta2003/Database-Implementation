a1:test_assign3_1.o buffer_mgr.o buffer_mgr_stat.o dberror.o storage_mgr.o rm_serializer.o record_mgr.o expr.o
	gcc test_assign3_1.o buffer_mgr.o buffer_mgr_stat.o dberror.o storage_mgr.o rm_serializer.o record_mgr.o expr.o   -lm -o output
dberror.o:dberror.c dberror.h
	gcc -c dberror.c
storage_mgr.o:storage_mgr.c storage_mgr.h dberror.h
	gcc -c storage_mgr.c
test_assign3_1.o:test_assign3_1.c test_helper.h dberror.h storage_mgr.h buffer_mgr_stat.h buffer_mgr.h expr.h tables.h record_mgr.h
	gcc -c test_assign3_1.c
buffer_mgr.o:buffer_mgr.c storage_mgr.h buffer_mgr.h
	gcc -c buffer_mgr.c
buffer_mgr_stat.o:buffer_mgr_stat.c buffer_mgr_stat.h buffer_mgr.h
	gcc -c 	buffer_mgr_stat.c
rm_serializer.o:rm_serializer.c dberror.h tables.h record_mgr.h
	gcc -c rm_serializer.c
record_mgr.o:record_mgr.c record_mgr.h buffer_mgr.h storage_mgr.h dt.h
	gcc -c record_mgr.c
expr.o:expr.c dberror.h record_mgr.h expr.h tables.h
	gcc -c expr.c

