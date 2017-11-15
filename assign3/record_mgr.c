#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include <ctype.h>


typedef struct DatabaseManager {
        BM_PageHandle page;
        BM_BufferPool buffer;
        int numRows;
        int maxRecords;
        int freePage;
        int snumAttr;
        char sattrName[5];
        int sdataTypes[5];
        int stypeLength[5];


}DatabaseManager;

DatabaseManager *dbm;
int attribute_size = 10;
int pNo = 0;
int getFreeSlot(int,char *);

extern RC initRecordManager(void *mgmtData){
        initStorageManager();
        return RC_OK;
}

extern RC shutdownRecordManager (){
        shutdownBufferPool(&dbm->buffer);
        dbm = NULL;

        free(dbm);
        return RC_OK;
}


extern RC createTable (char *name, Schema *schema){

        dbm =(DatabaseManager *) malloc(sizeof(DatabaseManager));
        int result;

        if(initBufferPool(&dbm->buffer,name,40,RS_FIFO,NULL) != RC_OK) {
                return RC_ERROR;
        }

        char data[PAGE_SIZE];
        char *dataPage;

        for(int i =0; i<PAGE_SIZE; i++) {
                data[i] = '-';
        }

        int index = 0;
        data[index] = '1';
        index = index + sizeof(int);
        dbm->freePage = 1;

        //rows
        data[index] = '0';
        index = index + sizeof(int);
        dbm->numRows = 0;



        char temp[20];
        sprintf(temp,"%d",schema->numAttr);
        data[index]= *temp;
        index = index + sizeof(int);
        dbm->snumAttr = schema->numAttr;


        int i,len,count = 3;


        for(i = 0; i < schema->numAttr; i++) {
                dataPage = &data[index];
                for(int j=0; j<strlen(schema->attrNames[i]); j++) {
                        data[index] = schema->attrNames[i][j];
                }
                char *temp = &dbm->sattrName[i];
                strncpy(temp,schema->attrNames[i],1);

                index = index + attribute_size;
                sprintf(temp,"%d",schema->dataTypes[i]);
                data[index] = *temp;

                index = index + sizeof(int);
                dbm->sdataTypes[i] = schema->dataTypes[i];

                char ts[20];
                sprintf(ts,"%d",schema->typeLength[i]);
                data[index] = *ts;
                index = index + sizeof(int);
                dbm->stypeLength[i] = schema->typeLength[i];



        }

        data[index] = '\0';
        SM_FileHandle fh;


        if((result =createPageFile(name)) != RC_OK)
                return result;
        if((result = openPageFile(name,&fh)) != RC_OK)
                return result;
        if((result = writeBlock(0, &fh, data)) != RC_OK)
                return result;


        if((result = closePageFile(&fh)!= RC_OK))
                return result;

        return RC_OK;
}

extern RC openTable (RM_TableData *rel, char *name){

        SM_PageHandle page;

        rel->name = name;
        rel->mgmtData = dbm;

        Schema *s = (Schema *)malloc(sizeof(Schema));
        s->numAttr = dbm->snumAttr;


        int count = s->numAttr;

        s->attrNames = (char **)malloc(count * sizeof(char *));
        s->dataTypes = malloc(count * sizeof(DataType));
        s->typeLength = malloc(count * sizeof(int));

        int i,len,c=3;


        for(i =0; i<3; i++) {

                s->attrNames[i] = (char *)malloc(attribute_size);
                char *temp = &dbm->sattrName[i];
                strncpy(s->attrNames[i],temp,1);

                s->dataTypes[i] = dbm->sdataTypes[i];

                s->typeLength[i] = dbm->stypeLength[i];

        }

        rel->schema = s;


        return RC_OK;

}

extern RC closeTable (RM_TableData *rel){
        DatabaseManager  *dbmanger = rel->mgmtData;
        forceFlushPool(&dbmanger->buffer);

        return RC_OK;
}

extern RC deleteTable (char *name){
        destroyPageFile(name);

        return RC_OK;
}

extern int getNumTuples (RM_TableData *rel){
        DatabaseManager *dbm = (DatabaseManager *)rel->mgmtData;

        return dbm->numRows;

}


extern RC insertRecord (RM_TableData *rel, Record *record){
        DatabaseManager *dbmanager = rel->mgmtData;
        char d[PAGE_SIZE];
        char *data,*location;

        int max_records = dbmanager->maxRecords;
        int numRows = getNumTuples(rel);
        int freeSlot,pageNumber;


        int record_size = getRecordSize(rel->schema);


        pageNumber = dbmanager->freePage;



        pinPage(&dbmanager->buffer,&dbmanager->page,pageNumber);


        int loc=0;
        RID *recordID = &record->id;
        data = dbmanager->page.data;

        recordID->page = pageNumber;

        freeSlot = dbmanager->numRows;
        recordID->slot = freeSlot;
        int flag = 0;

        if(numRows > (PAGE_SIZE/record_size)) {

                pageNumber++;

                dbmanager->freePage = dbmanager->freePage + 1;
                flag = 1;
                dbmanager->numRows = 0;
                unpinPage(&dbmanager->buffer,&dbmanager->page);
                recordID->page= pageNumber;

                recordID->slot = dbmanager->numRows;

                pinPage(&dbmanager->buffer,&dbmanager->page,recordID->page);

                data =  dbmanager->page.data;

        }

        markDirty(&dbmanager->buffer,&dbmanager->page);
        location = data;

        location = location + (recordID->slot * record_size);

        loc = (recordID->slot * record_size);



        for(int counter = 0; counter < record_size; counter++) {

                data[loc++] = record->data[counter];

        }



        unpinPage(&dbmanager->buffer,&dbmanager->page);

        forcePage(&dbmanager->buffer,&dbmanager->page);

        dbmanager->numRows++;

        pinPage(&dbmanager->buffer,&dbmanager->page,0);

        return RC_OK;


}
extern RC updateRecord (RM_TableData *rel, Record *record){

        DatabaseManager *dbmanager = rel->mgmtData;

        char *dataPointer;

        pinPage(&dbmanager->buffer,&dbmanager->page,record->id.page);

        RID recordID = record->id;

        int record_size = getRecordSize(rel->schema);

        dataPointer = dbmanager->page.data;
        dataPointer = dataPointer + (recordID.slot * record_size);
        memcpy(dataPointer,record->data,record_size);
        markDirty(&dbmanager->buffer,&dbmanager->page);
        unpinPage(&dbmanager->buffer,&dbmanager->page);
        forcePage(&dbmanager->buffer,&dbmanager->page);

        return RC_OK;
}
extern RC deleteRecord(RM_TableData *rel, RID id){
        DatabaseManager *dbmanager = rel->mgmtData;


        pinPage(&dbmanager->buffer,&dbmanager->page,id.page);
        dbmanager->freePage = id.page;
        char *data =dbmanager->page.data;
        int record_size = getRecordSize(rel->schema);

        for(int i=0; i<record_size; i++) {
                data[(id.slot * record_size) + i] = '-';

        }

        markDirty(&dbmanager->buffer,&dbmanager->page);
        unpinPage(&dbmanager->buffer,&dbmanager->page);
        forcePage(&dbmanager->buffer,&dbmanager->page);

        return RC_OK;
}

extern RC getRecord (RM_TableData *rel, RID id, Record *record){
        DatabaseManager *dbmanager = (DatabaseManager *)rel->mgmtData;

        pinPage(&dbmanager->buffer,&dbmanager->page,id.page);
        int record_size = getRecordSize(rel->schema);


        char *d = dbmanager->page.data;
        for(int i =0; i<12; i++) {

                record->data[i] = d[(id.slot*12)+i];

        }
        unpinPage(&dbmanager->buffer,&dbmanager->page);

        return RC_OK;
}
int scanCount =  0;
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
        scan->mgmtData = cond;
        scan->rel = rel;

        return RC_OK;
}
extern RC next (RM_ScanHandle *scan, Record *record){
        Expr *cond = (Expr *)scan->mgmtData;
        Schema *schema = scan->rel->schema;
        if (cond == NULL)
        {
                return RC_ERROR;
        }
        Value *result = (Value *) malloc(sizeof(Value));
        int record_size = getRecordSize(schema);
        int maxRecords = PAGE_SIZE/record_size;
        int totalRows = dbm->numRows;


        while(scanCount <= totalRows) {
                pinPage(&dbm->buffer,&dbm->page,1);
                char *data = dbm->page.data;

                data = data + (record_size*scanCount);
                record->id.page = 1;
                record->id.slot = scanCount;

                char *recordData = record->data;
                scanCount++;
                memcpy(recordData,data,record_size);

                evalExpr(record,schema,cond,&result);
                if(result->v.boolV == TRUE) {
                        unpinPage(&dbm->buffer,&dbm->page);
                        return RC_OK;
                }
        }
        scanCount = 0;
        return RC_RM_NO_MORE_TUPLES;
}
extern RC closeScan (RM_ScanHandle *scan)
{
        return RC_OK;
}

extern int getRecordSize (Schema *schema){
        Schema *s = schema;
        int count = s->numAttr;
        int i,size = 0;

        for(i=0; i<count; i++) {

                switch(s->dataTypes[i]) {
                case DT_INT: size += sizeof(int); break;
                case DT_STRING: size += s->typeLength[i]; break;
                case DT_FLOAT: size += sizeof(float); break;
                case DT_BOOL: size += sizeof(bool); break;
                }
        }

        return size;
}

extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
        Schema *s = (Schema *)malloc(sizeof(Schema));
        s->numAttr = numAttr;
        s->attrNames = attrNames;
        s->dataTypes = dataTypes;
        s->typeLength = typeLength;
        s->keySize = keySize;
        s->keyAttrs = keys;

        return s;
}
extern RC freeSchema (Schema *schema){
        Schema *s = schema;
        int i;
        for(i =0; i<s->numAttr; i++) {
                free(s->attrNames[i]);
        }
        free(s->typeLength);
        free(s->dataTypes);
        free(s->keyAttrs);
        free(s);
        return RC_OK;
}


extern RC createRecord (Record **record, Schema *schema){
        Record *r =(Record *)malloc(sizeof(Record));
        r->id.page = r->id.slot = -1;
        int size = getRecordSize(schema);
        r->data = (char*)malloc(size);
        for(int k=0; k<getRecordSize(schema); k++) {
                r->data[k] = '-';
        }

        *record = r;
        return RC_OK;
}
extern int getAttributeOffset(int attrNum, Schema *schema){
        int i,offset =0;

        for(i=0; i<attrNum; i++) {

                switch(schema->dataTypes[i]) {
                case DT_INT: offset += sizeof(int); break;
                case DT_STRING: offset += schema->typeLength[i]; break;
                case DT_FLOAT: offset += sizeof(float); break;
                case DT_BOOL: offset += sizeof(bool); break;
                }
        }

        return offset;
}
extern RC freeRecord (Record *record){
        free(record->data);
        free(record);
        return RC_OK;
}

extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){

        int numAttr = schema->numAttr;
        int i;
        int offset=0;
        int dataP = 0;

        offset = getAttributeOffset(attrNum,schema);
        dataP = dataP + offset;

        switch(schema->dataTypes[attrNum]) {
        case 0: {
                char temp[20];
                sprintf(temp,"%d", value->v.intV);
                int count =0;
                while(value->v.intV != 0)
                {
                        value->v.intV /= 10;
                        ++count;
                }
                for(int t = 0; t < count; t++) {
                        record->data[dataP+t] = temp[t];
                }
                break;
        }
        case 1: {

                int length = schema->typeLength[attrNum];
                char *charPointer = &record->data[dataP];
                int t = dataP;
                for(int k=0; k<strlen(value->v.stringV); k++) {
                        record->data[t+k] = value->v.stringV[k];
                }
                break;
        }
        case 2: {
                char temp[20];
                sprintf(temp,"%f", value->v.floatV);
                record->data[dataP] =*temp;
                break;
        }
        case 3: {
                char temp[20];
                sprintf(temp,"%d", value->v.boolV);
                record->data[dataP] =*temp;
                break;
        }
      }
        return RC_OK;
}
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
        Value *new_value = (Value *)malloc(sizeof(Value));
        int offset = 0;
        int dataP = 0;
        offset = getAttributeOffset(attrNum,schema);

        dataP = dataP + offset;
        char *data = record->data;
        data = data + offset;
        switch(schema->dataTypes[attrNum]) {
        case 0: {
                char temp[20];
                int count = 0;
                int i;
                for(i =0; i<4; i++) {
                        if((data[i] - '0') > 0) {
                                temp[count] = data[i];
                                count++;
                        }
                        else{
                                break;
                        }

                }
                int vs=0;
                for(int k =0; k<count; k++) {
                        vs = vs * 10 +(temp[k] - '0');

                }
                new_value->v.intV  = vs;
                new_value->dt = 0;
                break;
        }
        case 1: {
                new_value->v.stringV = (char *)malloc(4);
                new_value->dt = 1;
                strncpy(new_value->v.stringV,data,4);
                new_value->v.stringV[4] = '\0';
                break;
        }
        case 2: {
                new_value->v.floatV  = data[dataP] - '0';
                new_value->dt = 2;
                break;
        }
        case 3: {
                new_value->v.boolV  = data[dataP] - '0';
                new_value->dt = 3;
                break;
        }
      }
        *value = new_value;
        return RC_OK;

}
