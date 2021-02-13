#include<iostream>
#include<vector>
using namespace std;

int transactionIDCount = 1;
//>>>>>> global parameters. change only these params
const int maxDirectoryEntriesInMainMemory=1024;
const int maxSizeOfSSMBucketArray = 1024;
const int maxBucketCapacityForRecords = 2;
const int maxBucketCapacityForDirectoryEntries=2;
const string nameOfTAInputFile = "test.csv";
const int countRandomRecord = 200;
const string nameOFSelfGeneratedFile = "test_generated.csv";
//<<<<<< do not change any other values apart from these



class Record{
    int transactionID;
    int amount;
    int category;
    string name;
    
    public:
        //dummy constructor
        Record();
        //derive record type from entry type for directory overflow
        Record(Entry);
        //standard record type addition
        Record(int, int, int, string);
        //used to generate random record
        static Record generateRandomRecord(int);
        //as directory entry, append 0 to prefix, maintaining same bucket pointer
        bool asDirectoryUpdateHashPrefix(int);
        //as directory entry, append 1 to parent prefix, maintaining same bucket pointer as parent
        bool asDirectoryDeriveFromParent(Record);
};

class Bucket
{
    int size;
    int curEmptySpace;
    int localDepth;
    int next;
    Record *data;
    public:
    // initialize new bucket based on record/entry
    void setBucket(int);
    // add a record into the bucket
    bool addRecord(Record);
    void setNextBucket(Bucket *);// 
};

class SSM{
    int size;
    int curRecordsBucketPosition;
    int curEntryBucketPosition;
    vector<int> freedBuckets;
    Bucket *data;

public:
    SSM();
    bool insertAsRecord(Record, int,bool);
    bool insertAsDirectory(Record, int);
    int insertBucket(bool);
    bool isDirectoryExpansionRequired(int); // given index of bucket, checks if directory expansion is required;
    vector<Record> getCompleteRecordsOfBucketsLinked(int);// gives list of buckets, also overflown.
    void resetBucketsLinked(int);// resets all the buckets to nil, also adds them to freed buckets except first bucket

private:
    int getIndexForDirectory(int); // fill from the last;
};

class Entry{
    public:
    int hashPrefix;
    int bucketIndex;
        Entry();
        Entry(string, int); 
        // used only during directory expansion. appends 0 to end of hashprefix
        void updateEntry(); 
        // used only during directory expansion. append 1 to end of input hashprefix.
        void deriveFromEntry(Entry); 
};

class DirectoryTable{
    int size;
    int curSize;
    int depth;
    vector<Entry*> data;

public:
    DirectoryTable(SSM);
    int getBucketIndex(Record,SSM);
    void expandDirectory(SSM);// p1
    void rearrangeAfterLocalSplit(int, int);

private:
    bool insertEntry(Entry*,SSM);// p1
    bool createDirectoryEntry();
};

class ExtendibleHash{
    DirectoryTable dTable;
    SSM ssm;
    public:
        ExtendibleHash();
        void insertAsLoop(vector<Record>, bool);
        void insertRecord(Record,bool);
        void visualize();
};

class Util
{
    public:
    void generateRecordsAndStore();
    void runExtendibleHashWithTAInput();
    void runExtendibleHashWithSelfGeneratedFileInput();

    private:
    vector<Record> loadFromfile(string);
    void runExtendibleHash(vector<Record> data){
        ExtendibleHash extendibleHash;
        extendibleHash.insertAsLoop(data,true);
        extendibleHash.visualize();
    }
};

int main()
{
    Util util;
    // util.generateRecordsAndStore();
}


// extendible hash class declarations begin >>>>>>>>>>>>>>>>>>>>
ExtendibleHash::ExtendibleHash():ssm(),dTable(ssm){}
void ExtendibleHash::insertAsLoop(vector<Record> data, bool isLogged){
    for (int i = 0; i < data.size();i++){
        insertRecord(data[i]);
        if(isLogged)
            visualize();
    }
}
void ExtendibleHash::insertRecord(Record data,bool isFirst=true){
    int bIndex = dTable.getBucketIndex(data,this->ssm);
    bool inserted=ssm.insertAsRecord(data, bIndex,!isFirst);
    if(!inserted){
        if(isFirst){
            bool directoryExpansionRequired = ssm.isDirectoryExpansionRequired(bIndex);
            if(directoryExpansionRequired){
                dTable.expandDirectory(this->ssm);
            }
        }
        // internal table splitting
        // create new bucket, rehash all records of current index

        vector<Record> rehashableRecords = ssm.getCompleteRecordsOfBucketsLinked(bIndex);
        ssm.resetBucketsLinked(bIndex);
        rehashableRecords.push_back(data);
        // re arrange the directory table entries below
        int newBucketIndex=ssm.insertBucket(true);
        dTable.rearrangeAfterLocalSplit(bIndex, newBucketIndex);
        // insert the records once again, forcefully
        while (!rehashableRecords.empty())
        {
            Record temp = rehashableRecords.back();
            insertRecord(temp, false);
            rehashableRecords.pop_back();
        }
    }
    return;
}
// extendible hash class declarations end   <<<<<<<<<<<<<<<<<<<<

// ssm class declaration begin >>>>>>>>>>>>>>>>>>>>
SSM::SSM(){
    size = maxSizeOfSSMBucketArray;
    data = new Bucket[size];
    curRecordsBucketPosition = 0;
    curEntryBucketPosition = maxSizeOfSSMBucketArray - 1;
    insertBucket(true);
}
int SSM::insertBucket(bool isRecordType=true){
    int newIndex;
    if (isRecordType)
    {
        if(curRecordsBucketPosition>=curEntryBucketPosition){
            if(freedBuckets.empty())
                return -1;
            else{
                newIndex = freedBuckets.back();
                data[newIndex].setBucket(maxBucketCapacityForRecords);
                freedBuckets.pop_back();
            }
        }
        else{
            newIndex = curRecordsBucketPosition++;
            data[newIndex].setBucket(maxBucketCapacityForRecords);
        }
    }
    else{
        newIndex = curEntryBucketPosition--;
        data[newIndex].setBucket(maxBucketCapacityForDirectoryEntries);
    }
    return newIndex;
}
// ssm class declaration end <<<<<<<<<<<<<<<<<<<<<<

// bucket class declarations begin >>>>>>>>>>>>>>>>>>>>
void Bucket::setBucket(int newSize){
    size = newSize;
    curEmptySpace = newSize;
    data = new Record[curEmptySpace];
}
bool Bucket::addRecord(Record data){
    if(curEmptySpace==0)
        return false;
    else{
        this->data[size-curEmptySpace] = data;
        curEmptySpace--;
    }
    return true;
}
// bucket class declarations end <<<<<<<<<<<<<<<<<<<<<<<

//Directory table declarations begin >>>>>>>>>>>>>>>>>>>>>>>
DirectoryTable::DirectoryTable(SSM ssm){
    size = maxDirectoryEntriesInMainMemory;
    depth = 0;
    curSize = 0;
    Entry *e = new Entry("", 0);
    insertEntry(e,ssm);
}
bool DirectoryTable::insertEntry(Entry *data,SSM ssm){
    if(curSize<size){
        this->data.push_back(data);
        curSize++;
    }
    else{
        Record newRecord(*data);
        ssm.insertAsDirectory(newRecord,++curSize);
    }
}

void DirectoryTable::expandDirectory(SSM ssm){
    int curSize = data.size();
    for (int i = 0; i < curSize;i++){
        Entry *curEntry = data[i], *newEntry;
        newEntry = new Entry();
        newEntry->deriveFromEntry(*curEntry);
        curEntry->updateEntry();
        insertEntry(newEntry,ssm);
    }
    ++depth;
}
//Directory table declarations end <<<<<<<<<<<<<<<<<<<<<<<<<

//Record class declaration begin >>>>>>>>>>>>>>>>>>>>>>>>
Record::Record() { ; }
Record::Record(Entry e) {
    transactionID = e.hashPrefix;
    amount = e.bucketIndex;
}
Record::Record(int i, int j, int k, string s) {
    transactionID = i;
    amount = j;
    category = k;
    name = s;
}
bool Record::asDirectoryDeriveFromParent(Record r){
    transactionID = r.transactionID << 1;
    transactionID++;
    amount = r.amount;
}
bool Record::asDirectoryUpdateHashPrefix(int i){
    transactionID <<= 1;
}
Record Record::generateRandomRecord(int i){
    int transactionID = transactionIDCount++;
    int category = rand()*1500;
    int amount = rand()*500000;
    string name = gen_random();
    return Record(transactionID, amount, category, name);
}
//Records class declarations end <<<<<<<<<<<<<<<<<<<<<<<<

//Entry class declarations begin >>>>>>>>>>>>>>>>>>>>>>>>
Entry::Entry() { ; }
Entry::Entry(string s, int i) {
    hashPrefix = stringToInt(s);
    bucketIndex = i;
}
void Entry::deriveFromEntry(Entry e) {
    hashPrefix = e.hashPrefix << 1;
    hashPrefix++;
    bucketIndex = e.bucketIndex;
}
void Entry::updateEntry() {
    hashPrefix <<= 1;
}
//Entry class declarations end <<<<<<<<<<<<<<<<<<<<<<<<<<


//helper functions
int stringToInt(string s){
    return stoi(s,nullptr,2);
}

string intToString(int i){
    return to_string(i);
}

string gen_random() {
    string s="abc";
    for (size_t i = 0; i < 3; ++i)
    {
        int randomChar = rand() % (26 + 26);
        if (randomChar < 26)
            s[i] = 'a' + randomChar;
        else if (randomChar < 26 + 26)
            s[i] = 'A' + randomChar - 26;
        else
            s[i] = 'z';
    }
    return s;
}