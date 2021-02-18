#include<iostream>
#include<vector>
#include<stack>
using namespace std;

//do not change following values
int transactionIDCount = 1;
int endOfBucketFlag = -1;
//

//>>>>>> global parameters. change only these params
const int maxDirectoryEntriesInMainMemory=1024;
const int maxSizeOfSSMBucketArray = 1024;
const int maxBucketCapacityForRecords = 1;
const int maxBucketCapacityForDirectoryEntries=1;
const string nameOfTAInputFile = "test.csv";
const int countRandomRecord = 200;
const string nameOFSelfGeneratedFile = "test_generated.csv";
//<<<<<< do not change any other values apart from these



//helper functions
int stringToInt(string s){
    if(s=="")
        return 0;
    return stoi(s, nullptr, 2);
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
class Record{
    public:

    int transactionID;
    int amount;
    int category;
    string name;
    
        //dummy constructor
        Record();
        //derive record type from entry type for directory overflow
        Record(Entry);
        //standard record type addition
        Record(int, int, int, string);
        //used to generate random record
        //as directory entry, append 0 to prefix, maintaining same bucket pointer
        bool asDirectoryUpdateHashPrefix(int);
        //as directory entry, append 1 to parent prefix, maintaining same bucket pointer as parent
        bool asDirectoryDeriveFromParent(Record);
};

Record generateRandomRecord(int);

int getHashValue(Record r, int depth){
    bitset<16> bnum(r.transactionID);
    string bstring = bnum.to_string();
    bstring = bstring.substr(0, depth);
    return stringToInt(bstring);
}
class Bucket
{
    public:
    int size;
    int curEmptySpace;
    int localDepth;
    int next;
    vector<Record> data;
    // initialize new bucket based on record/entry
    void setBucket(int);
    // add a record into the bucket
    bool addRecord(Record);
    // this is called by bucket, which wants to set next bucket in the chain, pass index of new link
    void setNextBucket(int);//
    //used when local splitting
    void resetBucket();
};

class SSM{
    public:

    int size;
    int curRecordsBucketPosition;
    int curEntryBucketPosition;
    vector<int> freedBuckets;
    Bucket *data;

    SSM();
    bool insertAsRecord(Record, int,bool);
    bool insertAsDirectory(Record, int);
    int insertBucket(bool);
    bool isDirectoryExpansionRequired(int,int); // given index of bucket, checks if directory expansion is required;
    void getCompleteRecordsOfBucketsLinked(int,vector<Record> &);// gives list of buckets, also overflown.
    void resetBucketsLinked(int);// resets all the buckets to nil, also adds them to freed buckets except first bucket
};



class DirectoryTable{
    public:

    int size;
    int curSize;
    int depth;
    vector<Entry*> data;

    DirectoryTable(SSM*);
    int getBucketIndex(Record,SSM*);
    void expandDirectory(SSM*);// p1
    void rearrangeAfterLocalSplit(int, int,SSM *);

private:
    bool insertEntry(Entry*,SSM*);// p1
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

    void runExtendibleHash(vector<Record> data){
        ExtendibleHash extendibleHash;
        extendibleHash.insertAsLoop(data,true);
        extendibleHash.visualize();
    }
    private:
    vector<Record> loadFromfile(string);
};

pair<int,int> getSSMBucketIndexOffsetForDirectory(int i){
    int ssmIndex, bucketOffset;
    bucketOffset = (i - (maxDirectoryEntriesInMainMemory - 1)) % (maxBucketCapacityForDirectoryEntries + 1);
    ssmIndex = ceil((i - (maxDirectoryEntriesInMainMemory - 1)) / maxBucketCapacityForDirectoryEntries)-1;
    return make_pair(ssmIndex, bucketOffset);
}

int main()
{
    Util util;
    vector<Record> test;
    Record r;
    for (int i = 0; i < 100;i++)
    {
        r = generateRandomRecord(1);
        test.push_back(r);
        cout << r.transactionID;
    }
    util.runExtendibleHash(test);
    // util.generateRecordsAndStore();
}


// extendible hash class declarations begin >>>>>>>>>>>>>>>>>>>>
ExtendibleHash::ExtendibleHash():ssm(),dTable(nullptr){}
void ExtendibleHash::insertAsLoop(vector<Record> data, bool isLogged){
    for (int i = 0; i < data.size();i++){
        insertRecord(data[i],true);
        if(isLogged)
            visualize();
    }
}
void ExtendibleHash::insertRecord(Record data,bool isFirst=true){
    int bIndex = dTable.getBucketIndex(data,&this->ssm);
    bool inserted=ssm.insertAsRecord(data, bIndex,!isFirst);
    if(!inserted){
        if(isFirst){
            bool directoryExpansionRequired = ssm.isDirectoryExpansionRequired(bIndex,dTable.depth);
            if(directoryExpansionRequired){
                dTable.expandDirectory(&this->ssm);
            }
        }
        // internal table splitting
        // create new bucket, rehash all records of current index
        vector<Record> rehashableRecords;
        ssm.getCompleteRecordsOfBucketsLinked(bIndex,rehashableRecords);
        ssm.resetBucketsLinked(bIndex);
        int curLocalDepth = ssm.data[bIndex].localDepth;
        rehashableRecords.push_back(data);
        // re arrange the directory table entries below
        int newBucketIndex=ssm.insertBucket(true);
        //set local depths after splitting
        ssm.data[bIndex].localDepth = ssm.data[newBucketIndex].localDepth = curLocalDepth + 1;
        dTable.rearrangeAfterLocalSplit(bIndex, newBucketIndex, &ssm);
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
void ExtendibleHash::visualize(){
    // first print directory table
    cout<<endl<<"Directory table begin >>>>>>>>>>>>>>>"<<endl;
    for (int i = 0; i < dTable.curSize;i++){
        if(i<maxDirectoryEntriesInMainMemory){
            cout << " in main memory " << endl;
            cout <<i<<") "<< dTable.data[i]->hashPrefix << "\t" << dTable.data[i]->bucketIndex << endl;
        }
        else{
            pair<int, int> ssmEntry = getSSMBucketIndexOffsetForDirectory(i);
            cout << " in secondary memory " << endl;
            cout << ssmEntry.first<<") "<<ssm.data[ssmEntry.first].data[ssmEntry.second].transactionID << "\t" << ssm.data[ssmEntry.first].data[ssmEntry.second].amount << endl;
        }
    }
    cout<<endl << "Directory table end <<<<<<<<<<<<" << endl
         << endl;

    cout<<endl << " SSM Begin >>>>>>>>>>>>>>>>>>>" << endl;
    for (int i = 0; i < ssm.curRecordsBucketPosition;i++){
        for (int j = 0; j < ssm.data[i].data.size();j++){
            Record r = ssm.data[i].data[j];
            cout <<i<<") "<< r.transactionID << " " << r.name << " " << r.transactionID << " " << r.category << endl;
        }
    }
    cout<<endl << " SSM END >>>>>>>>>>>>>>>>>>>" << endl;

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
bool SSM::insertAsRecord(Record r, int bIndex,bool forced){
    if(data[bIndex].next!=endOfBucketFlag){
        return insertAsRecord(r, data[bIndex].next, forced);
    }
    if(!data[bIndex].addRecord(r)){
        if(forced){
            int nextIndex = insertBucket(true);
            data[bIndex].setNextBucket(nextIndex);
            data[nextIndex].addRecord(r);
            return true;
        }
        return false;
    }
    else
        return true;
}
bool SSM::insertAsDirectory(Record r,int i){
    int ssmIndex, bucketOffset;
    pair<int, int> temp = getSSMBucketIndexOffsetForDirectory(i);
    ssmIndex = temp.first;
    bucketOffset = temp.second;
    data[ssmIndex].data[bucketOffset] = r;
    return true;
}

bool SSM::isDirectoryExpansionRequired(int localDept,int dTableDepth){
    return localDept == dTableDepth;
}
void SSM::resetBucketsLinked(int startingIndex){
    if(startingIndex==endOfBucketFlag)
        return;
    int nextIndex = data[startingIndex].next;
    // freedbucket functionality remains here
    data[startingIndex].resetBucket();
}
void SSM::getCompleteRecordsOfBucketsLinked(int bIndex, vector<Record> &ans){
    if(data[bIndex].next==endOfBucketFlag)
        return;
    for (int i = 0; i <data[bIndex].data.size();i++){
        ans.push_back(data[bIndex].data[i]);
    }
    return getCompleteRecordsOfBucketsLinked(data[bIndex].next, ans);
}
// ssm class declaration end <<<<<<<<<<<<<<<<<<<<<<

// bucket class declarations begin >>>>>>>>>>>>>>>>>>>>
void Bucket::setBucket(int newSize){
    size = newSize;
    curEmptySpace = newSize;
    data.resize(curEmptySpace);
    next = endOfBucketFlag;
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
void Bucket::setNextBucket(int nextIndex){
    this->next=nextIndex;
    
}
void Bucket::resetBucket(){
    curEmptySpace = size;
    next = endOfBucketFlag;
}
// bucket class declarations end <<<<<<<<<<<<<<<<<<<<<<<

//Directory table declarations begin >>>>>>>>>>>>>>>>>>>>>>>
DirectoryTable::DirectoryTable(SSM *ssm){
    size = maxDirectoryEntriesInMainMemory;
    depth = 0;
    curSize = 0;
    Entry *e = new Entry("", 0);
    insertEntry(e,ssm);
}
bool DirectoryTable::insertEntry(Entry *data,SSM *ssm){
    if(curSize<size){
        this->data.push_back(data);
        curSize++;
    }
    else{
        Record newRecord(*data);
        ssm->insertAsDirectory(newRecord,++curSize);
    }
    return true;
}
void DirectoryTable::expandDirectory(SSM *ssm){
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
int DirectoryTable::getBucketIndex(Record r, SSM *ssm){
    int hashedVal = getHashValue(r, this->depth);
    // this loop is coupled with rearrgange after local split
    for (int i = 0; i < curSize;i++){
        if(i<maxDirectoryEntriesInMainMemory){
            if(data[i]->hashPrefix==hashedVal){
                return data[i]->bucketIndex;
            }
        }
        else{
            int ssmIndex, bucketOffset;
            pair<int, int> temp = getSSMBucketIndexOffsetForDirectory(i);
            ssmIndex = temp.first;
            bucketOffset = temp.second;
            if (ssm[ssmIndex].data->data[bucketOffset].transactionID == hashedVal)
            {
                return ssm[ssmIndex].data->data[bucketOffset].amount;
            }
        }
    }
    exit(1);
}
void DirectoryTable::rearrangeAfterLocalSplit(int parentIndex,int childIndex,SSM * ssm){
    stack<int> pointsToparentIndex;
    // this loop is coupled with get bindex
    for (int i = 0; i < curSize;i++){
        if(i<maxDirectoryEntriesInMainMemory){
            if(data[i]->bucketIndex==parentIndex){
                pointsToparentIndex.push(i);
            }
        }
        else{
            int ssmIndex, bucketOffset;
            pair<int, int> temp = getSSMBucketIndexOffsetForDirectory(i);
            ssmIndex = temp.first;
            bucketOffset = temp.second;
            if(ssm[ssmIndex].data->data[bucketOffset].amount==parentIndex){
                pointsToparentIndex.push(i);
            }
        }
    }
    int stackMaxSize = pointsToparentIndex.size();
    while(pointsToparentIndex.size()>(stackMaxSize/2)){
        int i = pointsToparentIndex.top();
        pointsToparentIndex.pop();
        if(i<maxDirectoryEntriesInMainMemory){
            data[i]->bucketIndex = childIndex;
        }
        else{
            int ssmIndex, bucketOffset;
            pair<int, int> temp = getSSMBucketIndexOffsetForDirectory(i);
            ssmIndex = temp.first;
            bucketOffset = temp.second;
            ssm[ssmIndex].data->data[bucketOffset].amount = childIndex;
        }
    }
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
    return true;
}
bool Record::asDirectoryUpdateHashPrefix(int i){
    transactionID <<= 1;
    return true;
}
Record generateRandomRecord(int i){
    int transactionID = transactionIDCount++;
    int category = rand()%1500;
    int amount = rand()%500000;
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


