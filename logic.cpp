#include<iostream>
#include<vector>
#include<stack>
using namespace std;

/*to do

entry overflow to ssm, issue= no init of bucket happening

*/

//do not change following values
int transactionIDCount = 1;
int endOfBucketFlag = -1;
//
// logger flag
bool development = false;
//

//>>>>>> global parameters. change only these params
const int maxDirectoryEntriesInMainMemory=2;
const int maxSizeOfSSMBucketArray = 1024;
const int maxBucketCapacityForRecords = 1;
const int maxBucketCapacityForDirectoryEntries=1;
const string nameOfTAInputFile = "test.csv";
const int countRandomRecord = 200;
const string nameOFSelfGeneratedFile = "test_generated.csv";
//<<<<<< do not change any other values apart from these



//helper functions
int stringToInt(string s){
    s = "0" + s;
    return stoi(s, nullptr, 2);
}

//  check getHashValue function for int to string

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

pair<int,int> getSSMBucketIndexOffsetForDirectory(int i){
    int ssmIndex, bucketOffset;
    bucketOffset = (i - (maxDirectoryEntriesInMainMemory - 1)) % (maxBucketCapacityForDirectoryEntries + 1);
    ssmIndex = ceil((i - (maxDirectoryEntriesInMainMemory - 1)) / maxBucketCapacityForDirectoryEntries)-1;
    ssmIndex = maxSizeOfSSMBucketArray-ssmIndex;
    if(development){
    cout << i <<" "<< ssmIndex << " " << bucketOffset << endl;
    }
    return make_pair(ssmIndex, bucketOffset);
}

class Entry{
    public:
    int hashPrefix;
    int bucketIndex;
    Entry() {
        hashPrefix = bucketIndex=-1;
    }
    Entry(string s, int i) {
        hashPrefix = stringToInt(s);
        bucketIndex = i;
    }
    void deriveFromEntryWhenExpansion(Entry e) {
        hashPrefix = e.hashPrefix << 1;
        hashPrefix++;
        bucketIndex = e.bucketIndex;
    }
    void updateEntryWhenExpansion() {
        hashPrefix =hashPrefix<< 1;
    }
};

class Record{
    public:

    int transactionID;
    int amount;
    int category;
    string name;
    
    Record() { 
     transactionID=amount=category=-1;
     name = "zzz";
    }
    Record(Entry e) {
        transactionID = e.hashPrefix;
        amount = e.bucketIndex;
    }
    Record(int i, int j, int k, string s) {
        transactionID = i;
        amount = j;
        category = k;
        name = s;
    }
    bool asDirectoryDeriveFromParent(Record r){
        transactionID = r.transactionID << 1;
        transactionID++;
        amount = r.amount;
        return true;
    }
    bool asDirectoryUpdateHashPrefix(int i){
        transactionID = transactionID<<1;
        return true;
    }
    
};

Record generateRandomRecord(int i){
        int transactionID = transactionIDCount++;
        int category = rand()%1500;
        int amount = rand()%500000;
        string name = gen_random();
        return Record(transactionID, amount, category, name);
}

// for msb
// int getHashValue(Record r, int depth){
//     bitset<16> bnum(r.transactionID);
//     string bstring = bnum.to_string();
//     bstring = bstring.substr(0, depth);
//     return stringToInt(bstring);
// }


// for lsb
int getHashValue(Record r, int depth){
    string bstring = getHashValueAsString(r, depth);
    return stringToInt(bstring);
}

string getHashValueAsString(Record r, int depth){
    bitset<16> bnum(r.transactionID);
    string bstring = bnum.to_string();
    bstring = bstring.substr(bstring.length()-depth-1,depth);
    return bstring;
}

string intToString(Entry e, int depth ){
    bitset<16> bnum(e.hashPrefix);
    string bstring = bnum.to_string();
    bstring = bstring.substr(bstring.length()-depth-1,depth);
    return bstring;
}

class Bucket
{
    public:
    int size;
    int curEmptySpace;
    int localDepth;
    int next;
    bool isStale;
    vector<Record> data;
    Bucket(){
        isStale = true;
    }
    void setBucket(int newSize)
    {
        size = newSize;
        curEmptySpace = newSize;
        data.resize(curEmptySpace);
        next = endOfBucketFlag;
        isStale = false;
    }
    bool addRecord(Record data){
        if(curEmptySpace==0)
            return false;
        else{
            this->data[size-curEmptySpace] = data;
            curEmptySpace--;
        }
        return true;
    }
    
    void resetBucket(){
        data.clear();
        next = endOfBucketFlag;
        isStale = true;
    }
};

class SSM{
    public:

    int size;
    // int curRecordsBucketPosition;
    int curEntryBucketPosition;
    vector<int> freedBuckets;
    vector<Bucket> data;

    SSM(){
    size = maxSizeOfSSMBucketArray;
    data.resize(size);
    // curRecordsBucketPosition = 0;
    curEntryBucketPosition = maxSizeOfSSMBucketArray - 1;
    insertBucket(true);
    }

    int insertBucket(bool isRecordType=true){
        int newIndex;
        if (isRecordType)
        {
            for (int i = 0; i < curEntryBucketPosition;i++){
                if(data[i].isStale){
                    data[i].setBucket(maxBucketCapacityForRecords);
                    return i;
                }
            }
        }
        else{
            newIndex = curEntryBucketPosition--;
            data[newIndex].setBucket(maxBucketCapacityForDirectoryEntries);
        }
        return newIndex;
    }

    bool insertAsRecord(Record r, int bIndex,bool forced){
        if(data[bIndex].next!=endOfBucketFlag){
            return insertAsRecord(r, data[bIndex].next, forced);
        }
        bool isAdded = data[bIndex].addRecord(r);
        if (!isAdded)
        {
            if(forced){
                int nextIndex = insertBucket(true);
                data[bIndex].next=nextIndex;
                data[nextIndex].addRecord(r);
                return true;
            }
            return false;
        }
        else
            return true;
    }

    bool insertAsDirectory(Record r,int i){
        int ssmIndex, bucketOffset;
        pair<int, int> temp = getSSMBucketIndexOffsetForDirectory(i);
        ssmIndex = temp.first;
        bucketOffset = temp.second;
        if(data[ssmIndex].data.size()!=maxBucketCapacityForDirectoryEntries){
            data[ssmIndex].setBucket(maxBucketCapacityForDirectoryEntries);
        }
        data[ssmIndex].data[bucketOffset] = r;
        return true;
    }

    bool isDirectoryExpansionRequired(int localDept,int dTableDepth){
        return localDept == dTableDepth;
    }

    void resetBucketsLinked(int startingIndex){
        if(startingIndex==endOfBucketFlag)
            return;
        int nextIndex = data[startingIndex].next;
        // freedbucket functionality remains here
        data[startingIndex].resetBucket();
        resetBucketsLinked(nextIndex);
    }

    void getCompleteRecordsOfBucketsLinked(int bIndex, vector<Record> &ans){
        for (int i = 0; i <data[bIndex].size-data[bIndex].curEmptySpace;i++){
            ans.push_back(data[bIndex].data[i]);
        }
        if(data[bIndex].next==endOfBucketFlag)
            return;
        return getCompleteRecordsOfBucketsLinked(data[bIndex].next, ans);
    }
};



class DirectoryTable{
    public:

    int size;
    int curSize;
    int depth;
    vector<Entry> data;

    DirectoryTable(SSM *ssm){
    size = maxDirectoryEntriesInMainMemory;
    depth = 0;
    curSize = 0;
    Entry e("", 0);
    insertEntry(e,ssm);
}
    bool insertEntry(Entry data,SSM *ssm){
        if(curSize<size){
            this->data.push_back(data);
            curSize++;
        }
        else{
            Record newRecord(data);
            ssm->insertAsDirectory(newRecord,++curSize);
        }
        return true;
    }
    void expandDirectory(SSM *ssm){
        int curSize = data.size();
        for (int i = 0; i < curSize;i++){
            Entry  newEntry;
            newEntry.deriveFromEntryWhenExpansion(data[i]);
            data[i].updateEntryWhenExpansion();
            insertEntry(newEntry,ssm);
        }
        ++depth;
    }
    int getBucketIndex(Record r, SSM *ssm){
        int hashedVal = getHashValue(r, this->depth);
        if(development) {
            cout << "input tid:" << r.transactionID << " "
                << " hashed value:" << hashedVal << endl;
        }

        // this loop is coupled with rearrgange after local split
        for (int i = 0; i < curSize;i++){
            if(i<maxDirectoryEntriesInMainMemory){
                if(data[i].hashPrefix==hashedVal){
                    return data[i].bucketIndex;
                }
            }
            else{
                int ssmIndex, bucketOffset;
                pair<int, int> temp = getSSMBucketIndexOffsetForDirectory(i);
                ssmIndex = temp.first;
                bucketOffset = temp.second;
                if (ssm->data[ssmIndex].data[bucketOffset].transactionID == hashedVal)
                {
                    return ssm->data[ssmIndex].data[bucketOffset].amount;
                }
            }
        }
        exit(1);
    }
    void rearrangeAfterLocalSplit(int parentIndex,int childIndex,SSM * ssm){
        stack<int> pointsToparentIndex;
        // this loop is coupled with get bindex
        for (int i = 0; i < curSize;i++){
            if(i<maxDirectoryEntriesInMainMemory){
                if(data[i].bucketIndex==parentIndex){
                    pointsToparentIndex.push(i);
                }
            }
            else{
                int ssmIndex, bucketOffset;
                pair<int, int> temp = getSSMBucketIndexOffsetForDirectory(i);
                ssmIndex = temp.first;
                bucketOffset = temp.second;
                if(ssm->data[ssmIndex].data[bucketOffset].amount==parentIndex){
                    pointsToparentIndex.push(i);
                }
            }
        }
        int stackMaxSize = pointsToparentIndex.size();
        while(pointsToparentIndex.size()>(stackMaxSize/2)){
            int i = pointsToparentIndex.top();
            pointsToparentIndex.pop();
            if(i<maxDirectoryEntriesInMainMemory){
                data[i].bucketIndex = childIndex;
            }
            else{
                int ssmIndex, bucketOffset;
                pair<int, int> temp = getSSMBucketIndexOffsetForDirectory(i);
                ssmIndex = temp.first;
                bucketOffset = temp.second;
                ssm->data[ssmIndex].data[bucketOffset].amount = childIndex;
            }
        }
    }
};

class ExtendibleHash{
    DirectoryTable dTable;
    SSM ssm;
    public:
        ExtendibleHash():ssm(),dTable(nullptr){}
    void insertAsLoop(vector<Record> data, bool isLogged){
        for (int i = 0; i < data.size();i++){
            if(development){
            char c;
            cin >> c;
            }
            insertRecord(data[i], true);
            if(isLogged)
                visualize();
        }
    }

    void localSplitBuckets(){
        
    }

    void insertRecord(Record data,bool isFirst=true){
        int bIndex = dTable.getBucketIndex(data,&this->ssm);
        if(development){
            cout << " bucket index is:" << bIndex << endl;
        }
        bool inserted=ssm.insertAsRecord(data, bIndex,!isFirst);
        if(!inserted){
            if(isFirst){
                bool directoryExpansionRequired = ssm.isDirectoryExpansionRequired(ssm.data[bIndex].localDepth,dTable.depth);
                if(directoryExpansionRequired){
                    dTable.expandDirectory(&this->ssm);
                }
            }
            // internal table splitting
            // create new bucket, rehash all records of current index
            vector<Record> rehashableRecords;
            int curLocalDepth = ssm.data[bIndex].localDepth;
            ssm.getCompleteRecordsOfBucketsLinked(bIndex,rehashableRecords);
            rehashableRecords.push_back(data);
            ssm.resetBucketsLinked(bIndex);
            ssm.data[bIndex].setBucket(maxBucketCapacityForRecords);
            // re arrange the directory table entries below
            int newBucketIndex=ssm.insertBucket(true);
            //set local depths after splitting
            ssm.data[bIndex].localDepth = ssm.data[newBucketIndex].localDepth = curLocalDepth + 1;
            dTable.rearrangeAfterLocalSplit(bIndex, newBucketIndex, &ssm);
            // insert the records once again, forcefully
            for (int i = 0;i<rehashableRecords.size();i++)
            {
                Record temp = rehashableRecords[i];
                insertRecord(temp, false);
            }
        }
        return;
    }
    void visualize(){
        // first print directory table
        cout << endl
             << "Directory table begin >>>>>>>>>>>>>>>" << endl
             << "current depth: " << dTable.depth << endl;
        for (int i = 0; i < dTable.curSize; i++)
        {
            if(i<maxDirectoryEntriesInMainMemory){
                cout << " in main memory " << endl;
                cout <<i<<") "<< dTable.data[i].hashPrefix << "\t" << dTable.data[i].bucketIndex << endl;
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
        for (int i = 0; i < ssm.curEntryBucketPosition;i++){
            if(!ssm.data[i].isStale && ssm.data[i].curEmptySpace!=ssm.data[i].size)
            for (int j = 0; j < ssm.data[i].data.size()-ssm.data[i].curEmptySpace;j++){
                Record r = ssm.data[i].data[j];
                cout <<i<<") "<< r.transactionID << " " << r.name << " " << r.transactionID << " " << r.category << endl;
                cout << "depth" << ssm.data[i].localDepth << endl;
            }
        }
        cout<<endl << " SSM END >>>>>>>>>>>>>>>>>>>" << endl;

    }
};

class Util
{
    public:
    void generateRecordsAndStore();
    void runExtendibleHashWithTAInput();
    void runExtendibleHashWithSelfGeneratedFileInput();

    void runExtendibleHash(vector<Record> data){
        ExtendibleHash extendibleHash;
        extendibleHash.insertAsLoop(data,false);
        extendibleHash.visualize();
    }
    private:
    vector<Record> loadFromfile(string);
};



int main()
{
    Util util;
    vector<Record> test;
    Record r;
    for (int i = 0; i < 4;i++)
    {
        r = generateRandomRecord(1);
        test.push_back(r);
    }
    util.runExtendibleHash(test);
    // util.generateRecordsAndStore();
}


// extendible hash class declarations begin >>>>>>>>>>>>>>>>>>>>

// extendible hash class declarations end   <<<<<<<<<<<<<<<<<<<<

// ssm class declaration begin >>>>>>>>>>>>>>>>>>>>

// ssm class declaration end <<<<<<<<<<<<<<<<<<<<<<

// bucket class declarations begin >>>>>>>>>>>>>>>>>>>>

// bucket class declarations end <<<<<<<<<<<<<<<<<<<<<<<

//Directory table declarations begin >>>>>>>>>>>>>>>>>>>>>>>

//Directory table declarations end <<<<<<<<<<<<<<<<<<<<<<<<<

//Record class declaration begin >>>>>>>>>>>>>>>>>>>>>>>>

//Records class declarations end <<<<<<<<<<<<<<<<<<<<<<<<

//Entry class declarations begin >>>>>>>>>>>>>>>>>>>>>>>>
//Entry class declarations end <<<<<<<<<<<<<<<<<<<<<<<<<<


