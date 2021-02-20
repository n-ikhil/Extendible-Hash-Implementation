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
const int maxDirectoryEntriesInMainMemory=1;
const int maxSizeOfSSMBucketArray = 100;
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
    ++i;
    int ssmIndex, bucketOffset;
    bucketOffset = (i -1- (maxDirectoryEntriesInMainMemory)) % (maxBucketCapacityForDirectoryEntries);
    float f = floor((i-1  - (maxDirectoryEntriesInMainMemory)) / maxBucketCapacityForDirectoryEntries);
    ssmIndex = f;
    ssmIndex = maxSizeOfSSMBucketArray-ssmIndex-1;
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

    bool deriveFromEntryInSSMWhenExpansion(int transactionID,int amount){
        hashPrefix = transactionID << 1;
        hashPrefix++;
        bucketIndex = amount;
        return true;
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
    
    bool asDirectoryUpdateHashPrefix(){
        transactionID = transactionID<<1;
        return true;
    }

    
    
};

ostream &operator<<(std::ostream &os,Record r) { 
    return os <<"("<<r.transactionID<<","<<r.name<<","<<r.category<<","<<r.amount<<")";
}

Record generateRandomRecord(int i){
        int transactionID = transactionIDCount++;
        int category = rand()%1500;
        int amount = rand()%50000;
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

string getHashValueAsString(Record r, int depth){
    bitset<16> bnum(r.transactionID);
    string bstring = bnum.to_string();
    bstring = bstring.substr(bstring.length()-depth);
    return bstring;
}

int getHashValue(Record r, int depth){
    string bstring = getHashValueAsString(r, depth);
    return stringToInt(bstring);
}



string intToString(int i, int depth ){
    bitset<16> bnum(i);
    string bstring = bnum.to_string();
    bstring = bstring.substr(bstring.length()-depth);
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

void OutOfMemory(){
    cout << "Memory bounds exceeded. exiting" << endl;
}
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
            int newIndex = curEntryBucketPosition--;
            data[newIndex].setBucket(maxBucketCapacityForDirectoryEntries);
            return newIndex;
        }
        OutOfMemory();
        exit(0);
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
            ssm->insertAsDirectory(newRecord,curSize++);
        }
        return true;
    }
    void expandDirectory(SSM *ssm){
        int sizeBeforeExpansion = curSize;
        for (int i = 0; i < sizeBeforeExpansion;i++){
            Entry  newEntry;//////
            if (i < maxDirectoryEntriesInMainMemory)
            {
                newEntry.deriveFromEntryWhenExpansion(data[i]);
                data[i].updateEntryWhenExpansion();
            }
            else{
                int ssmIndex, bucketOffset;
                pair<int, int> temp = getSSMBucketIndexOffsetForDirectory(i);
                ssmIndex = temp.first;
                bucketOffset = temp.second;
                newEntry.deriveFromEntryInSSMWhenExpansion(ssm->data[ssmIndex].data[bucketOffset].transactionID,ssm->data[ssmIndex].data[bucketOffset].amount);
                ssm->data[ssmIndex].data[bucketOffset].asDirectoryUpdateHashPrefix();
            }
            // newEntry.deriveFromEntryWhenExpansion(data[i]);
            // data[i].updateEntryWhenExpansion();
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
                    if(development){
                    cout << i << " " << data[i].bucketIndex;
                    }
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
            insertRecord(data[i]);
            if(isLogged)
                visualize();
        }
    }

    int localSplitBuckets(int bIndex){
        int curLocalDepth = ssm.data[bIndex].localDepth;
        ssm.resetBucketsLinked(bIndex);
        ssm.data[bIndex].setBucket(maxBucketCapacityForRecords);
        // re arrange the directory table entries below
        int newBucketIndex=ssm.insertBucket(true);
        //set local depths after splitting
        ssm.data[bIndex].localDepth = ssm.data[newBucketIndex].localDepth = curLocalDepth + 1;

        return newBucketIndex;
    }

    void insertRecord(Record data){
        int bIndex = dTable.getBucketIndex(data,&this->ssm);
        if(development){
            cout << " bucket index is:" << bIndex << endl;
        }
        bool inserted=ssm.insertAsRecord(data,bIndex, false);
        if(!inserted){
            bool directoryExpansionRequired = ssm.isDirectoryExpansionRequired(ssm.data[bIndex].localDepth,dTable.depth);
            if(directoryExpansionRequired){
                dTable.expandDirectory(&this->ssm);
            }

            // initiate local splitting
            vector<Record> rehashableRecords;
            ssm.getCompleteRecordsOfBucketsLinked(bIndex,rehashableRecords);
            rehashableRecords.push_back(data);


            int curLocalDepth = ssm.data[bIndex].localDepth;
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
                int nBIndex = dTable.getBucketIndex(temp,&this->ssm);
                if(development){
                    cout << "index :"<<i<<", bucket index is:" << nBIndex << endl;
                }
                bool inserted=ssm.insertAsRecord(temp,nBIndex, true);
                if(!inserted) {
                    cout << " Memory shortage " << endl;
                    exit(1);
                }
            }
        }
        return;
    }
    void visualize(){
        // first print directory table
        cout << endl
             << "Directory table begin >>>>>>>>>>>>>>>" << endl;
        cout << "depth\t"
             << "index\t"
             << "loc\t"
             << "hash\t"
             << "points" << endl;
        for (int i = 0; i < dTable.curSize; i++)
        {
            if(i<maxDirectoryEntriesInMainMemory){
                cout <<dTable.depth<<"\t"<<i<<"\t"<<"MM"<<"\t"<<intToString(dTable.data[i].hashPrefix,dTable.depth) << "\t" << dTable.data[i].bucketIndex << endl;
            }
            else{
                pair<int, int> ssmEntry = getSSMBucketIndexOffsetForDirectory(i);
                cout <<dTable.depth<<"\t"<<i<<"\t"<<"SSM\t"<<intToString(ssm.data[ssmEntry.first].data[ssmEntry.second].transactionID,dTable.depth) << "\t" << ssm.data[ssmEntry.first].data[ssmEntry.second].amount<< endl;
                // cout << ssmEntry.first<<") "<<ssm.data[ssmEntry.first].data[ssmEntry.second].transactionID << "\t" << ssm.data[ssmEntry.first].data[ssmEntry.second].amount << endl;
            }
        }
        cout<<endl << "Directory table end <<<<<<<<<<<<" << endl
            << endl;

        cout<<endl << " SSM Begin >>>>>>>>>>>>>>>>>>>" << endl;
        cout << "ldepth\t"
             << "index\t"
             <<"sub-index\t"
             << "tid string\t"
             << "record\t"
             << endl;
        for (int i = 0; i < ssm.curEntryBucketPosition;i++){
            if(!ssm.data[i].isStale && ssm.data[i].curEmptySpace!=ssm.data[i].size)
            for (int j = 0; j < ssm.data[i].data.size()-ssm.data[i].curEmptySpace;j++){
                Record r = ssm.data[i].data[j];
                cout <<ssm.data[i].localDepth<<"\t"<<i<<"\t"<<j<<"\t"<<getHashValueAsString(r,16)<<"\t"<<r<< endl;
                // cout <<i<<") "<< r.transactionID << " " << r.name << " " << r.transactionID << " " << r.category << endl;
                // cout << "depth" << ssm.data[i].localDepth << endl;
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
    for (int i = 0; i < 100;i++)
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


