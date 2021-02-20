#include<iostream>

using namespace std;

//>>>>>> global parameters. change only these params
const int maxDirectoryEntriesInMainMemory=2;
const int maxSizeOfSSMBucketArray = 1024;
const int maxBucketCapacityForRecords = 1;
const int maxBucketCapacityForDirectoryEntries=3;
const string nameOfTAInputFile = "test.csv";
const int countRandomRecord = 200;
const string nameOFSelfGeneratedFile = "test_generated.csv";
//<<<<<< do not change any other values apart from these


void  getSSMBucketIndexOffsetForDirectory(int i){
    ++i;
    int ssmIndex, bucketOffset;
    bucketOffset = (i -1- (maxDirectoryEntriesInMainMemory)) % (maxBucketCapacityForDirectoryEntries);
    float f = floor((i-1  - (maxDirectoryEntriesInMainMemory)) / maxBucketCapacityForDirectoryEntries);
    ssmIndex = f;
    ssmIndex = maxSizeOfSSMBucketArray-ssmIndex-1;
    cout << ssmIndex << " " << bucketOffset << endl;
}


int main(){
    while(true){
        int n;
        cin >> n;
        getSSMBucketIndexOffsetForDirectory(n);
    }
}