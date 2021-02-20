#include "logic.cpp"
#include <fstream>
#include<string.h>
#include<sstream>

class Util
{
    public:
    void runExtendibleHashWithFileInput(){
        fstream fin;
        fin.open("dataset.csv", ios::in);
        vector<string> row;
        string line, word, temp;
        vector<Record> records;
        while (fin >> temp)
        {
            stringstream s(temp);
            row.clear();
            while (getline(s, word, ','))
                row.push_back(word);
            Record record(stoi(row[0]),stoi(row[1]),stoi(row[2]),row[3]);
            records.push_back(record);
        }
        runExtendibleHash(records);
    };

    void runExtendibleHash(vector<Record> data){
        ExtendibleHash extendibleHash;
        extendibleHash.insertAsLoop(data,false);
        // extendibleHash.visualize();
        extendibleHash.visualize2();
    }

    void runRandomVector(){
    vector<Record> test;
    Record r;

    for (int i = 1; i < 100;i++)
    {
        int j = rand() % 65000;
        r = generateRandomRecord(j);
        test.push_back(r);
    }
    runExtendibleHash(test);
    }
};

int main()
{
    Util util;

    util.runExtendibleHashWithFileInput();
    // util.runRandomVector();
}