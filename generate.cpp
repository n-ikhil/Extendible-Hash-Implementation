#include <iostream>
#include <fstream>
#include "logic.cpp"


using namespace std;


int main(){
    int count = 200;
    fstream fout;
    fout.open(nameOfOutputFile, ios::out);
    for (int i = 0; i < count; i++)
    {
        Record r = generateRandomRecord(i);
        fout << i << ","
             << r.amount << ","
             << r.category<< ","
             << r.name 
             << "\n";
    }
    fout.close();
}