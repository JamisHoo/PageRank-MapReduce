/******************************************************************************
 *  Copyright (c) 2015 Jamis Hoo
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: 
 *  Filename: result_view.cc 
 *  Version: 1.0
 *  Author: Jamis Hoo
 *  E-mail: hjm211324@gmail.com
 *  Date: Jul 13, 2015
 *  Time: 19:58:49
 *  Description: 
 *****************************************************************************/
#include <iostream>
#include <fstream>
#include <string>
#include <vector>


int main(int argc, char** argv) {
    using namespace std;
    
    ifstream fin("dictionary");
    
    // read in dictionary
    vector<string> dict;
    dict.reserve(67108864);
    string line;
    while (getline(fin, line)) dict.emplace_back(line);

    fin.close();
    cout << "Dictionary loaded. " << endl;

    constexpr size_t RECORD_LENGTH = 10;

    size_t top_k = 10;

    if (argc == 2) top_k = stoull(argv[1]);

    fin.open("pagerank_result");

    // get file size
    fin.seekg(0, std::ios::end);
    size_t size = fin.tellg();

    for (size_t i = 0; i < top_k; ++i) {
        size_t offset = size - (i + 1) * RECORD_LENGTH;

        fin.seekg(offset);

        char buff[RECORD_LENGTH];

        fin.read(buff, RECORD_LENGTH);
        
        swap(buff[0], buff[3]);
        swap(buff[1], buff[2]);
        swap(buff[5], buff[8]);
        swap(buff[6], buff[7]);

        float pr = *(float*)(buff);

        uint32_t dict_index = *(uint32_t*)(buff + sizeof(float) + 1);

        cout << pr << ' ' << dict[dict_index] << endl;
    }
}
