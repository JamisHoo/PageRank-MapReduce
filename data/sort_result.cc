/******************************************************************************
 *  Copyright (c) 2015 Jamis Hoo
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: 
 *  Filename: sort_result.cc 
 *  Version: 1.0
 *  Author: Jamis Hoo
 *  E-mail: hoojamis@gmail.com
 *  Date: Jul 12, 2015
 *  Time: 18:32:02
 *  Description: 
 *****************************************************************************/
#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>

int main(int, char**) {
    using namespace std;
    
    vector<string> dict;

    ifstream fin("dictionary");

    string line;
    while (getline(fin, line)) 
        dict.emplace_back(line);

    fin.close();
    fin.open("pagerank_result");


    vector< pair<float, uint32_t> > sorted_results;
    sorted_results.reserve(67108864);
    std::cout << "Memory allocated. " << std::endl;

    auto handle_line = [&sorted_results](const std::string& line) {
        uint32_t url = (uint32_t(line[0]) & 0xff)<< 24 | 
                       (uint32_t(line[1]) & 0xff)<< 16 | 
                       (uint32_t(line[2]) & 0xff)<<  8 | 
                       (uint32_t(line[3]) & 0xff)<<  0;

        float pr = *(float*)(line.data() + 5);

        sorted_results.emplace_back(make_pair(pr, url));
    };

    size_t counter = 0;


    char chr;
    int state = 0;
    line.clear();
    while (fin.get(chr)) {
        line += chr;
        switch (state) {
            case 0:
                if (chr == 'A') state = 1;
                break;
            case 1:
                if (chr == 'B') state = 2;
                else state = 0;
                break;
            case 2:
                if (chr == 'C') state = 3;
                else state = 0;
                break;
            case 3:
                if (chr == 'D') state = 4;
                else state = 0;
                break;
            case 4:
                if (chr == 'E') state = 5;
                else state = 0;
                break;
            case 5:
                if (chr == 'F') state = 6;
                else state = 0;
                break;
            case 6:
                if (chr == '\n') {
                    state = 0; 
                    line.resize(line.length() - 7);
                    handle_line(line);
                    line.clear();
                    ++counter;
                    if (counter % 131072 == 0) std::cout << counter << std::endl;
                } else state = 0;
                break;
        }
    }
    
    sort(sorted_results.begin(), sorted_results.end());

    std::ofstream fout("sorted_pagerank_result");

    for (const auto i: sorted_results) {
        if (i.second >= dict.size()) {
            std::cerr << "Error: i == " << i.second << ", sizeof(dict) == " << dict.size() << std::endl;
            return 1;
        }
        fout << dict[i.second] << ' ' << i.first << '\n';
    }
    
}
