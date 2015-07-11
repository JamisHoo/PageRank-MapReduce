/******************************************************************************
 *  Copyright (c) 2015 Jamis Hoo
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: PageRank
 *  Filename: convert_to_bin.cc 
 *  Version: 1.0
 *  Author: Jamis Hoo
 *  E-mail: hoojamis@gmail.com
 *  Date: Jul 11, 2015
 *  Time: 16:42:09
 *  Description: 
 *****************************************************************************/
#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>

std::unordered_map<std::string, uint32_t> dict;
std::vector<std::string> inverted_dict;

std::ofstream bin_input;


inline uint32_t find_no(const std::string& url) {
    auto ite = dict.find(url);
    if (ite == dict.end()) {
        uint32_t no = dict.size();
        dict.emplace(url, no);
        inverted_dict.emplace_back(url);
        return no;
    }
    return ite->second;
}

inline void output_uint32(const uint32_t x) {
    bin_input.put(x >> 24);
    bin_input.put(x >> 16);
    bin_input.put(x >>  8);
    bin_input.put(x >>  0);
}

inline void output_float(const float x) {
    uint8_t* xx = (uint8_t*)(&x);
    bin_input.put(xx[0]);
    bin_input.put(xx[1]);
    bin_input.put(xx[2]);
    bin_input.put(xx[3]);
}

inline void input(const char* line, const size_t line_length) {
    const char* header = "<page>    <title>";
    const char* tailer = "</title>";
    const size_t header_length = 17;
    const size_t tailer_length = 8;

    if (line_length < header_length + tailer_length) return;

    size_t i;

    for (i = 0; i < header_length; ++i)
        if (line[i] != header[i]) return;


    size_t match_length = 0;
    for (i = header_length; i < line_length; ++i)
        if (line[i] == tailer[match_length]) {
            ++match_length;
            if (match_length == tailer_length) break;
        } else {
            match_length = 0;
        }

    if (match_length != tailer_length) return;

    ++i;
    
    std::string URL(line + header_length, i - header_length - tailer_length);

    uint32_t URL_no = find_no(URL);

    output_uint32(URL_no);
    bin_input.put('\t');
    output_float(1.0f);

    while (i < line_length) {
        for (; i < line_length - 1; ++i) 
            if (line[i] == '[' && line[i + 1] == '[') break;
        size_t j;
        for (j = i + 2; j < line_length - 1; ++j)
            if (line[j] == ']' && line[j + 1] == ']') {
                // [i, j] is a url
                uint32_t url_no = find_no(std::string(line + i + 2, j - i - 2));
                output_uint32(url_no);    
                i = j + 2;
                break;
            }

        if (j >= line_length - 1) break;
    }
    bin_input.put('\n');
}


int main(int argc, char** argv) {
    std::ios_base::sync_with_stdio(false);

    if (argc < 2) return 0;

    std::ifstream fin(argv[1]);

    bin_input.open("bin_input");

    size_t counter = 0;

    std::string line;
    while (getline(fin, line)) {
        if (counter % 8 == 0)
            std::cout << counter++ << std::endl;
        input(line.data(), line.length());
    }

    // output dict
    std::ofstream fout("dictionary");
    for (const auto& i: inverted_dict) 
        fout << i << '\n';
}
