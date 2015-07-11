/******************************************************************************
 *  Copyright (c) 2015 Jamis Hoo
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: 
 *  Filename: page_per_line.cc 
 *  Version: 1.0
 *  Author: Jamis Hoo
 *  E-mail: hoojamis@gmail.com
 *  Date: Jul 11, 2015
 *  Time: 19:34:30
 *  Description: 
 *****************************************************************************/
#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>



int main(int argc, char** argv) {
    std::ios_base::sync_with_stdio(false);

    if (argc < 2) return 0;

    std::ifstream fin(argv[1]);

    std::ofstream fout("page_per_line");

    std::string page;
    std::string line;
    int state = 0;

    while (getline(fin, line)) {
        switch (state) {
            case 0:
                if (line == "  <page>") {
                    state = 1;
                    page += line.substr(2);
                }
                break;
            case 1:
                page += line;
                if (line == "  </page>") {
                    state = 0;
                    page += '\n';
                    fout.write(page.data(), page.length());
                    page.clear();
                }
        }
    }

}

