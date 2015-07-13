/******************************************************************************
 *  Copyright (c) 2015 Jamis Hoo
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: PageRank
 *  Filename: pagerank_result_sort.cc
 *  Version: 1.0
 *  Author: Jamis Hoo
 *  E-mail: hoojamis@gmail.com
 *  Date: Jul 11, 2015
 *  Time: 14:45:49
 *  Description: sort map-reduce result
 *****************************************************************************/
#include <iostream>
#include <cstring>
#include <cassert>

#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"

void host_to_network(uint32_t x, std::string& str) {
    str[0] = x >> 24;
    str[1] = x >> 16;
    str[2] = x >>  8;
    str[3] = x >>  0;
}

uint32_t network_to_host(const void* from) {
    uint8_t* network = (uint8_t*)(from);
    return uint32_t(network[0]) << 24 | 
           uint32_t(network[1]) << 16 |
           uint32_t(network[2]) <<  8 |
           uint32_t(network[3]) <<  0;
}

// assert url_list_header.length() > 4 Bytes
const std::string url_list_header = "     ";
const std::string delimiter = "ABCDEF";

/* Mapper input format:
   Key = ...
   Value = URL\t(PR, URL_list)
   PR is 4-Byte floating point defined in IEEE 754
   RUL is a 4-Byte unsigned integer in big-endian
*/
class PageRankResultSorterMapper: public HadoopPipes::Mapper {
public:
    PageRankResultSorterMapper(HadoopPipes::TaskContext& /* context */) { }
    
    void map(HadoopPipes::MapContext& context) {
        std::string input_value = context.getInputValue();
        context.emit(std::string(input_value, 0, 4), std::string(input_value, 5, 4));
    }        
};

/* Reducer input format:
   Key = URL
   Value = 4-Byte floating point defined in IEEE 754
   RUL is a 4-Byte unsigned integer in big-endian
*/
class PageRankResultSorterReducer: public HadoopPipes::Reducer {
public:
    PageRankResultSorterReducer(HadoopPipes::TaskContext& /* context */) { }
    
    void reduce(HadoopPipes::ReduceContext& context) {
        std::string input_value = context.getInputValue();

        std::string emit_key(4, 0);
        emit_key[0] = input_value[3];
        emit_key[1] = input_value[2];
        emit_key[2] = input_value[1];
        emit_key[3] = input_value[0];

        context.emit(emit_key, context.getInputKey());
    }
};

int main(int, char**) {
    return HadoopPipes::runTask(HadoopPipes::TemplateFactory<PageRankResultSorterMapper, PageRankResultSorterReducer>());
}
