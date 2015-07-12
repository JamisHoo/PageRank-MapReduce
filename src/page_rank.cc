/******************************************************************************
 *  Copyright (c) 2015 Jamis Hoo
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: PageRank
 *  Filename: page_rank.cc 
 *  Version: 1.0
 *  Author: Jamis Hoo
 *  E-mail: hoojamis@gmail.com
 *  Date: Jul 11, 2015
 *  Time: 14:45:49
 *  Description: mapper and reducer functions running on MapReduce
 *****************************************************************************/
#include <iostream>
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
class PageRankMapper: public HadoopPipes::Mapper {
public:
    PageRankMapper(HadoopPipes::TaskContext& /* context */) { }
    
    void map(HadoopPipes::MapContext& context) {
        std::string input_value = context.getInputValue();

        size_t offset = 0;

        // URL
        uint32_t URL = network_to_host(input_value.data() + offset);
        offset += sizeof(URL);

        // tab
        char space = *(char*)(input_value.data() + offset);
        offset += sizeof(space);
        if (space != '\t') return;

        // initial PR
        float pr = *(float*)(input_value.data() + offset);
        offset += sizeof(pr);

        // construct emit key
        std::string emit_key(sizeof(URL), 0);
        host_to_network(URL, emit_key);

        // emit url_list
        context.emit(emit_key, url_list_header + std::string(input_value.data() + offset, input_value.length() - offset));

        size_t url_list_size = (input_value.length() - offset) / sizeof(uint32_t);

        // compute new pr
        float new_pr = pr / url_list_size;
        
        // construct emit value
        std::string emit_value(sizeof(new_pr), 0);
        memcpy(&emit_value[0], &new_pr, sizeof(new_pr));

        // URL list
        while (offset + sizeof(uint32_t) <= input_value.length()) {
            // construct emit key (url)
            emit_key[0] = input_value[offset++];
            emit_key[1] = input_value[offset++];
            emit_key[2] = input_value[offset++];
            emit_key[3] = input_value[offset++];

            context.emit(emit_key, emit_value);
        }
    }
};

/* Reducer input format:
   Key = URL
   Value = 4-Byte floating point defined in IEEE 754
        or url list header + URL_list
   RUL is a 4-Byte unsigned integer in big-endian
*/
class PageRankReducer: public HadoopPipes::Reducer {
public:
    PageRankReducer(HadoopPipes::TaskContext& /* context */) { }
    
    void reduce(HadoopPipes::ReduceContext& context) {
        constexpr float d = 0.85f;

        uint32_t URL = network_to_host(context.getInputKey().data());

        std::string input_value;
        std::string url_list;
        
        float pr = 0.0f;

        while (context.nextValue()) {
            input_value = context.getInputValue();

            // url list
            if (input_value.length() >= url_list_header.length()) 
                url_list = input_value.substr(url_list_header.length());
            else {
                assert(input_value.length() == 4);
                pr += *(float*)(input_value.data());
            }
        }

        pr = 1.0f - d + d * pr;

        // construct emit key
        std::string emit_key(sizeof(URL), 0);
        host_to_network(URL, emit_key);

        // and emit value
        std::string emit_value(sizeof(pr), 0);
        emit_value += url_list;
        emit_value += delimiter;
        memcpy(&emit_value[0], &pr, sizeof(pr));

        context.emit(emit_key, emit_value);
    }
};

int main(int, char**) {
    return HadoopPipes::runTask(HadoopPipes::TemplateFactory<PageRankMapper, PageRankReducer>());
}
