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

#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"

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

        std::string emit_key(4, 0);
        emit_key[0] = input_value[8];
        emit_key[1] = input_value[7];
        emit_key[2] = input_value[6];
        emit_key[3] = input_value[5];

        std::string emit_value(input_value, 0, 4);

        context.emit(emit_key, emit_value);
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
        std::string emit_key = context.getInputKey();

        while (context.nextValue()) 
            context.emit(emit_key, context.getInputValue());
    }
};

int main(int, char**) {
    return HadoopPipes::runTask(HadoopPipes::TemplateFactory<PageRankResultSorterMapper, PageRankResultSorterReducer>());
}
