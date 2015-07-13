vpath %.cc src
CXXFLAGS += -std=c++11 -Wextra -O2

HADOOP_SRC_DIR = ./include/

all: pagerank_iter pagerank_resultsort

pagerank_iter: page_rank.cc
	$(CXX) $(CXXFLAGS) $? -o$@ -I $(HADOOP_SRC_DIR) $(HADOOP_SRC_DIR)/hadoop/*.cc -lcrypto -lpthread
pagerank_resultsort: pagerank_result_sort.cc
	$(CXX) $(CXXFLAGS) $? -o$@ -I $(HADOOP_SRC_DIR) $(HADOOP_SRC_DIR)/hadoop/*.cc -lcrypto -lpthread

clean:
	$(RM) main
