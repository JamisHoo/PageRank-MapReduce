CXXFLAGS += -std=c++11 -Wextra

HADOOP_SRC_DIR = ./include/

main: page_rank.cc
	$(CXX) $(CXXFLAGS) $? -o$@ -I $(HADOOP_SRC_DIR) $(HADOOP_SRC_DIR)/hadoop/*.cc -lcrypto -lpthread

clean:
	$(RM) main
