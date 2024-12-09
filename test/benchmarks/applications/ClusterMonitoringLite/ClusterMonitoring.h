#pragma once

#include <iostream>
#include <fstream>

#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/bzip2.hpp>
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>

#include "utils/TupleSchema.h"
#include "utils/QueryApplication.h"
#include "utils/Utils.h"
#include "benchmarks/applications/BenchmarkQuery.h"

class ClusterMonitoring : public BenchmarkQuery {
 private:
  struct InputSchema {
    long timestamp;
    long jobId;
    long taskId;
    long machineId;
    int eventType;
    int userId;
    int category;
    int priority;
    float cpu;

    static void parse(InputSchema &tuple, std::string &line) {
      std::istringstream iss(line);
      std::vector<std::string> words{std::istream_iterator<std::string>{iss},
                                     std::istream_iterator<std::string>{}};
      tuple.timestamp = std::stol(words[0]);
      tuple.jobId = std::stol(words[1]);
      tuple.taskId = std::stol(words[2]);
      tuple.machineId = std::stol(words[3]);
      tuple.eventType = std::stoi(words[4]);
      tuple.userId = std::stoi(words[5]);
      tuple.category = std::stoi(words[6]);
      tuple.priority = std::stoi(words[7]);
      tuple.cpu = std::stof(words[8]);
    }
  };

 public:
  TupleSchema *m_schema = nullptr;
  QueryApplication *m_application = nullptr;
  std::vector<char> *m_data = nullptr;
  bool m_debug = false;

  QueryApplication *getApplication() override {
    return m_application;
  }

  virtual void createApplication() = 0;

  void loadInMemoryData() {
    size_t len = SystemConf::getInstance().BUNDLE_SIZE;
    m_data = new std::vector<char>(len);
    auto buf = (InputSchema *) m_data->data();

    std::string filePath = Utils::getHomeDir() + "/LightSaber/resources/datasets/google-cluster-data/";
    std::ifstream file(filePath + "google-cluster-data.txt");
    // std::cout << filePath << std::endl;
    if (!file.good())
      throw std::runtime_error("error: input file does not exist, check the path.");
    std::string line;
    unsigned long idx = 0;
    while (std::getline(file, line) && idx < len / sizeof(InputSchema)) {
      InputSchema::parse(buf[idx], line);
      if (m_startTimestamp == 0) {
        m_startTimestamp = buf[0].timestamp;
      }
      m_endTimestamp = buf[idx].timestamp;
      idx++;
    }

    if (idx < len / sizeof(InputSchema)) {
      unsigned long iter = 0;
      auto barrier = idx-1;
      long lastTime = buf[idx-1].timestamp;
      while (idx < len / sizeof(InputSchema)) {
        std::memcpy(&buf[idx], &buf[iter], sizeof(InputSchema));
        buf[idx].timestamp += lastTime;
        m_endTimestamp = buf[idx].timestamp;
        idx++;
        iter++;
        if (iter == barrier) {
          iter = 0;
          lastTime = buf[idx-1].timestamp;
        }
      }
    }

    if (m_debug) {
      std::cout << "timestamp jobId machineId eventType userId category priority cpu" << std::endl;
      for (unsigned long i = 0; i < m_data->size() / sizeof(InputSchema); ++i) {
        printf("[DBG] %09d: %7d %13d %8d %13d %3d %6d %2d %2d %8.3f \n",
               i, buf[i].timestamp, buf[i].jobId, buf[i].taskId, buf[i].machineId,
               buf[i].eventType, buf[i].userId, buf[i].category, buf[i].priority,
               buf[i].cpu);
      }
    }

    //std::ifstream file(filePath + "compressed-512-norm.dat",
    //                   std::ios_base::in | std::ios_base::binary);
    //try {
    //    boost::iostreams::filtering_istream in;
    //    in.push(boost::iostreams::gzip_decompressor());
    //    in.push(file);
    //    for(std::string str; std::getline(in, str); ) {
    //        std::cout << "Processed line " << str << '\n';
    //    }
    //}
    //catch(const boost::iostreams::gzip_error& e) {
    //    std::cout << e.what() << '\n';
    //}
  };

  std::vector<char> *getInMemoryData() override {
    return m_data;
  }

  std::vector<char> *getStaticData() override {
    throw std::runtime_error("error: this benchmark does not have static data");
  }

  TupleSchema *getSchema() override {
    if (m_schema == nullptr)
      createSchema();
    return m_schema;
  }

  void createSchema() {
    m_schema = new TupleSchema(9, "ClusterMonitoring");
    auto longAttr = AttributeType(BasicType::Long);
    auto intAttr = AttributeType(BasicType::Integer);
    auto floatAttr = AttributeType(BasicType::Float);

    m_schema->setAttributeType(0, longAttr);  /*   timestamp:  long */
    m_schema->setAttributeType(1, longAttr);  /*       jobId:  long */
    m_schema->setAttributeType(2, longAttr);  /*      taskId:  long */
    m_schema->setAttributeType(3, longAttr);  /*   machineId:  long */
    m_schema->setAttributeType(4, intAttr);   /*   eventType:   int */
    m_schema->setAttributeType(5, intAttr);   /*      userId:   int */
    m_schema->setAttributeType(6, intAttr);   /*    category:   int */
    m_schema->setAttributeType(7, intAttr);   /*    priority:   int */
    m_schema->setAttributeType(8, floatAttr); /*         cpu: float */

    m_schema->setAttributeName(0, "timestamp");
    m_schema->setAttributeName(1, "jobId");
    m_schema->setAttributeName(2, "taskId");
    m_schema->setAttributeName(3, "machineId");
    m_schema->setAttributeName(4, "eventType");
    m_schema->setAttributeName(5, "userId");
    m_schema->setAttributeName(6, "category");
    m_schema->setAttributeName(7, "priority");
    m_schema->setAttributeName(8, "cpu");
  }
};
