#include <sstream>

namespace rdma {

typedef long long int64;

const int DEBUG = 0;
const int INFO = 1;
const int WARNING = 2;
const int ERROR = 3;
const int FATAL = 4;

class LogMessage : public std::basic_ostringstream<char> {
 public:
  LogMessage(const char* fname, int line, int severity);
  ~LogMessage();

 protected:
  void GenerateLogMessage();

 private:
  const char* fname_;
  int line_;
  int severity_;
};


#define LOG_DEBUG   LogMessage(__FILE__, __LINE__, DEBUG)
#define LOG_INFO    LogMessage(__FILE__, __LINE__, INFO)
#define LOG_WARNING LogMessage(__FILE__, __LINE__, WARNING)
#define LOG_ERROR   LogMessage(__FILE__, __LINE__, ERROR)
#define LOG_FATAL   LogMessage(__FILE__, __LINE__, FATAL)

#define PREDICT_FALSE(x) (__builtin_expect(x, 0))
#define CHECK(condition)  if (PREDICT_FALSE(!(condition)))  LOG_FATAL << "Check failed: " #condition " "

}
