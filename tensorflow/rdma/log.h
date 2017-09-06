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

}
