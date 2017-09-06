#include <string>
#include "log.h"

using namespace std;
namespace rdma{

int64 MinLogLevel() {
  const char* env_val = getenv("RDMA_COMMON_MIN_LOG_LEVEL");
  if (env_val == nullptr) {
    return 0;
  }

  string min_log_level(env_val);
  if      (min_log_level == "1") return 1;
  else if (min_log_level == "2") return 2;
  else if (min_log_level == "3") return 3;
  else if (min_log_level == "4") return 4;
  else                           return 0;
}

LogMessage::LogMessage(const char* fname, int line, int severity)
    : fname_(fname), line_(line), severity_(severity) {}

void LogMessage::GenerateLogMessage() {
  fprintf(stderr, "[%c %-40s:%-3d] %s\n", "DIWEF"[severity_], fname_, line_,
          str().c_str());
}

LogMessage::~LogMessage() {
  static int64 log_level = MinLogLevel();
  if (severity_ >= log_level) {
    GenerateLogMessage();
  	if(severity_ == FATAL){
		abort();
  	}
  }
}
}
