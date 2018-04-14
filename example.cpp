#include <string>
#include <iostream>
#include <chrono>
#include <thread>
#include "RateLimiter.hpp"

int main(int argc, char* argv[]) {
  double requests_per_second = 1.0;
  double burst_size = 2.0;

  std::string ip = "127.0.0.1";

  // std::string is default key type
  RateLimiter<std::string> rate_limiter(requests_per_second,  burst_size);
    
  std::cout << rate_limiter.aquire(ip) << std::endl; // true
  std::cout << rate_limiter.aquire(ip) << std::endl; // true (burst)
  std::cout << rate_limiter.aquire(ip) << std::endl; // true (burst)
  std::cout << rate_limiter.aquire(ip) << std::endl; // false

  std::this_thread::sleep_for(std::chrono::seconds(1));

  std::cout << rate_limiter.aquire(ip) << std::endl; // true
  std::cout << rate_limiter.aquire(ip) << std::endl; // false (burst exhausted)
}
