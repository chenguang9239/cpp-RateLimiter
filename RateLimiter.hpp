#ifndef RATELIMITER_H
#define RATELIMITER_H

#include <unordered_map>
#include <string>
#include <memory>
#include <boost/thread/thread.hpp> 
#include <stdint.h>
#include <atomic>
#include <chrono>

class Bucket {
 private:
  std::atomic<double> zero_time_;
 public:
  Bucket(double zero_time = 0.0) :
      zero_time_(zero_time) {};
  bool aquire(double now, double rate, double burst_size, double consume = 1.0) {
    auto zero_time_old = zero_time_.load();
    double zero_time_new;
    do {
      double tokens = std::min((now - zero_time_old) * rate, burst_size);
      if (tokens < consume) {
        return false;
      } else {
        tokens -= consume;
      }
      zero_time_new = now - tokens / rate;
    } while(!zero_time_.compare_exchange_weak(zero_time_old, zero_time_new));

    return true;
  }
};

template <class T=std::string>
class RateLimiter {
 private:
  double rate_;
  double burst_size_;

  std::unordered_map<T, std::unique_ptr<Bucket> > key_to_bucket_;
  boost::shared_mutex key_to_bucket_mu_;

  static double now() {
    using dur = std::chrono::duration<double>;
    auto const now = std::chrono::steady_clock::now().time_since_epoch();
    return std::chrono::duration_cast<dur>(now).count();
  }
 public:
  RateLimiter(double rate, double burst_size) :
      rate_(rate),
      burst_size_(burst_size) {};

  bool aquire(T &key) {
    boost::upgrade_lock<boost::shared_mutex> lock(key_to_bucket_mu_);

    auto bucket_it = key_to_bucket_.find(key);
    if (bucket_it == key_to_bucket_.end()) {
      boost::upgrade_to_unique_lock<boost::shared_mutex> unique_lock(lock);
      std::unique_ptr<Bucket> bucket(new Bucket());
      key_to_bucket_[key] = std::move(bucket);
      return true;
    } else {
      return bucket_it->second->aquire(now(), rate_, burst_size_);
    }
  };
};

#endif
