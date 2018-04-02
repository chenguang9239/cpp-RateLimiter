#pragma once
#include <unordered_map>
#include <string>
#include <memory>
#include <boost/thread/thread.hpp> 
#include <stdint.h>
#include <atomic>
#include <chrono>

class Bucket {
private:
    std::atomic<double> _zero_time;
public:
    Bucket(double zero_time = 0.0) :
        _zero_time(zero_time) {};
    bool aquire(double now, double rate, double burst_size, double consume = 1.0) {
        auto zero_time_old = _zero_time.load();
        double zero_time_new;
        do {
            double tokens = std::min((now - zero_time_old) * rate, burst_size);
            if (tokens < consume) {
                return false;
            } else {
                tokens -= consume;
            }
            zero_time_new = now - tokens / rate;
        } while(!_zero_time.compare_exchange_weak(zero_time_old, zero_time_new));

        return true;
    }
};

template <class T=std::string>
class RateLimiter {
private:
    double _rate;
    double _burst_size;

    std::unordered_map<T, std::unique_ptr<Bucket> > _key_to_bucket;
    boost::shared_mutex _key_to_bucket_mu;

    static double now() {
        using dur = std::chrono::duration<double>;
        auto const now = std::chrono::steady_clock::now().time_since_epoch();
        return std::chrono::duration_cast<dur>(now).count();
    }
public:
    RateLimiter(double rate, double burst_size) :
        _rate(rate),
        _burst_size(burst_size) {};
    bool aquire(std::string &key) {
        boost::upgrade_lock<boost::shared_mutex> lock(_key_to_bucket_mu);

        auto bucket_it = _key_to_bucket.find(key);
        if (bucket_it == _key_to_bucket.end()) {
            boost::upgrade_to_unique_lock<boost::shared_mutex> unique_lock(lock);
            std::unique_ptr<Bucket> bucket(new Bucket());
            _key_to_bucket[key] = std::move(bucket);
            return true;
        } else {
            return bucket_it->second->aquire(now(), _rate, _burst_size);
        }
    };
};
