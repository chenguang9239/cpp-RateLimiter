#ifndef RATELIMITER_H
#define RATELIMITER_H

#include <string>
#include <memory>
#include <atomic>
#include <chrono>
#include <stdint.h>
#include <iostream>
#include <unordered_map>
//#include <boost/thread/thread.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <stdio.h>

class Bucket {
private:
    double rate;
    double burstSize;
    std::atomic<double> zero_time_;
public:
    Bucket(double rate, double burstSize, double zero_time = 0.0) :
            zero_time_(zero_time),
            rate(rate),
            burstSize(burstSize) {}

    bool aquire(double now, double consume = 1.0) {
        auto zero_time_old = zero_time_.load();
        double zero_time_new;
        do {
            double tokens = std::min((now - zero_time_old) * rate, burstSize);
            if (tokens < consume) {
                return false;
            } else {
                tokens -= consume;
            }

            if (rate != 0) {
                /*
                 * 只对秒级别的请求做限制，即1秒内的请求速率可以不均匀
                 */
                zero_time_new = now - tokens / rate;
            } else {
                zero_time_new = now;
            }

//            printf("now is: %.10lf, zero_time_old is: %.10lf, tokens is: %.10lf, rate is: %.10lf\n",
//                   now, zero_time_old, tokens, rate);

        } while (!zero_time_.compare_exchange_weak(zero_time_old, zero_time_new));
        return true;
    }

    double aquireCnt(double now, double consume = 1.0) {
        auto zero_time_old = zero_time_.load();
        double zero_time_new;
        do {
            double tokens = std::min((now - zero_time_old) * rate, burstSize);
            if (tokens < consume) {
                consume = tokens;
            }
            tokens -= consume;
            if (rate != 0) {
                /*
                 * 只对秒级别的请求做限制，即1秒内的请求速率可以不均匀
                 */
                zero_time_new = now - tokens / rate;
            } else {
                zero_time_new = now;
            }

//            printf("now is: %.10lf, zero_time_old is: %.10lf, tokens is: %.10lf, rate is: %.10lf\n",
//                   now, zero_time_old, tokens, rate);

        } while (!zero_time_.compare_exchange_weak(zero_time_old, zero_time_new));
        return consume;
    }
};

template<class T=std::string>
class RateLimiter {
    typedef std::unordered_map<T, std::shared_ptr<Bucket>> UMAPTBUCKET;
    typedef std::function<std::map<T, double>(std::vector<std::string> &, const std::string &)> ANALYZE;
private:
    bool useDefaultKey;
    // todo 需要T类型实现 << 运算符
    T defaultKey;
    double defaultRate;
    double defaultBurstSize;
    std::map<T, double> rateMap;
    std::map<T, double> burstSizeMap;
    boost::shared_mutex bucketsSMtx;
    std::shared_ptr<UMAPTBUCKET> buckets;
    unsigned short updateTimes;
    ANALYZE analyze;

    double now();

public:
    RateLimiter(double defaultRate = 3000, double defaultBurstSize = 3000) :
            updateTimes(0),
            analyze(nullptr),
            useDefaultKey(false),
            defaultRate(defaultRate),
            defaultBurstSize(defaultBurstSize),
            buckets(std::make_shared<UMAPTBUCKET>()) {}

    bool aquire();

    bool aquire(const T &key);

    double aquireCnt(double consume = 1.0);

    double aquireCnt(const T &key, double consume = 1.0);

    void setAnalyzeFunc(ANALYZE func) { analyze = func; }

    void update(std::vector<std::string> &text, const std::string &key = "");

    unsigned short getUpdateTimes() { return updateTimes; }

    bool hasDefaultKey() { return useDefaultKey;}

    T getDefaultKey() { return defaultKey;}
};

template<class T>
double RateLimiter<T>::now() {
    using dur = std::chrono::duration<double>;
    auto const now = std::chrono::steady_clock::now().time_since_epoch();
    return std::chrono::duration_cast<dur>(now).count();
}

template<class T>
bool RateLimiter<T>::aquire() {
    if (useDefaultKey) {
        return aquire(defaultKey);
    }
    std::cerr << "aquire(defaultKey) is called, but there is no defaultKey! return false!" << std::endl;
    return false;
}

template<class T>
double RateLimiter<T>::aquireCnt(double consume) {
    if (useDefaultKey) {
        return aquireCnt(defaultKey, consume);
    }
    std::cerr << "aquireCnt(defaultKey,double) is called, but there is no defaultKey! return false!" << std::endl;
    return false;
}

template<class T>
bool RateLimiter<T>::aquire(const T &key) {

    try {
        boost::shared_lock<boost::shared_mutex> g(bucketsSMtx);
        if (buckets->count(key) > 0) {
            return buckets->at(key)->aquire(now());
        }
    } catch (const std::exception &e) {
        std::cerr << "find or visit buckets exception: " << e.what() << std::endl;
        return false;
    }

    try {
        boost::unique_lock<boost::shared_mutex> g(bucketsSMtx);
        (*buckets)[key] = std::make_shared<Bucket>(defaultRate, defaultBurstSize);
        return true;
    } catch (const std::exception &e) {
        std::cerr << "insert to buckets exception: " << e.what() << std::endl;
        return false;
    }
}

template<class T>
double RateLimiter<T>::aquireCnt(const T &key, double consume) {
    try {
        boost::shared_lock<boost::shared_mutex> g(bucketsSMtx);
        if (buckets->count(key) > 0) {
            return buckets->at(key)->aquireCnt(now(), consume);
        }
    } catch (const std::exception &e) {
        std::cerr << "find or visit buckets exception: " << e.what() << std::endl;
        return 0;
    }

    try {
        boost::unique_lock<boost::shared_mutex> g(bucketsSMtx);
        (*buckets)[key] = std::make_shared<Bucket>(defaultRate, defaultBurstSize);
        return buckets->at(key)->aquireCnt(now(), consume);
    } catch (const std::exception &e) {
        std::cerr << "insert to buckets exception: " << e.what() << std::endl;
        return 0;
    }
}

template<class T>
void RateLimiter<T>::update(std::vector<std::string> &text, const std::string &key) {
    if (analyze != nullptr) {
        rateMap = analyze(text, key);
    } else {
        std::cerr << "function used to analyze new data is nullptr!" << std::endl;
    }

    // 如果只有一条配置(默认已经取到了正确的配置)，需要设置defaultKey
    if (rateMap.size() == 1) {
        auto it = rateMap.begin();
        defaultKey = it->first;
        useDefaultKey = true;
    }

    UMAPTBUCKET newBuckets;
    for (auto &e: rateMap) {
        newBuckets[e.first] = std::make_shared<Bucket>(e.second, e.second);
    }

    auto tmpHolder = buckets;
    try {
        boost::unique_lock<boost::shared_mutex> g(bucketsSMtx);
        buckets = std::make_shared<UMAPTBUCKET>(std::move(newBuckets));
    } catch (const std::exception &e) {
        std::cerr << "update buckets exception: " << e.what() << std::endl;
    }

    ++updateTimes;
}

#endif
