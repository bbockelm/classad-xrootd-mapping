#ifndef __PTHREAD_UTILS_H_
#define __PTHREAD_UTILS_H_

#include <pthread.h>

/*
 * Simple helper class for not deadlocking things.
 */
class Lock {

public:
    Lock(pthread_mutex_t & mutex) : m_mutex(mutex) {
                pthread_mutex_lock(&mutex);
    }

    ~Lock() {
        pthread_mutex_unlock(&m_mutex);
    }

private:
    // no default constructor
    Lock();

    // non-copyable.
    Lock(const Lock&);
    Lock& operator=(const Lock&);

    pthread_mutex_t & m_mutex;
};

#endif
