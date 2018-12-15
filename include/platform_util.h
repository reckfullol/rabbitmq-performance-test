//
// Created by Reckful on 2018/11/24.
//

#ifndef RABBITMQ_PERFORMANCE_TEST_PLATFORM_UTIL_H
#define RABBITMQ_PERFORMANCE_TEST_PLATFORM_UTIL_H

#include <stdint.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <arpa/inet.h>

uint64_t now_microseconds(void);

void microsleep(int usec);

#endif //RABBITMQ_PERFORMANCE_TEST_PLATFORM_UTIL_H
