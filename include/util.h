//
// Created by Reckful on 2018/11/23.
//

#ifndef RABBITMQ_PERFORMANCE_TEST_UTIL_H
#define RABBITMQ_PERFORMANCE_TEST_UTIL_H

#include <ctype.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <amqp.h>
#include <amqp_framing.h>
#include <stdint.h>

#include "platform_util.h"

void die(const char *fmt, ...);
extern void die_on_error(int x, char const *context);
extern void die_on_amqp_error(amqp_rpc_reply_t x, char const *context);

extern void amqp_dump(void const *buffer, size_t len);

extern uint64_t now_microseconds(void);
extern void microsleep(int usec);

int ReadLong(const void* pvBuffer, uint64_t *pulVal, int iToHostOrder/* = 1*/);
int WriteLong(void* pvBuffer, uint64_t ulVal, int iToNetOrder/* = 1 */);
#endif //RABBITMQ_PERFORMANCE_TEST_UTIL_H
