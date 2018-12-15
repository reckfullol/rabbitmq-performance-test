include_directories(${PT_INCLUDE_DIR})

aux_source_directory(${PT_SRC_DIR}/util COMMON_SRCS)

if(${APPLE})
    set(STATIC_LIBS ${PT_LIB_DIR}/osx/librabbitmq.a -lSystem)
else()
    if(COMMAND cmake_policy)
        cmake_policy(SET CMP0003 NEW)
    endif(COMMAND cmake_policy)
    set(STATIC_LIBS ${PT_LIB_DIR}/linux/librabbitmq.a -lrt)
endif()

add_executable(amqp_statistical_reply ${PT_SRC_DIR}/pt/amqp_statistical_reply.cc ${COMMON_SRCS})
target_link_libraries(amqp_statistical_reply ${STATIC_LIBS})

add_executable(amqp_statistical_send_v2 ${PT_SRC_DIR}/pt/amqp_statistical_send_v2.cc ${COMMON_SRCS})
target_link_libraries(amqp_statistical_send_v2 ${STATIC_LIBS})

add_executable(amqp_statistical_recv_v2 ${PT_SRC_DIR}/pt/amqp_statistical_recv_v2.cc ${COMMON_SRCS})
target_link_libraries(amqp_statistical_recv_v2 ${STATIC_LIBS})
