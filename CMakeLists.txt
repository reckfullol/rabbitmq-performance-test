if(${APPLE})
    cmake_minimum_required(VERSION 3.12)
    set(CMAKE_CXX_STANDARD 17)
else()
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
endif()

project(rabbitmq_performance_test)


set(PT_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/include)

set(PT_SRC_DIR ${PROJECT_SOURCE_DIR}/src)

set(PT_LIB_DIR ${PROJECT_SOURCE_DIR}/lib)


add_subdirectory(${PT_SRC_DIR})