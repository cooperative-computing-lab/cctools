cmake_minimum_required(VERSION 3.30)

project(pledge C)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED on)


set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Wpedantic")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fdiagnostics-color=always")

include_directories(src)

set(SRCS
	src/enforcer.c
	src/list.c
	src/util.c
)

set(EXE_SRCS
	src/pledge.c
	src/list.c
	src/util.c
)
add_library(enforcer SHARED "${SRCS}")
target_link_options(enforcer PRIVATE "-ldl")

add_custom_command(
	OUTPUT libenforcer.h
	COMMAND xxd -i -n minienforcer ${CMAKE_CURRENT_BINARY_DIR}/libenforcer.so > ${CMAKE_CURRENT_LIST_DIR}/src/libenforcer.h
	DEPENDS enforcer
	WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
)


add_executable(pledge "${EXE_SRCS}" libenforcer.h)
# target_compile_options(pledge PRIVATE "-g")
