if (BUILD_ASCEND_DIRECT)
    add_subdirectory(ascend_direct)
endif()

if(BUILD_TCP)
    add_subdirectory(tcp)
endif()
