include(${CMAKE_CURRENT_LIST_DIR}/../../../../cmake/StandaloneAsio.cmake)

add_library(_slime_tcp SHARED
    tcp_memory_pool.cpp
    tcp_connection_pool.cpp
    tcp_context.cpp
    tcp_session.cpp
    tcp_endpoint.cpp
)

target_compile_definitions(_slime_tcp PRIVATE ASIO_STANDALONE)

if (USE_CUDA)
    find_package(CUDAToolkit REQUIRED)
    target_compile_definitions(_slime_tcp PRIVATE USE_CUDA)
    target_include_directories(_slime_tcp PRIVATE ${CUDAToolkit_INCLUDE_DIRS})
    target_link_libraries(_slime_tcp PUBLIC CUDA::cudart)
endif()

target_link_libraries(_slime_tcp PUBLIC
    asio::asio
    _slime_device
    _slime_engine
)

set_target_properties(_slime_tcp PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "${ORIGIN}"
)

install(TARGETS _slime_tcp
    EXPORT dlslimeTargets
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)
