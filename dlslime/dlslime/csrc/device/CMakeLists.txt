set(DEVICE_SRCS "")
set(DEVICE_LIBS "")

if (USE_CUDA)
    message(STATUS "Building DLSlime with CUDA Backend")
    enable_language(CUDA)
    set(CMAKE_CUDA_STANDARD 17)
    set(CMAKE_CUDA_ARCHITECTURES "90")
    find_package(CUDAToolkit REQUIRED)
    include_directories(${CUDAToolkit_INCLUDE_DIRS})
    list(APPEND DEVICE_SRCS cuda/device_cuda.cpp)
    list(APPEND DEVICE_LIBS CUDA::cudart CUDA::cuda_driver cuda)
    add_compile_definitions("SLIME_USE_CUDA")

elseif (USE_MACA)
    message(STATUS "Building DLSlime under meta platform.")
    enable_language(CUDA)
    set(CMAKE_CUDA_STANDARD 17)
    set(CMAKE_CUDA_ARCHITECTURES "90")
    find_package(CUDAToolkit REQUIRED)
    include_directories(${CUDAToolkit_INCLUDE_DIRS})
    list(APPEND DEVICE_SRCS cuda/device_cuda.cpp)
    list(APPEND DEVICE_LIBS CUDA::cudart CUDA::cuda_driver cuda)
    add_compile_definitions("SLIME_USE_CUDA")
    add_compile_definitions("USE_MACA")
    add_compile_definitions("SLIME_USE_MACA")

elseif (USE_ASCEND)
    message(STATUS "Building DLSlime with Ascend Backend")
    list(APPEND DEVICE_SRCS ascend/device_ascend.cpp)

elseif (USE_CAMB)
    message(STATUS "Building DLSlime with Ascend Backend")
    list(APPEND DEVICE_SRCS ascend/device_camb.cpp)

else()
    message(STATUS "Building DLSlime with CPU Backend")
    list(APPEND DEVICE_SRCS host/host_only.cpp)
endif()

add_library(
    _slime_device
    SHARED
    ${DEVICE_SRCS}
)

target_link_libraries(_slime_device PRIVATE ${DEVICE_LIBS})

set_target_properties(
    _slime_device
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "\${ORIGIN}"
)

target_include_directories(_slime_device PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>
    $<INSTALL_INTERFACE:include/dlslime>
)

install(
    TARGETS
    _slime_device
    EXPORT dlslimeTargets
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)

option(DLSLIME_RUNTIME_CUDA "Build native CUDA Driver runtime" ${USE_CUDA})
target_sources(_slime_device PRIVATE runtime/device_runtime.cpp runtime/host_runtime.cpp)
if(DLSLIME_RUNTIME_CUDA)
    find_package(CUDAToolkit REQUIRED)
    target_sources(_slime_device PRIVATE runtime/cuda_runtime.cpp)
    target_compile_definitions(_slime_device PRIVATE DLSLIME_RUNTIME_CUDA)
    target_link_libraries(_slime_device PRIVATE CUDA::cuda_driver)
endif()

option(DLSLIME_RUNTIME_MACA "Build native MACA runtime" ${USE_MACA})
if(DLSLIME_RUNTIME_MACA)
    set(DLSLIME_MACA_ROOT "/opt/maca" CACHE PATH "MACA SDK root")
    find_path(DLSLIME_MACA_INCLUDE mcr/mc_runtime_api.h HINTS ${DLSLIME_MACA_ROOT}/include REQUIRED)
    find_library(DLSLIME_MACA_LIBRARY mcruntime HINTS ${DLSLIME_MACA_ROOT}/lib REQUIRED)
    target_sources(_slime_device PRIVATE runtime/maca_runtime.cpp)
    target_compile_definitions(_slime_device PRIVATE DLSLIME_RUNTIME_MACA)
    target_include_directories(_slime_device PRIVATE ${DLSLIME_MACA_INCLUDE})
    target_link_libraries(_slime_device PRIVATE ${DLSLIME_MACA_LIBRARY})
endif()

option(DLSLIME_RUNTIME_ASCEND "Build Ascend staging runtime and HOST-only verbs policy" OFF)
if(DLSLIME_RUNTIME_ASCEND)
    set(DLSLIME_ASCEND_ROOT "/usr/local/Ascend/ascend-toolkit/latest" CACHE PATH "CANN SDK root")
    find_path(DLSLIME_ASCEND_INCLUDE acl/acl.h HINTS ${DLSLIME_ASCEND_ROOT}/include REQUIRED)
    find_library(DLSLIME_ASCEND_LIBRARY ascendcl HINTS ${DLSLIME_ASCEND_ROOT}/lib64 REQUIRED)
    find_package(Threads REQUIRED)
    target_sources(_slime_device PRIVATE runtime/ascend_runtime.cpp)
    target_compile_definitions(_slime_device PRIVATE DLSLIME_RUNTIME_ASCEND)
    target_include_directories(_slime_device PRIVATE ${DLSLIME_ASCEND_INCLUDE})
    target_link_libraries(_slime_device PRIVATE ${DLSLIME_ASCEND_LIBRARY} Threads::Threads)
endif()
