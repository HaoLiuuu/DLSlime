include_guard(GLOBAL)
# One deterministic standalone Asio version across vendor containers.
set(DLSLIME_ASIO_ROOT "" CACHE PATH "Offline standalone Asio root (contains include/asio.hpp)")
if(DLSLIME_ASIO_ROOT)
    set(_asio_include "${DLSLIME_ASIO_ROOT}/include")
else()
    include(FetchContent)
    FetchContent_Declare(dlslime_asio
        URL https://codeload.github.com/chriskohlhoff/asio/tar.gz/refs/tags/asio-1-36-0
        URL_HASH SHA256=0310a76b27e1854f09f696b30de57dc490b5e1b17faed1eb8c9a2891f956e52b)
    FetchContent_GetProperties(dlslime_asio)
    if(NOT dlslime_asio_POPULATED)
        FetchContent_Populate(dlslime_asio)
    endif()
    set(_asio_include "${dlslime_asio_SOURCE_DIR}/asio/include")
endif()
if(NOT EXISTS "${_asio_include}/asio/version.hpp")
    message(FATAL_ERROR "Standalone Asio headers missing: ${_asio_include}")
endif()
file(STRINGS "${_asio_include}/asio/version.hpp" _asio_version_line REGEX "^#define ASIO_VERSION ")
if(NOT _asio_version_line MATCHES "ASIO_VERSION +103600([^0-9]|$)")
    message(FATAL_ERROR "DLSlime requires standalone Asio 1.36.0: ${_asio_version_line}")
endif()
find_package(Threads REQUIRED)
add_library(asio::asio INTERFACE IMPORTED GLOBAL)
set_target_properties(asio::asio PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${_asio_include}"
    INTERFACE_COMPILE_DEFINITIONS ASIO_STANDALONE
    INTERFACE_LINK_LIBRARIES Threads::Threads)
message(STATUS "DLSlime standalone Asio 1.36.0: ${_asio_include}")

if(BUILD_TCP)
    install(DIRECTORY "${_asio_include}/" DESTINATION include/dlslime/third_party/asio)
    if(EXISTS "${_asio_include}/../LICENSE_1_0.txt")
        install(FILES "${_asio_include}/../LICENSE_1_0.txt" DESTINATION share/licenses/dlslime/asio)
    endif()
endif()
