add_subdirectory(engine)
add_subdirectory(utils)

if (BUILD_PYTHON)
    add_subdirectory(python)
endif (BUILD_PYTHON)

if (USE_GLOO_BACKEND)
    add_subdirectory(gloo)
endif()

if (BUILD_TORCH_PLUGIN)
    add_subdirectory(torch)
endif (BUILD_TORCH_PLUGIN)
