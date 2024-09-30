# ---------------------------------------------------------------------------
# btrblocks
# ---------------------------------------------------------------------------

include(ExternalProject)
find_package(Git REQUIRED)

# Get rapidjson
ExternalProject_Add(
    tbb_src
    PREFIX "vendor/intel"
    GIT_REPOSITORY "https://github.com/oneapi-src/oneTBB.git"
    GIT_TAG 2a7e0dbe46855b75497355ed3808c31af14a35b6
    TIMEOUT 10
    BUILD_COMMAND  make
    UPDATE_COMMAND "" # to prevent rebuilding everytime
    INSTALL_COMMAND ""
    CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/vendor/intel
    -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
    -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
    -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
)

# Prepare json
ExternalProject_Get_Property(tbb_src source_dir)
ExternalProject_Get_Property(tbb_src binary_dir)

set(TBB_INCLUDE_DIR ${source_dir}/include)
set(TBB_LIBRARY_PATH ${binary_dir}/gnu_13.2_cxx11_64_relwithdebinfo/libtbb.so)

file(MAKE_DIRECTORY ${TBB_INCLUDE_DIR})

add_library(tbb SHARED IMPORTED)
add_dependencies(tbb tbb_src)

set_property(TARGET tbb PROPERTY IMPORTED_LOCATION ${TBB_LIBRARY_PATH})
set_property(TARGET tbb APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${TBB_INCLUDE_DIR})
