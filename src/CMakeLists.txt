
include_directories( ${XROOTD_INCLUDES} )
add_library(classad_xrootd_mapping MODULE xrootd_mapping.cpp xrootd_client.cpp)
target_link_libraries(classad_xrootd_mapping XrdClient)

