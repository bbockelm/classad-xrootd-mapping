
include_directories( ${XROOTD_INCLUDES} ${CLASSAD_INCLUDES} ${BOOST_INCLUDES} )
add_library(classad_xrootd_mapping MODULE xrootd_mapping.cpp xrootd_client.cpp response_cache.cpp)
target_link_libraries(classad_xrootd_mapping ${XROOTD_CLIENT} ${XROOTD_UTILS} ${CLASSAD_LIB})

add_executable(classad_xrootd_mapping_tester test_main.cpp)
target_link_libraries(classad_xrootd_mapping_tester ${CLASSAD_LIB})

