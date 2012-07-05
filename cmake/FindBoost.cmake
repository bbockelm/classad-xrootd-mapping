
# Find the boost headers we use

FIND_PATH(BOOST_INCLUDES boost/tokenizer.hpp
  HINTS
  ${BOOST_DIR}
  $ENV{BOOST_DIR}
  /usr
  PATH_SUFFIXES include
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Boost DEFAULT_MSG BOOST_INCLUDES)

