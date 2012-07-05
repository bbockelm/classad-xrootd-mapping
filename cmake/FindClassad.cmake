
# Find the Condor classad library

FIND_PATH(CLASSAD_INCLUDES classad/classad_distribution.h
  HINTS
  ${CONDOR_DIR}
  $ENV{CONDOR_DIR}
  /usr
  PATH_SUFFIXES include src/classad
)

FIND_LIBRARY(CLASSAD_LIB classad
  HINTS
  ${CONDOR_DIR}
  $ENV{CONDOR_DIR}
  /usr
  PATH_SUFFIXES lib src/classad
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(ClassAd DEFAULT_MSG CLASSAD_LIB CLASSAD_INCLUDES)

