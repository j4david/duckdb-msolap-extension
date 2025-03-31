cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME msolap)

# Set C++ standard (requirement for <optional>)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
add_compile_options(/std:c++17)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

# Add COM/OLE DB dependencies for Windows
if(WIN32)
  set(COM_LIBS ole32 oleaut32 uuid)
else()
  message(FATAL_ERROR "DuckDB MSOLAP Extension is only supported on Windows due to COM dependencies")
endif()

set(EXTENSION_SOURCES 
    src/msolap_db.cpp
    src/msolap_stmt.cpp
    src/msolap_scanner.cpp
    src/msolap_utils.cpp
    src/msolap_extension.cpp
)

add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})
set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${LOADABLE_EXTENSION_NAME} ${COM_LIBS})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
