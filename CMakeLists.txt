cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME msolap)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

# Add COM/OLE DB dependencies for Windows
if(WIN32)
  # Extended list of libraries for OLE DB support
  if(MINGW)
    # MinGW specific libraries
    set(COM_LIBS 
        ole32 
        oleaut32 
        uuid
        # Add OLE DB related libraries
        msdaguid    # Contains OLE DB GUIDs
        # oledbguid   # Alternative/additional GUID library (if available)
    )
  else()
    # MSVC or other Windows compilers
    set(COM_LIBS 
        ole32 
        oleaut32 
        uuid
        oledb       # OLE DB library
        msdasc      # Microsoft Data Access Components
    )
  endif()
  
  set(EXTENSION_SOURCES 
      src/msolap_connection.cpp
      src/msolap_scanner.cpp
      src/msolap_utils.cpp
      src/msolap_extension.cpp
  )
else()
  # For non-Windows platforms, use a simplified implementation
  set(COM_LIBS "")
  set(EXTENSION_SOURCES 
      src/msolap_extension_nowin.cpp
  )
endif()

add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})
set(PARAMETERS "-warnings")
# build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

if(WIN32)
  target_link_libraries(${EXTENSION_NAME} ${COM_LIBS})
  target_link_libraries(${LOADABLE_EXTENSION_NAME} ${COM_LIBS})
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

