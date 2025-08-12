#ifndef MSOLAP_GUIDS_H
#define MSOLAP_GUIDS_H

#ifdef _WIN32

// Ensure we're defining the GUIDs, not just declaring them
#ifdef INITGUID
#undef INITGUID
#endif
#define INITGUID

#include <guiddef.h>

// Define the OLE DB GUIDs that MinGW is missing
#ifdef __MINGW32__

// DBPROPSET_DBINIT
DEFINE_GUID(DBPROPSET_DBINIT, 
    0xc8b522bc, 0x5cf3, 0x11ce, 0xad, 0xe5, 0x00, 0xaa, 0x00, 0x44, 0x77, 0x3d);

// DBPROPSET_ROWSET
DEFINE_GUID(DBPROPSET_ROWSET,
    0xc8b522be, 0x5cf3, 0x11ce, 0xad, 0xe5, 0x00, 0xaa, 0x00, 0x44, 0x77, 0x3d);

// DBGUID_DEFAULT
DEFINE_GUID(DBGUID_DEFAULT,
    0xc8b521fb, 0x5cf3, 0x11ce, 0xad, 0xe5, 0x00, 0xaa, 0x00, 0x44, 0x77, 0x3d);

#endif // __MINGW32__

#undef INITGUID

#endif // _WIN32

#endif // MSOLAP_GUIDS_H

