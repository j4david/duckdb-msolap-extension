//===----------------------------------------------------------------------===//
//                         DuckDB
//
// msolap_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include <windows.h>
#include <oledb.h>
#include <oledberr.h>
#include <comdef.h>
#include "duckdb/common/windows_util.hpp"

namespace duckdb {

class MSOLAPUtils {
public:
    // Convert DBTYPE to string for debugging
    static std::string DBTypeToString(DBTYPE type);
    
    // Sanitize column names - replace brackets with underscores
    static std::string SanitizeColumnName(const std::wstring &name);
    
    // Convert VARIANT to DuckDB Value
    static Value ConvertVariantToValue(VARIANT* pVar);
    
    // Get DuckDB LogicalType from DBTYPE
    static LogicalType GetLogicalTypeFromDBTYPE(DBTYPE type);
    
    // Get error message from HRESULT
    static std::string GetErrorMessage(HRESULT hr);
    
    // Helper to safely release COM interfaces
    template <class T>
    static void SafeRelease(T** ppT) {
        if (*ppT) {
            (*ppT)->Release();
            *ppT = NULL;
        }
    }
};

// Structure for column data when using variants
struct ColumnData {
    DBSTATUS dwStatus;
    DBLENGTH dwLength;
    VARIANT var;
};

} // namespace duckdb