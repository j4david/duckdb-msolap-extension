#pragma once

#include <string>
#include <exception>
#include <stdint.h>
#include <windows.h>
#include <oledb.h>
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/timestamp.hpp"

// OLE DB error codes
#ifndef DB_S_ENDOFROWSET
#define DB_S_ENDOFROWSET 0x00040EC6L
#endif

namespace duckdb {

// Define the MSOLAP CLSID for MSOLAP.8
const CLSID CLSID_MSOLAP = 
{ 0xDBC724B0, 0xDD86, 0x4772, { 0xBB, 0x5A, 0xFC, 0xC6, 0xCA, 0xB2, 0xFC, 0x1A } };

// Class to handle COM initialization and cleanup
class COMInitializer {
public:
    COMInitializer();
    ~COMInitializer();
    
    bool IsInitialized() const;
    
private:
    bool initialized;
};

// Exception class for MSOLAP errors
class MSOLAPException : public std::exception {
public:
    MSOLAPException(const std::string& message);
    MSOLAPException(HRESULT hr, const std::string& context = "");
    
    const char* what() const noexcept override;
    
private:
    std::string message;
};

// Utility functions for MSOLAP
std::string GetErrorMessage(HRESULT hr);
std::string BSTRToString(BSTR bstr);
BSTR StringToBSTR(const std::string& str);

// Data type conversion functions
int64_t ConvertVariantToInt64(VARIANT* var);
double ConvertVariantToDouble(VARIANT* var);
string_t ConvertVariantToString(VARIANT* var, Vector& result_vector);
timestamp_t ConvertVariantToTimestamp(VARIANT* var);
bool ConvertVariantToBool(VARIANT* var);

// DB type to logical type conversion
LogicalType DBTypeToLogicalType(DBTYPE dbType);

} // namespace duckdb

// COM object release helper - defined in global namespace to match Windows convention
template <class T>
inline void SafeRelease(T** ppT) {
    if (*ppT) {
        (*ppT)->Release();
        *ppT = NULL;
    }
}