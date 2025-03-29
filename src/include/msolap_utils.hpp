#pragma once

#include "duckdb.hpp"
#include <windows.h>
#include <oledb.h>
#include <comdef.h>
#include <string>
#include <optional>

namespace duckdb {

// Safe release template for COM objects
template <class T>
inline void SafeRelease(T** ppT) {
    if (*ppT) {
        (*ppT)->Release();
        *ppT = NULL;
    }
}

// Convert HRESULT to readable error message
std::string GetErrorMessage(HRESULT hr);

// Convert BSTR to std::string
std::string BSTRToString(BSTR bstr);

// Convert std::string to BSTR
BSTR StringToBSTR(const std::string& str);

// Type conversion utilities
int64_t ConvertVariantToInt64(VARIANT* var);
double ConvertVariantToDouble(VARIANT* var);
string_t ConvertVariantToString(VARIANT* var, Vector& result_vector);
timestamp_t ConvertVariantToTimestamp(VARIANT* var);
bool ConvertVariantToBool(VARIANT* var);

// Map DBTYPE to DuckDB LogicalType
LogicalType DBTypeToLogicalType(DBTYPE dbType);

// COM initialization guard
class COMInitializer {
public:
    COMInitializer();
    ~COMInitializer();
    bool IsInitialized() const;
    
private:
    bool initialized;
};

// Exception class for OLE DB errors
class MSOLAPException : public std::exception {
public:
    explicit MSOLAPException(const std::string& message);
    explicit MSOLAPException(HRESULT hr, const std::string& context = "");
    const char* what() const noexcept override;
    
private:
    std::string message;
};

} // namespace duckdb