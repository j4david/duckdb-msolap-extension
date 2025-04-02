#include "msolap_utils.hpp"
#include <comdef.h>
namespace duckdb {

std::string GetErrorMessage(HRESULT hr) {
    _com_error err(hr);
    LPCTSTR errMsg = err.ErrorMessage();
    
    // Convert from TCHAR to std::string
    #ifdef UNICODE
    size_t size = wcslen(errMsg) + 1;
    char* buffer = new char[size];
    size_t convertedSize;
    wcstombs_s(&convertedSize, buffer, size, errMsg, size);
    std::string result(buffer);
    delete[] buffer;
    return result;
    #else
    return std::string(errMsg);
    #endif
}

std::string BSTRToString(BSTR bstr) {
    // MSOLAP_LOG("Converting BSTR to string");
    if (!bstr) {
        // MSOLAP_LOG("BSTR is null, returning empty string");
        return "";
    }
    
    try {
        // Convert using safe methods that handle null termination properly
        int wslen = ::SysStringLen(bstr);
        
        // Sanity check
        if (wslen <= 0 || wslen > 10000) {
            return "Column_unknown";
        }
        
        // Create a proper C++ wstring (handles null characters correctly)
        std::wstring wstr(bstr, wslen);
        
        // Use WideCharToMultiByte more carefully
        int bufferSize = ::WideCharToMultiByte(CP_UTF8, 0, wstr.c_str(), wstr.length(), nullptr, 0, nullptr, nullptr);
        
        if (bufferSize <= 0) {
            return "Column_unknown";
        }
        
        std::string result(bufferSize, 0);
        ::WideCharToMultiByte(CP_UTF8, 0, wstr.c_str(), wstr.length(), &result[0], bufferSize, nullptr, nullptr);
        
        // Clean the string - remove any non-printable characters 
        result.erase(
            std::remove_if(result.begin(), result.end(), 
                [](unsigned char c) { return c < 32 || c > 126; }
            ), 
            result.end()
        );
        
        return result;
    } catch (...) {
        return "Column_unknown";
    }
}

BSTR StringToBSTR(const std::string& str) {
    // Convert std::string to BSTR
    int wslen = ::MultiByteToWideChar(CP_ACP, 0, str.c_str(), str.length(), NULL, 0);
    BSTR wsdata = ::SysAllocStringLen(NULL, wslen);
    ::MultiByteToWideChar(CP_ACP, 0, str.c_str(), str.length(), wsdata, wslen);
    return wsdata;
}

int64_t ConvertVariantToInt64(VARIANT* var) {
    if (!var) {
        return 0;
    }
    
    switch (var->vt) {
        case VT_I2:
            return var->iVal;
        case VT_I4:
            return var->lVal;
        case VT_I8:
            return var->llVal;
        case VT_UI2:
            return var->uiVal;
        case VT_UI4:
            return var->ulVal;
        case VT_UI8:
            return static_cast<int64_t>(var->ullVal);
        case VT_INT:
            return var->intVal;
        case VT_UINT:
            return var->uintVal;
        case VT_R4:
            return static_cast<int64_t>(var->fltVal);
        case VT_R8:
            return static_cast<int64_t>(var->dblVal);
        case VT_BOOL:
            return var->boolVal ? 1 : 0;
        case VT_BSTR: {
            try {
                return std::stoll(BSTRToString(var->bstrVal));
            } catch (...) {
                return 0;
            }
        }
        default:
            return 0;
    }
}

double ConvertVariantToDouble(VARIANT* var) {
    if (!var) {
        return 0.0;
    }
    
    switch (var->vt) {
        case VT_I2:
            return var->iVal;
        case VT_I4:
            return var->lVal;
        case VT_I8:
            return static_cast<double>(var->llVal);
        case VT_UI2:
            return var->uiVal;
        case VT_UI4:
            return var->ulVal;
        case VT_UI8:
            return static_cast<double>(var->ullVal);
        case VT_INT:
            return var->intVal;
        case VT_UINT:
            return var->uintVal;
        case VT_R4:
            return var->fltVal;
        case VT_R8:
            return var->dblVal;
        case VT_CY:
            return static_cast<double>(var->cyVal.int64) / 10000.0;
        case VT_BOOL:
            return var->boolVal ? 1.0 : 0.0;
        case VT_BSTR: {
            try {
                return std::stod(BSTRToString(var->bstrVal));
            } catch (...) {
                return 0.0;
            }
        }
        default:
            return 0.0;
    }
}

string_t ConvertVariantToString(VARIANT* var, Vector& result_vector) {
    if (!var) {
        return string_t();
    }
    
    std::string result;
    
    switch (var->vt) {
        case VT_NULL:
            return string_t();
        case VT_EMPTY:
            return string_t();
        case VT_I2:
            result = std::to_string(var->iVal);
            break;
        case VT_I4:
            result = std::to_string(var->lVal);
            break;
        case VT_I8:
            result = std::to_string(var->llVal);
            break;
        case VT_UI2:
            result = std::to_string(var->uiVal);
            break;
        case VT_UI4:
            result = std::to_string(var->ulVal);
            break;
        case VT_UI8:
            result = std::to_string(var->ullVal);
            break;
        case VT_INT:
            result = std::to_string(var->intVal);
            break;
        case VT_UINT:
            result = std::to_string(var->uintVal);
            break;
        case VT_R4:
            result = std::to_string(var->fltVal);
            break;
        case VT_R8:
            result = std::to_string(var->dblVal);
            break;
        case VT_BOOL:
            result = var->boolVal ? "true" : "false";
            break;
        case VT_BSTR:
            result = BSTRToString(var->bstrVal);
            break;
        case VT_DATE: {
            // Convert OLE date to string
            SYSTEMTIME sysTime;
            VariantTimeToSystemTime(var->date, &sysTime);
            char buffer[128];
            sprintf_s(buffer, sizeof(buffer), "%04d-%02d-%02d %02d:%02d:%02d", 
                    sysTime.wYear, sysTime.wMonth, sysTime.wDay,
                    sysTime.wHour, sysTime.wMinute, sysTime.wSecond);
            result = buffer;
            break;
        }
        default:
            result = "[Unsupported Type]";
            break;
    }
    
    return StringVector::AddString(result_vector, result);
}

timestamp_t ConvertVariantToTimestamp(VARIANT* var) {
    if (!var || var->vt != VT_DATE) {
        return timestamp_t(0);
    }
    
    // Convert OLE date to timestamp
    SYSTEMTIME sysTime;
    VariantTimeToSystemTime(var->date, &sysTime);
    
    // Use DuckDB's date/time constructors 
    int32_t year = sysTime.wYear;
    int32_t month = sysTime.wMonth;
    int32_t day = sysTime.wDay;
    int32_t hour = sysTime.wHour;
    int32_t minute = sysTime.wMinute;
    int32_t second = sysTime.wSecond;
    int32_t microsecond = 0;
    
    date_t date = Date::FromDate(year, month, day);
    
    // Create time struct directly without using constructor with 4 arguments
    dtime_t time = dtime_t(hour * Interval::MICROS_PER_HOUR + 
                           minute * Interval::MICROS_PER_MINUTE + 
                           second * Interval::MICROS_PER_SEC +
                           microsecond);
    
    return Timestamp::FromDatetime(date, time);
}

bool ConvertVariantToBool(VARIANT* var) {
    if (!var) {
        return false;
    }
    
    switch (var->vt) {
        case VT_BOOL:
            return var->boolVal != VARIANT_FALSE;
        case VT_I2:
        case VT_I4:
        case VT_I8:
        case VT_UI2:
        case VT_UI4:
        case VT_UI8:
        case VT_INT:
        case VT_UINT:
            return ConvertVariantToInt64(var) != 0;
        case VT_R4:
        case VT_R8:
            return ConvertVariantToDouble(var) != 0.0;
        case VT_BSTR: {
            std::string str = BSTRToString(var->bstrVal);
            return !str.empty() && (str == "1" || str == "true" || str == "TRUE" || str == "True");
        }
        default:
            return false;
    }
}

LogicalType DBTypeToLogicalType(DBTYPE dbType) {
    try {
        LogicalType result;
        
        switch (dbType) {
            case DBTYPE_I2:
                result = LogicalType::SMALLINT;
                break;
            case DBTYPE_I4:
                result = LogicalType::INTEGER;
                break;
            case DBTYPE_I8:
                result = LogicalType::BIGINT;
                break;
            case DBTYPE_R4:
                result = LogicalType::FLOAT;
                break;
            case DBTYPE_R8:
                result = LogicalType::DOUBLE;
                break;
            case DBTYPE_BOOL:
                result = LogicalType::BOOLEAN;
                break;
            case DBTYPE_BSTR:
            case DBTYPE_STR:
            case DBTYPE_WSTR:
                result = LogicalType::VARCHAR;
                break;
            case DBTYPE_CY:
                result = LogicalType::DECIMAL(19, 4);
                break;
            case DBTYPE_DATE:
            case DBTYPE_DBDATE:
            case DBTYPE_DBTIME:
            case DBTYPE_DBTIMESTAMP:
                result = LogicalType::TIMESTAMP;
                break;
            default:
                result = LogicalType::VARCHAR;
                break;
        }
        
        // MSOLAP_LOG("Converted to LogicalType: " + result.ToString());
        return result;
    } catch (const std::exception& e) {
        // MSOLAP_LOG("Exception in DBTypeToLogicalType: " + std::string(e.what()));
        // Default to VARCHAR for any exception
        return LogicalType::VARCHAR;
    } catch (...) {
        return LogicalType::VARCHAR;
    }
}

std::string SanitizeColumnName(const std::string& name) {
    
    try {
        if (name.empty()) {
            return "Column_empty";
        }
        
        std::string sanitized = name;
        
        // Replace problematic characters with underscores
        for (size_t i = 0; i < sanitized.length(); i++) {
            char c = sanitized[i];
            if (c == '[' || c == ']' || c == ' ' || c == '.' || c == ',' || 
                c == ';' || c == ':' || c == '/' || c == '\\' || c == '?' || 
                c == '*' || c == '+' || c == '=' || c == '@' || c == '!' || 
                c == '%' || c == '&' || c == '(' || c == ')' || c == '<' || 
                c == '>' || c == '{' || c == '}' || c == '|' || c == '^' || 
                c == '~' || c == '`' || c == '\'' || c == '"' || c == '-') {
                sanitized[i] = '_';
            }
        }
        
        // Truncate if too long to avoid issues
        if (sanitized.length() > 64) {
            sanitized = sanitized.substr(0, 64);
        }
        
        return sanitized;
    } catch (const std::exception& e) {
        return "Column_error";
    } catch (...) {
        return "Column_error";
    }
}

COMInitializer::COMInitializer() : initialized(false) {
    HRESULT hr = CoInitializeEx(NULL, COINIT_MULTITHREADED);
    if (SUCCEEDED(hr) || hr == S_FALSE) {
        initialized = true;
    }
}

COMInitializer::~COMInitializer() {
    if (initialized) {
        CoUninitialize();
    }
}

bool COMInitializer::IsInitialized() const {
    return initialized;
}

MSOLAPException::MSOLAPException(const std::string& message) : message(message) {}

MSOLAPException::MSOLAPException(HRESULT hr, const std::string& context) {
    std::string errMsg = GetErrorMessage(hr);
    if (!context.empty()) {
        message = context + ": " + errMsg + " (HRESULT: 0x" + std::to_string(hr) + ")";
    } else {
        message = errMsg + " (HRESULT: 0x" + std::to_string(hr) + ")";
    }
}

const char* MSOLAPException::what() const noexcept {
    return message.c_str();
}

} // namespace duckdb