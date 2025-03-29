#include "msolap_utils.hpp"
#include <comdef.h>

namespace duckdb {

std::string GetErrorMessage(HRESULT hr) {
    _com_error err(hr);
    std::wstring wstr = err.ErrorMessage();
    return std::string(wstr.begin(), wstr.end());
}

std::string BSTRToString(BSTR bstr) {
    if (!bstr) {
        return "";
    }
    
    // Convert BSTR to std::string
    std::wstring wstr(bstr);
    return std::string(wstr.begin(), wstr.end());
}

BSTR StringToBSTR(const std::string& str) {
    // Convert std::string to BSTR
    std::wstring wstr(str.begin(), str.end());
    return SysAllocString(wstr.c_str());
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
            sprintf(buffer, "%04d-%02d-%02d %02d:%02d:%02d", 
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
    
    date_t date(sysTime.wYear, sysTime.wMonth, sysTime.wDay);
    dtime_t time(sysTime.wHour, sysTime.wMinute, sysTime.wSecond, 0);
    
    return timestamp_t(date, time);
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
    switch (dbType) {
        case DBTYPE_I2:
            return LogicalType::SMALLINT;
        case DBTYPE_I4:
            return LogicalType::INTEGER;
        case DBTYPE_I8:
            return LogicalType::BIGINT;
        case DBTYPE_R4:
            return LogicalType::FLOAT;
        case DBTYPE_R8:
            return LogicalType::DOUBLE;
        case DBTYPE_BOOL:
            return LogicalType::BOOLEAN;
        case DBTYPE_BSTR:
        case DBTYPE_STR:
        case DBTYPE_WSTR:
            return LogicalType::VARCHAR;
        case DBTYPE_CY:
            return LogicalType::DECIMAL(19, 4);
        case DBTYPE_DATE:
        case DBTYPE_DBDATE:
        case DBTYPE_DBTIME:
        case DBTYPE_DBTIMESTAMP:
            return LogicalType::TIMESTAMP;
        default:
            return LogicalType::VARCHAR;
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