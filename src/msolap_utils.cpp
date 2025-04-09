#include "msolap_utils.hpp"

namespace duckdb {

std::string MSOLAPUtils::SanitizeColumnName(const std::wstring &name) {
    std::wstring sanitized = name;
    for (size_t i = 0; i < sanitized.size(); i++) {
        if (sanitized[i] == L'[' || sanitized[i] == L']') {
            sanitized[i] = L'_';
        }
    }
    
    // Convert the entire string to UTF-8 properly
    return WindowsUtil::UnicodeToUTF8(sanitized.c_str());
}

Value MSOLAPUtils::ConvertVariantToValue(VARIANT* pVar) {
    if (!pVar) {
        return Value();
    }
    
    switch (pVar->vt) {
    case VT_NULL:
    case VT_EMPTY:
        return Value();
    case VT_I2:
        return Value::SMALLINT(pVar->iVal);
    case VT_I4:
        return Value::INTEGER(pVar->lVal);
    case VT_I8:
        return Value::BIGINT(pVar->llVal);
    case VT_R4:
        return Value::FLOAT(pVar->fltVal);
    case VT_R8:
        return Value::DOUBLE(pVar->dblVal);
    case VT_BOOL:
        return Value::BOOLEAN(pVar->boolVal != 0);
    case VT_BSTR:
        if (pVar->bstrVal) {
            // Convert wide string to UTF8
            std::wstring wstr(pVar->bstrVal);
            std::string utf8_str;
            try {
                utf8_str = WindowsUtil::UnicodeToUTF8(wstr.c_str());
            } catch (...) {
                // Fallback to direct conversion on error
                utf8_str = std::string(wstr.begin(), wstr.end());
            }
            return Value(utf8_str);
        } else {
            return Value("");
        }
    case VT_DATE: {
        date_t date_val = date_t(0); // Initialize with epoch
        try {
            // Convert OLE automation date to DuckDB date
            time_t epoch_seconds = (time_t)((pVar->date - 25569) * 86400); // 25569 is days between 1899-12-30 and 1970-01-01
            date_val = Date::EpochToDate(epoch_seconds);
        } catch (...) {
            // Return default date on error
        }
        return Value::DATE(date_val);
    }
    case VT_CY: {
        // Currency is a 64-bit integer scaled by 10,000
        int64_t scaled_value = ((int64_t)pVar->cyVal.Hi) << 32 | pVar->cyVal.Lo;
        double value = scaled_value / 10000.0;
        return Value::DOUBLE(value);
    }
    default:
        // For types we can't handle well, convert to string
        try {
            VARIANT varStr;
            VariantInit(&varStr);
            if (SUCCEEDED(VariantChangeType(&varStr, pVar, 0, VT_BSTR))) {
                std::wstring wstr(varStr.bstrVal ? varStr.bstrVal : L"");
                std::string str;
                try {
                    str = WindowsUtil::UnicodeToUTF8(wstr.c_str());
                } catch (...) {
                    str = std::string(wstr.begin(), wstr.end());
                }
                VariantClear(&varStr);
                return Value(str);
            }
            VariantClear(&varStr);
        } catch (...) {
            // Fallback to empty string on conversion error
        }
        return Value("");
    }
}

LogicalType MSOLAPUtils::GetLogicalTypeFromDBTYPE(DBTYPE type) {
    switch (type) {
    case DBTYPE_BOOL:
        return LogicalType::BOOLEAN;
    case DBTYPE_I1:
    case DBTYPE_UI1:
        return LogicalType::TINYINT;
    case DBTYPE_I2:
    case DBTYPE_UI2:
        return LogicalType::SMALLINT;
    case DBTYPE_I4:
    case DBTYPE_UI4:
        return LogicalType::INTEGER;
    case DBTYPE_I8:
    case DBTYPE_UI8:
        return LogicalType::BIGINT;
    case DBTYPE_R4:
        return LogicalType::FLOAT;
    case DBTYPE_R8:
    case DBTYPE_DECIMAL:
    case DBTYPE_NUMERIC:
    case DBTYPE_CY:
        return LogicalType::DOUBLE;
    case DBTYPE_DATE:
    case DBTYPE_DBDATE:
        return LogicalType::DATE;
    case DBTYPE_DBTIME:
    case DBTYPE_DBTIMESTAMP:
        return LogicalType::TIMESTAMP;
    case DBTYPE_GUID:
    case DBTYPE_WSTR:
    case DBTYPE_STR:
    case DBTYPE_BSTR:
    default:
        return LogicalType::VARCHAR;
    }
}

std::string MSOLAPUtils::GetErrorMessage(HRESULT hr) {
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


} // namespace duckdb