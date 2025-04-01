#include "msolap_stmt.hpp"
#include "msolap_db.hpp"
#include <comdef.h>

namespace duckdb {

MSOLAPStatement::MSOLAPStatement()
    : pICommand(nullptr),
      pICommandText(nullptr),
      pIRowset(nullptr),
      pIAccessor(nullptr),
      pColumnInfo(nullptr),
      pStringsBuffer(nullptr),
      cColumns(0),
      hAccessor(NULL),
      hRow(NULL),
      pRowData(nullptr),
      has_row(false),
      executed(false) {
}

MSOLAPStatement::MSOLAPStatement(MSOLAPDB& db, const std::string& dax_query)
    : pICommand(nullptr),
      pICommandText(nullptr),
      pIRowset(nullptr),
      pIAccessor(nullptr),
      pColumnInfo(nullptr),
      pStringsBuffer(nullptr),
      cColumns(0),
      hAccessor(NULL),
      hRow(NULL),
      pRowData(nullptr),
      has_row(false),
      executed(false) {
    
    HRESULT hr;
    
    // Create a command object
    hr = db.pIDBCreateCommand->CreateCommand(NULL, IID_ICommand, (IUnknown**)&pICommand);
    if (FAILED(hr)) {
        throw MSOLAPException(hr, "Failed to create command object");
    }
    
    // Get the ICommandText interface
    hr = pICommand->QueryInterface(IID_ICommandText, (void**)&pICommandText);
    if (FAILED(hr)) {
        ::SafeRelease(&pICommand);
        throw MSOLAPException(hr, "Failed to get ICommandText interface");
    }
    
    // Set the command text
    BSTR bstrQuery = StringToBSTR(dax_query);
    hr = pICommandText->SetCommandText(DBGUID_DEFAULT, bstrQuery);
    ::SysFreeString(bstrQuery);
    
    if (FAILED(hr)) {
        ::SafeRelease(&pICommandText);
        ::SafeRelease(&pICommand);
        throw MSOLAPException(hr, "Failed to set command text");
    }
    
    // Set command timeout property if available
    ICommandProperties* pICommandProperties = NULL;
    hr = pICommand->QueryInterface(IID_ICommandProperties, (void**)&pICommandProperties);
    if (SUCCEEDED(hr)) {
        DBPROP prop;
        DBPROPSET propset;
        
        // Initialize the property
        prop.dwPropertyID = DBPROP_COMMANDTIMEOUT;
        prop.dwOptions = DBPROPOPTIONS_REQUIRED;
        prop.vValue.vt = VT_I4;
        prop.vValue.lVal = db.timeout_seconds;
        prop.colid = DB_NULLID;
        
        // Initialize the property set
        propset.guidPropertySet = DBPROPSET_ROWSET;
        propset.cProperties = 1;
        propset.rgProperties = &prop;
        
        // Set the property
        pICommandProperties->SetProperties(1, &propset);
        ::SafeRelease(&pICommandProperties);
    }
}

MSOLAPStatement::~MSOLAPStatement() {
    Close();
}

MSOLAPStatement::MSOLAPStatement(MSOLAPStatement&& other) noexcept
    : pICommand(other.pICommand),
      pICommandText(other.pICommandText),
      pIRowset(other.pIRowset),
      pIAccessor(other.pIAccessor),
      pColumnInfo(other.pColumnInfo),
      pStringsBuffer(other.pStringsBuffer),
      cColumns(other.cColumns),
      hAccessor(other.hAccessor),
      hRow(other.hRow),
      pRowData(other.pRowData),
      bindings(std::move(other.bindings)),
      has_row(other.has_row),
      executed(other.executed) {
      
    // Clear the moved-from object's state
    other.pICommand = nullptr;
    other.pICommandText = nullptr;
    other.pIRowset = nullptr;
    other.pIAccessor = nullptr;
    other.pColumnInfo = nullptr;
    other.pStringsBuffer = nullptr;
    other.cColumns = 0;
    other.hAccessor = NULL;
    other.hRow = NULL;
    other.pRowData = nullptr;
    other.has_row = false;
    other.executed = false;
}

MSOLAPStatement& MSOLAPStatement::operator=(MSOLAPStatement&& other) noexcept {
    if (this != &other) {
        // Clean up existing resources
        Close();
        
        // Move resources from other
        pICommand = other.pICommand;
        pICommandText = other.pICommandText;
        pIRowset = other.pIRowset;
        pIAccessor = other.pIAccessor;
        pColumnInfo = other.pColumnInfo;
        pStringsBuffer = other.pStringsBuffer;
        cColumns = other.cColumns;
        hAccessor = other.hAccessor;
        hRow = other.hRow;
        pRowData = other.pRowData;
        bindings = std::move(other.bindings);
        has_row = other.has_row;
        executed = other.executed;
        
        // Clear the moved-from object's state
        other.pICommand = nullptr;
        other.pICommandText = nullptr;
        other.pIRowset = nullptr;
        other.pIAccessor = nullptr;
        other.pColumnInfo = nullptr;
        other.pStringsBuffer = nullptr;
        other.cColumns = 0;
        other.hAccessor = NULL;
        other.hRow = NULL;
        other.pRowData = nullptr;
        other.has_row = false;
        other.executed = false;
    }
    return *this;
}
void MSOLAPStatement::SetupBindings() {
    MSOLAP_LOG("Begin SetupBindings(), columns: " + std::to_string(cColumns));
    if (cColumns == 0) {
        MSOLAP_LOG("No columns, returning");
        return;
    }
    
    // Create bindings for each column
    MSOLAP_LOG("Resizing bindings array to " + std::to_string(cColumns));
    bindings.resize(cColumns);
    
    DWORD dwOffset = 0;
    
    for (DBORDINAL i = 0; i < cColumns; i++) {
        MSOLAP_LOG("Setting up binding for column " + std::to_string(i));
        // Initialize binding fully
        bindings[i].iOrdinal = pColumnInfo[i].iOrdinal;  // Use actual column ordinal
        bindings[i].obValue = dwOffset + offsetof(COLUMNDATA, var);
        bindings[i].obLength = dwOffset + offsetof(COLUMNDATA, dwLength);
        bindings[i].obStatus = dwOffset + offsetof(COLUMNDATA, dwStatus);
        bindings[i].pTypeInfo = NULL;
        bindings[i].pObject = NULL;
        bindings[i].pBindExt = NULL;
        bindings[i].cbMaxLen = sizeof(VARIANT);
        bindings[i].dwFlags = 0;
        bindings[i].eParamIO = DBPARAMIO_NOTPARAM;
        bindings[i].dwPart = DBPART_VALUE | DBPART_LENGTH | DBPART_STATUS;
        bindings[i].dwMemOwner = DBMEMOWNER_CLIENTOWNED;
        bindings[i].wType = DBTYPE_VARIANT;
        bindings[i].bPrecision = 0;
        bindings[i].bScale = 0;
        
        // Move to next COLUMNDATA structure
        dwOffset += sizeof(COLUMNDATA);
    }
    
    // Create the accessor with correct buffer size
    MSOLAP_LOG("Creating accessor with buffer size: " + std::to_string(dwOffset));
    HRESULT hr = pIAccessor->CreateAccessor(
        DBACCESSOR_ROWDATA,
        cColumns,
        bindings.data(),
        dwOffset,
        &hAccessor,
        NULL);
    
    if (FAILED(hr)) {
        MSOLAP_LOG("Failed to create accessor: " + std::to_string(hr));
        throw MSOLAPException(hr, "Failed to create accessor");
    }
    MSOLAP_LOG("Accessor created successfully");
    
    // Allocate memory for row data
    MSOLAP_LOG("Allocating memory for row data, size: " + std::to_string(dwOffset));
    pRowData = new BYTE[dwOffset];
    memset(pRowData, 0, dwOffset);
    MSOLAP_LOG("SetupBindings complete");
}

bool MSOLAPStatement::Execute() {
    MSOLAP_LOG("Begin Execute()");
    if (executed) {
        MSOLAP_LOG("Already executed, returning");
        return true;
    }
    
    HRESULT hr;
    
    // Execute the command
    MSOLAP_LOG("About to execute command");
    hr = pICommand->Execute(NULL, IID_IRowset, NULL, NULL, (IUnknown**)&pIRowset);
    if (FAILED(hr)) {
        MSOLAP_LOG("Failed to execute command with HRESULT: " + std::to_string(hr));
        throw MSOLAPException(hr, "Failed to execute command");
    }
    MSOLAP_LOG("Command executed successfully");
    
    // Get column information
    IColumnsInfo* pIColumnsInfo = NULL;
    MSOLAP_LOG("Getting IColumnsInfo interface");
    hr = pIRowset->QueryInterface(IID_IColumnsInfo, (void**)&pIColumnsInfo);
    if (FAILED(hr)) {
        MSOLAP_LOG("Failed to get IColumnsInfo interface: " + std::to_string(hr));
        ::SafeRelease(&pIRowset);
        throw MSOLAPException(hr, "Failed to get IColumnsInfo interface");
    }
    
    MSOLAP_LOG("Getting column info");
    hr = pIColumnsInfo->GetColumnInfo(&cColumns, &pColumnInfo, &pStringsBuffer);
    ::SafeRelease(&pIColumnsInfo);
    
    if (FAILED(hr)) {
        MSOLAP_LOG("Failed to get column info: " + std::to_string(hr));
        ::SafeRelease(&pIRowset);
        throw MSOLAPException(hr, "Failed to get column info");
    }
    MSOLAP_LOG("Got column info, columns: " + std::to_string(cColumns));
    
    // Get the IAccessor interface
    MSOLAP_LOG("Getting IAccessor interface");
    hr = pIRowset->QueryInterface(IID_IAccessor, (void**)&pIAccessor);
    if (FAILED(hr)) {
        MSOLAP_LOG("Failed to get IAccessor interface: " + std::to_string(hr));
        ::SafeRelease(&pIRowset);
        throw MSOLAPException(hr, "Failed to get IAccessor interface");
    }
    
    // Setup bindings for the columns
    MSOLAP_LOG("Setting up bindings");
    SetupBindings();
    
    executed = true;
    MSOLAP_LOG("Execution complete");
    return true;
}

bool MSOLAPStatement::Step() {
    MSOLAP_LOG("Begin Step()");
    if (!executed) {
        MSOLAP_LOG("Not executed yet, calling Execute()");
        Execute();
    }
    
    if (has_row) {
        // Release the previous row
        MSOLAP_LOG("Releasing previous row");
        pIRowset->ReleaseRows(1, &hRow, NULL, NULL, NULL);
        has_row = false;
    }
    
    HRESULT hr;
    DBCOUNTITEM cRowsObtained = 0;
    
    // Get the next row - using double indirection with an array of HROWs
    MSOLAP_LOG("Getting next row");
    HROW* phRows = &hRow; // array of one HROW
    hr = pIRowset->GetNextRows(DB_NULL_HCHAPTER, 0, 1, &cRowsObtained, &phRows);
    
    if (hr == DB_S_ENDOFROWSET || cRowsObtained == 0) {
        // No more rows
        MSOLAP_LOG("No more rows");
        return false;
    }
    
    if (FAILED(hr)) {
        MSOLAP_LOG("Failed to get next row: " + std::to_string(hr));
        throw MSOLAPException(hr, "Failed to get next row");
    }
    MSOLAP_LOG("Got next row");
    
    // GetNextRows will always set hRow since we're using phRows = &hRow
    
    // Get the data for the row
    MSOLAP_LOG("Getting row data");
    hr = pIRowset->GetData(hRow, hAccessor, pRowData);
    if (FAILED(hr)) {
        MSOLAP_LOG("Failed to get row data: " + std::to_string(hr));
        pIRowset->ReleaseRows(1, &hRow, NULL, NULL, NULL);
        throw MSOLAPException(hr, "Failed to get row data");
    }
    MSOLAP_LOG("Got row data successfully");
    
    has_row = true;
    return true;
}

DBORDINAL MSOLAPStatement::GetColumnCount() const {
    return cColumns;
}

std::string MSOLAPStatement::GetColumnName(DBORDINAL column) const {
    if (column >= cColumns) {
        throw MSOLAPException("Column index out of range");
    }
    
    return BSTRToString(pColumnInfo[column].pwszName);
}

DBTYPE MSOLAPStatement::GetColumnType(DBORDINAL column) const {
    if (column >= cColumns) {
        throw MSOLAPException("Column index out of range");
    }
    
    return pColumnInfo[column].wType;
}

std::vector<LogicalType> MSOLAPStatement::GetColumnTypes() const {
    MSOLAP_LOG("Begin GetColumnTypes for " + std::to_string(cColumns) + " columns");
    std::vector<LogicalType> types;
    types.reserve(cColumns);
    
    try {
        for (DBORDINAL i = 0; i < cColumns; i++) {
            MSOLAP_LOG("Getting type for column " + std::to_string(i) + " with DBTYPE: " + std::to_string(pColumnInfo[i].wType));
            types.push_back(DBTypeToLogicalType(pColumnInfo[i].wType));
        }
    } catch (const std::exception& e) {
        MSOLAP_LOG("Exception in GetColumnTypes: " + std::string(e.what()));
        throw MSOLAPException("Error in GetColumnTypes: " + std::string(e.what()));
    } catch (...) {
        MSOLAP_LOG("Unknown exception in GetColumnTypes");
        throw MSOLAPException("Unknown error in GetColumnTypes");
    }
    
    MSOLAP_LOG("GetColumnTypes complete, returned " + std::to_string(types.size()) + " types");
    return types;
}

std::vector<std::string> MSOLAPStatement::GetColumnNames() const {
    MSOLAP_LOG("Begin GetColumnNames for " + std::to_string(cColumns) + " columns");
    std::vector<std::string> names;
    names.reserve(cColumns);
    
    try {
        for (DBORDINAL i = 0; i < cColumns; i++) {
            MSOLAP_LOG("Getting name for column " + std::to_string(i));
            std::string name;
            
            // Use safe pointer check
            if (pColumnInfo[i].pwszName != nullptr) {
                // Use try/catch for each conversion to isolate failures
                try {
                    name = BSTRToString(pColumnInfo[i].pwszName);
                    MSOLAP_LOG("Raw column " + std::to_string(i) + " name: " + name);
                } catch (...) {
                    MSOLAP_LOG("Error converting column name, using default");
                    name = "Column_" + std::to_string(i);
                }
            } else {
                MSOLAP_LOG("Column " + std::to_string(i) + " has null name, using default");
                name = "Column_" + std::to_string(i);
            }
            
            // Sanitize the name
            std::string sanitized_name = SanitizeColumnName(name);
            
            // Make sure names are unique
            bool is_unique = true;
            for (size_t j = 0; j < names.size(); j++) {
                if (names[j] == sanitized_name) {
                    is_unique = false;
                    sanitized_name += "_" + std::to_string(i);
                    break;
                }
            }
            
            MSOLAP_LOG("Final column " + std::to_string(i) + " name: " + sanitized_name);
            names.push_back(sanitized_name);
        }
    } catch (const std::exception& e) {
        MSOLAP_LOG("Exception in GetColumnNames: " + std::string(e.what()));
        
        // If we fail, return default column names
        names.clear();
        for (DBORDINAL i = 0; i < cColumns; i++) {
            names.push_back("Column_" + std::to_string(i));
        }
    } catch (...) {
        MSOLAP_LOG("Unknown exception in GetColumnNames");
        
        // If we fail, return default column names
        names.clear();
        for (DBORDINAL i = 0; i < cColumns; i++) {
            names.push_back("Column_" + std::to_string(i));
        }
    }
    
    MSOLAP_LOG("GetColumnNames complete, returned " + std::to_string(names.size()) + " names");
    return names;
}

Value MSOLAPStatement::GetValue(DBORDINAL column, const LogicalType& type) {
    MSOLAP_LOG("GetValue for column " + std::to_string(column) + ", type: " + type.ToString());
    if (!has_row) {
        MSOLAP_LOG("No current row");
        throw MSOLAPException("No current row");
    }
    
    if (column >= cColumns) {
        MSOLAP_LOG("Column index out of range: " + std::to_string(column) + " >= " + std::to_string(cColumns));
        throw MSOLAPException("Column index out of range");
    }
    
    // Calculate the offset to the COLUMNDATA structure for this column
    COLUMNDATA* pColData = (COLUMNDATA*)(pRowData + (column * sizeof(COLUMNDATA)));
    
    // Get status
    MSOLAP_LOG("Column status: " + std::to_string(pColData->dwStatus));
    if (pColData->dwStatus != DBSTATUS_S_OK) {
        // Handle NULL or error
        MSOLAP_LOG("Column status not OK, returning NULL value");
        return Value(type);
    }
    
    // Get variant value
    MSOLAP_LOG("Getting variant value");
    Value val = GetVariantValue(&(pColData->var), type);
    MSOLAP_LOG("Got variant value, returning");
    return val;
}

Value MSOLAPStatement::GetVariantValue(VARIANT* var, const LogicalType& type) {
    MSOLAP_LOG("GetVariantValue with variant type: " + std::to_string(var->vt));
    
    switch (type.id()) {
        case LogicalTypeId::SMALLINT:
        case LogicalTypeId::INTEGER:
        case LogicalTypeId::BIGINT:
            MSOLAP_LOG("Converting to BIGINT");
            return Value::BIGINT(ConvertVariantToInt64(var));
            
        case LogicalTypeId::FLOAT:
        case LogicalTypeId::DOUBLE:
            MSOLAP_LOG("Converting to DOUBLE");
            return Value::DOUBLE(ConvertVariantToDouble(var));
            
        case LogicalTypeId::VARCHAR:
        {
            MSOLAP_LOG("Converting to VARCHAR");
            // Create a temporary vector for string conversion
            Vector result_vec(LogicalType::VARCHAR);
            auto str_val = ConvertVariantToString(var, result_vec);
            MSOLAP_LOG("Converted to string: " + str_val.GetString());
            return Value(str_val);
        }
            
        case LogicalTypeId::BOOLEAN:
            MSOLAP_LOG("Converting to BOOLEAN");
            return Value::BOOLEAN(ConvertVariantToBool(var));
            
        case LogicalTypeId::TIMESTAMP:
            MSOLAP_LOG("Converting to TIMESTAMP");
            return Value::TIMESTAMP(ConvertVariantToTimestamp(var));
            
        case LogicalTypeId::DECIMAL:
            MSOLAP_LOG("Converting to DECIMAL (as DOUBLE)");
            return Value::DOUBLE(ConvertVariantToDouble(var));
            
        default:
        {
            // Default to string for unsupported types
            Vector result_vec(LogicalType::VARCHAR);
            return Value(ConvertVariantToString(var, result_vec));
        }
    }
}

void MSOLAPStatement::Close() {
    FreeResources();
    
    ::SafeRelease(&pIAccessor);
    ::SafeRelease(&pIRowset);
    ::SafeRelease(&pICommandText);
    ::SafeRelease(&pICommand);
    
    has_row = false;
    executed = false;
}

void MSOLAPStatement::FreeResources() {
    if (has_row && pIRowset) {
        pIRowset->ReleaseRows(1, &hRow, NULL, NULL, NULL);
        has_row = false;
    }
    
    if (hAccessor && pIAccessor) {
        pIAccessor->ReleaseAccessor(hAccessor, NULL);
        hAccessor = NULL;
    }
    
    if (pColumnInfo) {
        CoTaskMemFree(pColumnInfo);
        pColumnInfo = nullptr;
    }
    
    if (pStringsBuffer) {
        CoTaskMemFree(pStringsBuffer);
        pStringsBuffer = nullptr;
    }
    
    if (pRowData) {
        delete[] pRowData;
        pRowData = nullptr;
    }
    
    cColumns = 0;
    bindings.clear();
}

} // namespace duckdb