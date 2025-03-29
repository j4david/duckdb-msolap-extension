#include "msolap_stmt.hpp"
#include "msolap_db.hpp"
#include <comdef.h>

namespace duckdb {

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
        SafeRelease(&pICommand);
        throw MSOLAPException(hr, "Failed to get ICommandText interface");
    }
    
    // Set the command text
    BSTR bstrQuery = StringToBSTR(dax_query);
    hr = pICommandText->SetCommandText(DBGUID_DEFAULT, bstrQuery);
    SysFreeString(bstrQuery);
    
    if (FAILED(hr)) {
        SafeRelease(&pICommandText);
        SafeRelease(&pICommand);
        throw MSOLAPException(hr, "Failed to set command text");
    }
}

MSOLAPStatement::~MSOLAPStatement() {
    Close();
}

void MSOLAPStatement::SetupBindings() {
    if (cColumns == 0) {
        return;
    }
    
    // Create bindings for each column
    bindings.resize(cColumns);
    
    DBBINDING* pBinding = bindings.data();
    DBLENGTH dwOffset = 0;
    
    for (DBORDINAL i = 0; i < cColumns; i++) {
        // Initialize binding
        pBinding[i].iOrdinal = i + 1; // 1-based column ordinals
        pBinding[i].dwPart = DBPART_VALUE | DBPART_LENGTH | DBPART_STATUS;
        pBinding[i].dwMemOwner = DBMEMOWNER_CLIENTOWNED;
        pBinding[i].eParamIO = DBPARAMIO_NOTPARAM;
        pBinding[i].cbMaxLen = sizeof(VARIANT);
        pBinding[i].dwFlags = 0;
        pBinding[i].wType = DBTYPE_VARIANT;
        pBinding[i].pTypeInfo = NULL;
        pBinding[i].pObject = NULL;
        pBinding[i].pBindExt = NULL;
        
        // Set offset for status
        pBinding[i].obStatus = dwOffset;
        dwOffset += sizeof(DBSTATUS);
        
        // Set offset for length
        pBinding[i].obLength = dwOffset;
        dwOffset += sizeof(DBLENGTH);
        
        // Set offset for value
        pBinding[i].obValue = dwOffset;
        dwOffset += sizeof(VARIANT);
    }
    
    // Create the accessor
    HRESULT hr = pIAccessor->CreateAccessor(
        DBACCESSOR_ROWDATA,
        cColumns,
        bindings.data(),
        0,
        &hAccessor,
        NULL);
    
    if (FAILED(hr)) {
        throw MSOLAPException(hr, "Failed to create accessor");
    }
    
    // Allocate memory for row data
    pRowData = new BYTE[dwOffset];
    memset(pRowData, 0, dwOffset);
}

bool MSOLAPStatement::Execute() {
    if (executed) {
        // Already executed
        return true;
    }
    
    HRESULT hr;
    
    // Set properties for the command
    DBPROP rgProps[1];
    DBPROPSET rgPropSets[1];
    
    // Initialize the property structure
    rgProps[0].dwPropertyID = DBPROP_COMMANDTIMEOUT;
    rgProps[0].dwOptions = DBPROPOPTIONS_REQUIRED;
    rgProps[0].vValue.vt = VT_I4;
    rgProps[0].vValue.lVal = 60; // 60 seconds timeout
    rgProps[0].colid = DB_NULLID;
    
    // Initialize the property set structure
    rgPropSets[0].guidPropertySet = DBPROPSET_ROWSET;
    rgPropSets[0].cProperties = 1;
    rgPropSets[0].rgProperties = rgProps;
    
    // Set the properties
    ICommandProperties* pICommandProperties = NULL;
    hr = pICommand->QueryInterface(IID_ICommandProperties, (void**)&pICommandProperties);
    if (SUCCEEDED(hr)) {
        hr = pICommandProperties->SetProperties(1, rgPropSets);
        SafeRelease(&pICommandProperties);
    }
    
    // Execute the command
    hr = pICommand->Execute(NULL, IID_IRowset, NULL, NULL, (IUnknown**)&pIRowset);
    if (FAILED(hr)) {
        throw MSOLAPException(hr, "Failed to execute command");
    }
    
    // Get column information
    IColumnsInfo* pIColumnsInfo = NULL;
    hr = pIRowset->QueryInterface(IID_IColumnsInfo, (void**)&pIColumnsInfo);
    if (FAILED(hr)) {
        SafeRelease(&pIRowset);
        throw MSOLAPException(hr, "Failed to get IColumnsInfo interface");
    }
    
    hr = pIColumnsInfo->GetColumnInfo(&cColumns, &pColumnInfo, &pStringsBuffer);
    SafeRelease(&pIColumnsInfo);
    
    if (FAILED(hr)) {
        SafeRelease(&pIRowset);
        throw MSOLAPException(hr, "Failed to get column info");
    }
    
    // Get the IAccessor interface
    hr = pIRowset->QueryInterface(IID_IAccessor, (void**)&pIAccessor);
    if (FAILED(hr)) {
        SafeRelease(&pIRowset);
        throw MSOLAPException(hr, "Failed to get IAccessor interface");
    }
    
    // Setup bindings for the columns
    SetupBindings();
    
    executed = true;
    return true;
}

bool MSOLAPStatement::Step() {
    if (!executed) {
        throw MSOLAPException("Statement not executed");
    }
    
    if (has_row) {
        // Release the previous row
        pIRowset->ReleaseRows(1, &hRow, NULL, NULL, NULL);
        has_row = false;
    }
    
    HRESULT hr;
    DBCOUNTITEM cRowsObtained = 0;
    
    // Get the next row
    hr = pIRowset->GetNextRows(DB_NULL_HCHAPTER, 0, 1, &cRowsObtained, &hRow);
    
    if (hr == DB_S_ENDOFROWSET || cRowsObtained == 0) {
        // No more rows
        return false;
    }
    
    if (FAILED(hr)) {
        throw MSOLAPException(hr, "Failed to get next row");
    }
    
    // Get the data for the row
    hr = pIRowset->GetData(hRow, hAccessor, pRowData);
    if (FAILED(hr)) {
        pIRowset->ReleaseRows(1, &hRow, NULL, NULL, NULL);
        throw MSOLAPException(hr, "Failed to get row data");
    }
    
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
    std::vector<LogicalType> types;
    types.reserve(cColumns);
    
    for (DBORDINAL i = 0; i < cColumns; i++) {
        types.push_back(DBTypeToLogicalType(pColumnInfo[i].wType));
    }
    
    return types;
}

std::vector<std::string> MSOLAPStatement::GetColumnNames() const {
    std::vector<std::string> names;
    names.reserve(cColumns);
    
    for (DBORDINAL i = 0; i < cColumns; i++) {
        names.push_back(BSTRToString(pColumnInfo[i].pwszName));
    }
    
    return names;
}

Value MSOLAPStatement::GetValue(DBORDINAL column, const LogicalType& type) {
    if (!has_row) {
        throw MSOLAPException("No current row");
    }
    
    if (column >= cColumns) {
        throw MSOLAPException("Column index out of range");
    }
    
    // Get the status from the row data
    DBSTATUS status = *(DBSTATUS*)(pRowData + bindings[column].obStatus);
    
    if (status != DBSTATUS_S_OK) {
        // Handle NULL or error
        return Value(type);
    }
    
    // Get the variant value from the row data
    VARIANT* pVariant = (VARIANT*)(pRowData + bindings[column].obValue);
    
    return GetVariantValue(pVariant, type);
}

Value MSOLAPStatement::GetVariantValue(VARIANT* var, const LogicalType& type) {
    switch (type.id()) {
        case LogicalTypeId::SMALLINT:
        case LogicalTypeId::INTEGER:
        case LogicalTypeId::BIGINT:
            return Value::BIGINT(ConvertVariantToInt64(var));
            
        case LogicalTypeId::FLOAT:
        case LogicalTypeId::DOUBLE:
            return Value::DOUBLE(ConvertVariantToDouble(var));
            
        case LogicalTypeId::VARCHAR:
        {
            // Create a temporary vector for string conversion
            Vector result_vec(LogicalType::VARCHAR);
            return Value(ConvertVariantToString(var, result_vec));
        }
            
        case LogicalTypeId::BOOLEAN:
            return Value::BOOLEAN(ConvertVariantToBool(var));
            
        case LogicalTypeId::TIMESTAMP:
            return Value::TIMESTAMP(ConvertVariantToTimestamp(var));
            
        case LogicalTypeId::DECIMAL:
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
    
    SafeRelease(&pIAccessor);
    SafeRelease(&pIRowset);
    SafeRelease(&pICommandText);
    SafeRelease(&pICommand);
    
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