#include "msolap_connection.hpp"
#include "msolap_utils.hpp"
#include <stdexcept>

namespace duckdb {

// Initialize static member
bool MSOLAPConnection::com_initialized = false;

MSOLAPConnection::MSOLAPConnection() 
    : pIDBInitialize(nullptr), pIDBCreateCommand(nullptr) {
}

MSOLAPConnection::~MSOLAPConnection() {
    Close();
}

MSOLAPConnection::MSOLAPConnection(MSOLAPConnection &&other) noexcept
    : pIDBInitialize(nullptr), pIDBCreateCommand(nullptr) {
    std::swap(pIDBInitialize, other.pIDBInitialize);
    std::swap(pIDBCreateCommand, other.pIDBCreateCommand);
    std::swap(server_name, other.server_name);
    std::swap(database_name, other.database_name);
}

MSOLAPConnection &MSOLAPConnection::operator=(MSOLAPConnection &&other) noexcept {
    std::swap(pIDBInitialize, other.pIDBInitialize);
    std::swap(pIDBCreateCommand, other.pIDBCreateCommand);
    std::swap(server_name, other.server_name);
    std::swap(database_name, other.database_name);
    return *this;
}

void MSOLAPConnection::InitializeCOM() {
    if (!com_initialized) {
        HRESULT hr = CoInitialize(NULL);
        if (FAILED(hr)) {
            throw std::runtime_error("COM initialization failed: " + MSOLAPUtils::GetErrorMessage(hr));
        }
        com_initialized = true;
    }
}

void MSOLAPConnection::ParseConnectionString(const std::string &connection_string) {
    // Simple parsing of connection string
    // Format expected: "Server=server_name;Database=database_name"
    
    std::map<std::string, std::string> properties;
    
    size_t pos = 0;
    std::string token;
    std::string str = connection_string;
    
    while ((pos = str.find(';')) != std::string::npos) {
        token = str.substr(0, pos);
        
        // Find key-value separator
        size_t sep_pos = token.find('=');
        if (sep_pos != std::string::npos) {
            std::string key = token.substr(0, sep_pos);
            std::string value = token.substr(sep_pos + 1);
            properties[key] = value;
        }
        
        str.erase(0, pos + 1);
    }
    
    // Handle the last token after the last semicolon
    if (!str.empty()) {
        size_t sep_pos = str.find('=');
        if (sep_pos != std::string::npos) {
            std::string key = str.substr(0, sep_pos);
            std::string value = str.substr(sep_pos + 1);
            properties[key] = value;
        }
    }
    
    // Extract server and database
    auto server_it = properties.find("Server");
    if (server_it != properties.end()) {
        server_name = WindowsUtil::UTF8ToUnicode(server_it->second.c_str());
    } else {
        server_name = L"localhost";
    }
    
    auto db_it = properties.find("Database");
    if (db_it != properties.end()) {
        database_name = WindowsUtil::UTF8ToUnicode(db_it->second.c_str());
    } else {
        database_name = L"";
    }
}

MSOLAPConnection MSOLAPConnection::Connect(const std::string &connection_string) {
    MSOLAPConnection connection;
    
    // Initialize COM
    InitializeCOM();
    
    // Parse connection string
    connection.ParseConnectionString(connection_string);
    
    // Create data source
    HRESULT hr = CoCreateInstance(CLSID_MSOLAP, NULL, CLSCTX_INPROC_SERVER,
        IID_IDBInitialize, (void**)&connection.pIDBInitialize);
        
    if (FAILED(hr)) {
        throw std::runtime_error("Failed to create MSOLAP provider: " + MSOLAPUtils::GetErrorMessage(hr));
    }
    
    // Get the IDBProperties interface
    IDBProperties* pIDBProperties = NULL;
    hr = connection.pIDBInitialize->QueryInterface(IID_IDBProperties, (void**)&pIDBProperties);
    if (FAILED(hr)) {
        MSOLAPUtils::SafeRelease(&connection.pIDBInitialize);
        throw std::runtime_error("Failed to get IDBProperties: " + MSOLAPUtils::GetErrorMessage(hr));
    }
    
    // Set the properties for the connection
    DBPROP dbProps[3];
    DBPROPSET dbPropSet;

    // Initialize the property structures
    ZeroMemory(dbProps, sizeof(dbProps));

    // Set the Data Source property
    dbProps[0].dwPropertyID = DBPROP_INIT_DATASOURCE;
    dbProps[0].dwOptions = DBPROPOPTIONS_REQUIRED;
    dbProps[0].vValue.vt = VT_BSTR;
    dbProps[0].vValue.bstrVal = SysAllocString(connection.server_name.c_str());

    // Set the Catalog property (database name)
    dbProps[1].dwPropertyID = DBPROP_INIT_CATALOG;
    dbProps[1].dwOptions = DBPROPOPTIONS_REQUIRED;
    dbProps[1].vValue.vt = VT_BSTR;
    dbProps[1].vValue.bstrVal = SysAllocString(connection.database_name.c_str());

    // Set the Mode property to read-only
    dbProps[2].dwPropertyID = DBPROP_INIT_MODE;
    dbProps[2].dwOptions = DBPROPOPTIONS_REQUIRED;
    dbProps[2].vValue.vt = VT_I4;
    dbProps[2].vValue.lVal = DB_MODE_READ; // Read-only mode

    // Set up the property set
    dbPropSet.guidPropertySet = DBPROPSET_DBINIT;
    dbPropSet.cProperties = 3;
    dbPropSet.rgProperties = dbProps;

    // Set the initialization properties
    hr = pIDBProperties->SetProperties(1, &dbPropSet);

    // Free the BSTR allocations
    SysFreeString(dbProps[0].vValue.bstrVal);
    SysFreeString(dbProps[1].vValue.bstrVal);
    
    if (FAILED(hr)) {
        MSOLAPUtils::SafeRelease(&pIDBProperties);
        MSOLAPUtils::SafeRelease(&connection.pIDBInitialize);
        throw std::runtime_error("Failed to set connection properties: " + MSOLAPUtils::GetErrorMessage(hr));
    }

    // Initialize the data source
    hr = connection.pIDBInitialize->Initialize();
    MSOLAPUtils::SafeRelease(&pIDBProperties);

    if (FAILED(hr)) {
        MSOLAPUtils::SafeRelease(&connection.pIDBInitialize);
        throw std::runtime_error("Failed to initialize data source: " + MSOLAPUtils::GetErrorMessage(hr));
    }

    // Create a session
    IDBCreateSession* pIDBCreateSession = NULL;
    hr = connection.pIDBInitialize->QueryInterface(IID_IDBCreateSession, (void**)&pIDBCreateSession);
    if (FAILED(hr)) {
        MSOLAPUtils::SafeRelease(&connection.pIDBInitialize);
        throw std::runtime_error("Failed to get IDBCreateSession: " + MSOLAPUtils::GetErrorMessage(hr));
    }

    hr = pIDBCreateSession->CreateSession(NULL, IID_IDBCreateCommand, (IUnknown**)&connection.pIDBCreateCommand);
    MSOLAPUtils::SafeRelease(&pIDBCreateSession);
    if (FAILED(hr)) {
        MSOLAPUtils::SafeRelease(&connection.pIDBInitialize);
        throw std::runtime_error("Failed to create session: " + MSOLAPUtils::GetErrorMessage(hr));
    }
    
    return connection;
}

IRowset* MSOLAPConnection::ExecuteQuery(const std::string &dax_query) {
    if (!IsOpen()) {
        throw std::runtime_error("Connection is not open");
    }
    
    // Convert query to wide string
    std::wstring wquery = WindowsUtil::UTF8ToUnicode(dax_query.c_str());
    
    // Create command object
    ICommand* pICommand = NULL;
    HRESULT hr = pIDBCreateCommand->CreateCommand(NULL, IID_ICommand, (IUnknown**)&pICommand);
    if (FAILED(hr)) {
        throw std::runtime_error("Failed to create command: " + MSOLAPUtils::GetErrorMessage(hr));
    }

    // Set the command text
    ICommandText* pICommandText = NULL;
    hr = pICommand->QueryInterface(IID_ICommandText, (void**)&pICommandText);
    if (FAILED(hr)) {
        MSOLAPUtils::SafeRelease(&pICommand);
        throw std::runtime_error("Failed to get ICommandText: " + MSOLAPUtils::GetErrorMessage(hr));
    }

    hr = pICommandText->SetCommandText(DBGUID_DEFAULT, wquery.c_str());
    if (FAILED(hr)) {
        MSOLAPUtils::SafeRelease(&pICommandText);
        MSOLAPUtils::SafeRelease(&pICommand);
        throw std::runtime_error("Failed to set command text: " + MSOLAPUtils::GetErrorMessage(hr));
    }

    // Execute the command
    IRowset* pIRowset = NULL;
    hr = pICommand->Execute(NULL, IID_IRowset, NULL, NULL, (IUnknown**)&pIRowset);
    MSOLAPUtils::SafeRelease(&pICommandText);
    MSOLAPUtils::SafeRelease(&pICommand);

    if (FAILED(hr)) {
        throw std::runtime_error("Query execution failed: " + MSOLAPUtils::GetErrorMessage(hr));
    }
    
    return pIRowset;
}

bool MSOLAPConnection::GetColumnInfo(IRowset *rowset, std::vector<std::string> &names, std::vector<LogicalType> &types) {
    if (!rowset) {
        return false;
    }
    
    // Get column information using IColumnsInfo
    IColumnsInfo* pIColumnsInfo = NULL;
    HRESULT hr = rowset->QueryInterface(IID_IColumnsInfo, (void**)&pIColumnsInfo);
    if (FAILED(hr)) {
        return false;
    }

    // Get column information
    DBORDINAL cColumns;
    WCHAR* pStringsBuffer = NULL;
    DBCOLUMNINFO* pColumnInfo = NULL;

    hr = pIColumnsInfo->GetColumnInfo(&cColumns, &pColumnInfo, &pStringsBuffer);
    if (FAILED(hr)) {
        MSOLAPUtils::SafeRelease(&pIColumnsInfo);
        return false;
    }

    // Process column information
    names.clear();
    types.clear();
    
    for (DBORDINAL i = 0; i < cColumns; i++) {
        std::string column_name;
        if (pColumnInfo[i].pwszName) {
            // Sanitize column name (replace [] with _)
            column_name = MSOLAPUtils::SanitizeColumnName(pColumnInfo[i].pwszName);
        } else {
            column_name = "Column" + std::to_string(i);
        }
        
        names.push_back(column_name);
        types.push_back(MSOLAPUtils::GetLogicalTypeFromDBTYPE(pColumnInfo[i].wType));
    }
    
    // Clean up
    CoTaskMemFree(pColumnInfo);
    CoTaskMemFree(pStringsBuffer);
    MSOLAPUtils::SafeRelease(&pIColumnsInfo);
    
    return true;
}

bool MSOLAPConnection::IsOpen() const {
    return pIDBInitialize != nullptr && pIDBCreateCommand != nullptr;
}

void MSOLAPConnection::Close() {
    if (pIDBCreateCommand) {
        MSOLAPUtils::SafeRelease(&pIDBCreateCommand);
    }
    
    if (pIDBInitialize) {
        pIDBInitialize->Uninitialize();
        MSOLAPUtils::SafeRelease(&pIDBInitialize);
    }
}

} // namespace duckdb