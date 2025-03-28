#include <windows.h>
#include <oledb.h>
#include <oledberr.h>
#include <iostream>
#include <string>
#include <comdef.h>

// Need to link with these libraries
#pragma comment(lib, "oledb.lib")
#pragma comment(lib, "ole32.lib")
#pragma comment(lib, "uuid.lib")

//MSOLAP.8
const CLSID CLSID_MSOLAP =
{ 0xDBC724B0, 0xDD86, 0x4772, { 0xBB, 0x5A, 0xFC, 0xC6, 0xCA, 0xB2, 0xFC, 0x1A } };

// Helper function to convert HRESULT to readable error message
std::wstring GetErrorMessage(HRESULT hr) {
    _com_error err(hr);
    return err.ErrorMessage();
}

// Helper function to convert DBTYPE to string for debugging
std::wstring DBTypeToString(DBTYPE type) {
    switch (type) {
    case DBTYPE_EMPTY:
        return L"EMPTY";
    case DBTYPE_NULL:
        return L"NULL";
    case DBTYPE_I2:
        return L"I2";
    case DBTYPE_I4:
        return L"I4";
    case DBTYPE_R4:
        return L"R4";
    case DBTYPE_R8:
        return L"R8";
    case DBTYPE_CY:
        return L"CY";
    case DBTYPE_DATE:
        return L"DATE";
    case DBTYPE_BSTR:
        return L"BSTR";
    case DBTYPE_ERROR:
        return L"ERROR";
    case DBTYPE_BOOL:
        return L"BOOL";
    case DBTYPE_VARIANT:
        return L"VARIANT";
    case DBTYPE_DECIMAL:
        return L"DECIMAL";
    case DBTYPE_I1:
        return L"I1";
    case DBTYPE_UI1:
        return L"UI1";
    case DBTYPE_UI2:
        return L"UI2";
    case DBTYPE_UI4:
        return L"UI4";
    case DBTYPE_I8:
        return L"I8";
    case DBTYPE_UI8:
        return L"UI8";
    case DBTYPE_GUID:
        return L"GUID";
    case DBTYPE_BYTES:
        return L"BYTES";
    case DBTYPE_STR:
        return L"STR";
    case DBTYPE_WSTR:
        return L"WSTR";
    case DBTYPE_NUMERIC:
        return L"NUMERIC";
    case DBTYPE_UDT:
        return L"UDT";
    case DBTYPE_DBDATE:
        return L"DBDATE";
    case DBTYPE_DBTIME:
        return L"DBTIME";
    case DBTYPE_DBTIMESTAMP:
        return L"DBTIMESTAMP";
    default:
        return L"UNKNOWN(" + std::to_wstring(type) + L")";
    }
}

// Helper function to safely release COM interfaces
template <class T>
inline void SafeRelease(T** ppT) {
    if (*ppT) {
        (*ppT)->Release();
        *ppT = NULL;
    }
}

int main()
{
    // Initialize COM
    HRESULT hr = CoInitialize(NULL);
    if (FAILED(hr)) {
        std::wcerr << L"COM initialization failed: " << GetErrorMessage(hr) << std::endl;
        return 1;
    }

    // Connection parameters
    std::wstring serverName, databaseName, connectionString;

    // Get user input for connection parameters
    std::wcout << L"Enter server name (default: localhost): ";
    std::getline(std::wcin, serverName);
    if (serverName.empty()) serverName = L"localhost:63521";

    std::wcout << L"Enter database/cube name (default: d3ecf514-bab5-4ad9-92e2-4e8dafdd3082): ";
    std::getline(std::wcin, databaseName);
    if (databaseName.empty()) databaseName = L"d3ecf514-bab5-4ad9-92e2-4e8dafdd3082";

    // Build connection string
    connectionString = L"Provider=MSOLAP;Data Source=" + serverName + L";Initial Catalog=" + databaseName + L";Format=Tabular;";

    // Create data source
    IDBInitialize* pIDBInitialize = NULL;
    IDBProperties* pIDBProperties = NULL;

    // Create an instance of the OLE DB Provider for Analysis Services
    hr = CoCreateInstance(CLSID_MSOLAP, NULL, CLSCTX_INPROC_SERVER,
        IID_IDBInitialize, (void**)&pIDBInitialize);

    if (FAILED(hr)) {
        std::wcerr << L"Failed to create MSOLAP provider: " << GetErrorMessage(hr) << std::endl;
        CoUninitialize();
        return 1;
    }

    // Get the IDBProperties interface
    hr = pIDBInitialize->QueryInterface(IID_IDBProperties, (void**)&pIDBProperties);
    if (FAILED(hr)) {
        std::wcerr << L"Failed to get IDBProperties: " << GetErrorMessage(hr) << std::endl;
        SafeRelease(&pIDBInitialize);
        CoUninitialize();
        return 1;
    }

    // Set the properties for the connection
    DBPROP dbProps[3]; // Increased to 3 to add Format=Tabular
    DBPROPSET dbPropSet;

    // Initialize the property structures
    ZeroMemory(dbProps, sizeof(dbProps));

    // Set the Data Source property
    dbProps[0].dwPropertyID = DBPROP_INIT_DATASOURCE;
    dbProps[0].dwOptions = DBPROPOPTIONS_REQUIRED;
    dbProps[0].vValue.vt = VT_BSTR;
    dbProps[0].vValue.bstrVal = SysAllocString(serverName.c_str());

    // Set the Catalog property (database name)
    dbProps[1].dwPropertyID = DBPROP_INIT_CATALOG;
    dbProps[1].dwOptions = DBPROPOPTIONS_REQUIRED;
    dbProps[1].vValue.vt = VT_BSTR;
    dbProps[1].vValue.bstrVal = SysAllocString(databaseName.c_str());

    // Set the Format property to Tabular
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
        std::wcerr << L"Failed to set connection properties: " << GetErrorMessage(hr) << std::endl;
        SafeRelease(&pIDBProperties);
        SafeRelease(&pIDBInitialize);
        CoUninitialize();
        return 1;
    }

    // Initialize the data source
    hr = pIDBInitialize->Initialize();
    SafeRelease(&pIDBProperties);

    if (FAILED(hr)) {
        std::wcerr << L"Failed to initialize data source: " << GetErrorMessage(hr) << std::endl;
        SafeRelease(&pIDBInitialize);
        CoUninitialize();
        return 1;
    }

    std::wcout << L"Connected to " << serverName << L", database: " << databaseName << std::endl;

    // Create a session
    IDBCreateSession* pIDBCreateSession = NULL;
    hr = pIDBInitialize->QueryInterface(IID_IDBCreateSession, (void**)&pIDBCreateSession);
    if (FAILED(hr)) {
        std::wcerr << L"Failed to get IDBCreateSession: " << GetErrorMessage(hr) << std::endl;
        SafeRelease(&pIDBInitialize);
        CoUninitialize();
        return 1;
    }

    IDBCreateCommand* pIDBCreateCommand = NULL;
    hr = pIDBCreateSession->CreateSession(NULL, IID_IDBCreateCommand, (IUnknown**)&pIDBCreateCommand);
    SafeRelease(&pIDBCreateSession);
    if (FAILED(hr)) {
        std::wcerr << L"Failed to create session: " << GetErrorMessage(hr) << std::endl;
        SafeRelease(&pIDBInitialize);
        CoUninitialize();
        return 1;
    }

    // DAX query execution loop
    bool continueExecution = true;
    while (continueExecution) {
        std::wstring daxQuery;
        std::wcout << L"\nEnter DAX query (or 'exit' to quit):\n";
        std::getline(std::wcin, daxQuery);

        if (daxQuery == L"exit" || daxQuery == L"quit") {
            continueExecution = false;
            continue;
        }

        if (daxQuery.empty()) {
            // Use a default query if none provided
            daxQuery = L"EVALUATE ROW(\"Example\", 123)";
            std::wcout << L"Using default query: " << daxQuery << std::endl;
        }

        // Create command object
        ICommand* pICommand = NULL;
        hr = pIDBCreateCommand->CreateCommand(NULL, IID_ICommand, (IUnknown**)&pICommand);
        if (FAILED(hr)) {
            std::wcerr << L"Failed to create command: " << GetErrorMessage(hr) << std::endl;
            continue;
        }

        // Set the command text
        ICommandText* pICommandText = NULL;
        hr = pICommand->QueryInterface(IID_ICommandText, (void**)&pICommandText);
        if (FAILED(hr)) {
            std::wcerr << L"Failed to get ICommandText: " << GetErrorMessage(hr) << std::endl;
            SafeRelease(&pICommand);
            continue;
        }

        // Use DBGUID_DEFAULT instead of DBGUID_DAX - this is the key change!
        hr = pICommandText->SetCommandText(DBGUID_DEFAULT, daxQuery.c_str());
        if (FAILED(hr)) {
            std::wcerr << L"Failed to set command text: " << GetErrorMessage(hr) << std::endl;
            SafeRelease(&pICommandText);
            SafeRelease(&pICommand);
            continue;
        }

        // Execute the command
        IRowset* pIRowset = NULL;
        hr = pICommand->Execute(NULL, IID_IRowset, NULL, NULL, (IUnknown**)&pIRowset);
        SafeRelease(&pICommandText);
        SafeRelease(&pICommand);

        if (FAILED(hr)) {
            std::wcerr << L"Query execution failed: " << GetErrorMessage(hr) << std::endl;
            continue;
        }

        std::wcout << L"Query executed successfully." << std::endl;

        // Get column information
        IColumnsInfo* pIColumnsInfo = NULL;
        hr = pIRowset->QueryInterface(IID_IColumnsInfo, (void**)&pIColumnsInfo);
        if (FAILED(hr)) {
            std::wcerr << L"Failed to get IColumnsInfo: " << GetErrorMessage(hr) << std::endl;
            SafeRelease(&pIRowset);
            continue;
        }

        DBORDINAL cColumns;
        DBCOLUMNINFO* pColumnInfo;
        OLECHAR* pStringBuffer;

        hr = pIColumnsInfo->GetColumnInfo(&cColumns, &pColumnInfo, &pStringBuffer);
        SafeRelease(&pIColumnsInfo);

        if (FAILED(hr)) {
            std::wcerr << L"Failed to get column info: " << GetErrorMessage(hr) << std::endl;
            SafeRelease(&pIRowset);
            continue;
        }

        std::wcout << L"Number of columns: " << cColumns << std::endl;

        // Display column headers and detailed info
        std::wcout << L"\nColumn Information:" << std::endl;
        for (DBORDINAL i = 0; i < cColumns; i++) {
            std::wcout << L"Column " << i << L": ";
            if (pColumnInfo[i].pwszName) {
                std::wcout << pColumnInfo[i].pwszName;
            }
            else {
                std::wcout << L"(No Name)";
            }
            std::wcout << L", Type = " << pColumnInfo[i].wType << std::endl;
        }

        std::wcout << L"\nResults:\n";
        for (DBORDINAL i = 0; i < cColumns; i++) {
            if (pColumnInfo[i].pwszName) {
                std::wcout << pColumnInfo[i].pwszName << L"\t";
            }
            else {
                std::wcout << L"Column" << i << L"\t";
            }
        }
        std::wcout << std::endl;

        // Create accessors to retrieve data
        IAccessor* pIAccessor = NULL;
        hr = pIRowset->QueryInterface(IID_IAccessor, (void**)&pIAccessor);
        if (FAILED(hr)) {
            std::wcerr << L"Failed to get IAccessor: " << GetErrorMessage(hr) << std::endl;
            CoTaskMemFree(pColumnInfo);
            CoTaskMemFree(pStringBuffer);
            SafeRelease(&pIRowset);
            continue;
        }

        // Create bindings for all columns
        DBBINDING* rgBindings = new DBBINDING[cColumns];
        ULONG cbRowSize = 0;

        // Set up the binding structure for all columns
        DBORDINAL bindColumnCount = 0;
        for (DBORDINAL i = 0; i < cColumns; i++) {
            // Skip the problematic Column 4 (Format String)
            if (i == 4 && cColumns > 4) {
                std::wcout << L"Skipping binding for column 4 (Format String) due to previous errors." << std::endl;
                continue;
            }

            rgBindings[bindColumnCount].iOrdinal = i + 1;  // 1-based
            rgBindings[bindColumnCount].obValue = cbRowSize;

            // Determine max buffer size based on column type
            DBLENGTH cbMaxLen;
            DBTYPE wType;

            // Match the column's native type instead of using VARIANT
            switch (pColumnInfo[i].wType) {
            case DBTYPE_WSTR:
            case DBTYPE_STR:
            //case 130: // Treat type 130 as string (BSTR-like)
                // Special case for Currency column (index 3) - use even larger buffer
                if (i == 3 && cColumns > 4) {
                    cbMaxLen = 8192; // Extra large buffer for Currency column
                    std::wcout << "Using extra large buffer for Currency column" << std::endl;
                }
                else {
                    cbMaxLen = 4096; // Large buffer for other strings
                }
                wType = DBTYPE_WSTR; // Always bind as wide strings
                break;
            case DBTYPE_I8:
                cbMaxLen = sizeof(LONGLONG);
                wType = DBTYPE_I8;
                break;
            case DBTYPE_I4:
                cbMaxLen = sizeof(LONG);
                wType = DBTYPE_I4;
                break;
            case DBTYPE_R8:
                cbMaxLen = sizeof(double);
                wType = DBTYPE_R8;
                break;
            default:
                // For unknown types, use large string buffer
                cbMaxLen = 4096;
                wType = DBTYPE_WSTR;
                break;
            }

            rgBindings[bindColumnCount].obLength = cbRowSize + cbMaxLen;
            rgBindings[bindColumnCount].obStatus = cbRowSize + cbMaxLen + sizeof(ULONG);
            rgBindings[bindColumnCount].pTypeInfo = NULL;
            rgBindings[bindColumnCount].pObject = NULL;
            rgBindings[bindColumnCount].pBindExt = NULL;
            rgBindings[bindColumnCount].dwPart = DBPART_VALUE | DBPART_LENGTH | DBPART_STATUS;
            rgBindings[bindColumnCount].dwMemOwner = DBMEMOWNER_CLIENTOWNED;
            rgBindings[bindColumnCount].dwFlags = 0;
            rgBindings[bindColumnCount].eParamIO = DBPARAMIO_NOTPARAM;
            rgBindings[bindColumnCount].cbMaxLen = cbMaxLen;
            rgBindings[bindColumnCount].wType = wType;
            rgBindings[bindColumnCount].bPrecision = 0;
            rgBindings[bindColumnCount].bScale = 0;

            // Update row size for next column
            cbRowSize += cbMaxLen + sizeof(ULONG) + sizeof(DBSTATUS);

            // Increment our binding counter
            bindColumnCount++;
        }

        // Update the actual number of columns we're binding
        std::wcout << "Binding " << bindColumnCount << " out of " << cColumns << " columns." << std::endl;

        std::wcout << "Binding setup complete. Row size: " << cbRowSize << " bytes." << std::endl;

        // Create the accessor
        HACCESSOR hAccessor;
        DBBINDSTATUS* rgBindStatus = new DBBINDSTATUS[bindColumnCount];

        hr = pIAccessor->CreateAccessor(DBACCESSOR_ROWDATA, bindColumnCount, rgBindings,
            cbRowSize, &hAccessor, rgBindStatus);

        if (FAILED(hr)) {
            std::wcerr << L"Failed to create accessor: " << GetErrorMessage(hr) << std::endl;

            // Output binding status for debugging
            std::wcerr << L"Binding status details:" << std::endl;
            DBORDINAL bindIndex = 0;
            for (DBORDINAL i = 0; i < cColumns; i++) {
                // Skip the column we're not binding
                if (i == 4 && cColumns > 4) {
                    std::wcerr << L"Column " << i << L" ("
                        << (pColumnInfo[i].pwszName ? pColumnInfo[i].pwszName : L"(No Name)")
                        << L"): Not bound" << std::endl;
                    continue;
                }

                std::wcerr << L"Column " << i << L" ("
                    << (pColumnInfo[i].pwszName ? pColumnInfo[i].pwszName : L"(No Name)")
                    << L"): Status = " << rgBindStatus[bindIndex];

                // Interpret status code
                switch (rgBindStatus[bindIndex]) {
                case DBBINDSTATUS_OK:
                    std::wcerr << L" (OK)";
                    break;
                case DBBINDSTATUS_BADORDINAL:
                    std::wcerr << L" (Bad ordinal)";
                    break;
                case DBBINDSTATUS_UNSUPPORTEDCONVERSION:
                    std::wcerr << L" (Unsupported conversion)";
                    break;
                case DBBINDSTATUS_BADBINDINFO:
                    std::wcerr << L" (Bad bind info)";
                    break;
                case DBBINDSTATUS_BADSTORAGEFLAGS:
                    std::wcerr << L" (Bad storage flags)";
                    break;
                case DBBINDSTATUS_NOINTERFACE:
                    std::wcerr << L" (No interface)";
                    break;
                default:
                    std::wcerr << L" (Unknown error)";
                }
                std::wcerr << std::endl;
                bindIndex++;
            }

            delete[] rgBindStatus;
            delete[] rgBindings;
            SafeRelease(&pIAccessor);
            CoTaskMemFree(pColumnInfo);
            CoTaskMemFree(pStringBuffer);
            SafeRelease(&pIRowset);
            continue;
        }

        delete[] rgBindStatus;

        // Allocate buffer for row data
        BYTE* pData = new BYTE[cbRowSize];

        // Fetch and display rows
        HROW hRow;
        HROW* pRows = &hRow;
        DBCOUNTITEM cRowsObtained = 0;
        int rowCount = 0;

        while (TRUE) {
            hr = pIRowset->GetNextRows(0, 0, 1, &cRowsObtained, &pRows);
            if (FAILED(hr) || cRowsObtained == 0)
                break;

            memset(pData, 0, cbRowSize);  // Clear the buffer

            // Get the row data
            hr = pIRowset->GetData(hRow, hAccessor, pData);
            if (SUCCEEDED(hr)) {
                rowCount++;

                // Process each column
                DBORDINAL bindIndex = 0;
                for (DBORDINAL i = 0; i < cColumns; i++) {
                    // If this is the column we skipped, output a placeholder
                    if (i == 4 && cColumns > 4) {
                        std::wcout << L"[Not Bound]" << L"\t";
                        continue;
                    }

                    DBSTATUS status = *(DBSTATUS*)(pData + rgBindings[bindIndex].obStatus);

                    if (status == DBSTATUS_S_OK) {
                        // Display value based on its type
                        switch (rgBindings[bindIndex].wType) {
                        case DBTYPE_WSTR:
                            std::wcout << (WCHAR*)(pData + rgBindings[bindIndex].obValue);
                            break;
                        case DBTYPE_I4:
                            std::wcout << *(LONG*)(pData + rgBindings[bindIndex].obValue);
                            break;
                        case DBTYPE_I8:
                            std::wcout << *(LONGLONG*)(pData + rgBindings[bindIndex].obValue);
                            break;
                        case DBTYPE_R8:
                            std::wcout << *(double*)(pData + rgBindings[bindIndex].obValue);
                            break;
                        default:
                            std::wcout << L"[Unknown type]";
                            break;
                        }
                    }
                    else if (status == DBSTATUS_S_ISNULL) {
                        std::wcout << L"NULL";
                    }
                    else if (status == DBSTATUS_S_TRUNCATED) {
                        // For truncated data, display what we have with a warning
                        ULONG actualLength = *(ULONG*)(pData + rgBindings[bindIndex].obLength);
                        std::wcout << L"[Truncated " << actualLength << L"bytes] ";

                        if (rgBindings[bindIndex].wType == DBTYPE_WSTR) {
                            // For string data, try to display what we have
                            std::wcout << (WCHAR*)(pData + rgBindings[bindIndex].obValue);

                            // If this is column 3 (Currency), add extra debugging
                            if (i == 3) {
                                std::wcout << L" [Debug: ";
                                WCHAR* str = (WCHAR*)(pData + rgBindings[bindIndex].obValue);
                                for (int j = 0; j < 10 && str[j]; j++) {
                                    std::wcout << L"\\u" << std::hex << (int)str[j] << std::dec;
                                }
                                std::wcout << L"]";
                            }
                        }
                        else {
                            std::wcout << L"[Data]";
                        }
                    }
                    else {
                        std::wcout << L"[Status: " << status << L"]";
                    }

                    std::wcout << L"\t";
                    bindIndex++;
                }
                std::wcout << std::endl;
            }
            else {
                std::wcerr << L"Failed to get row data: " << GetErrorMessage(hr) << std::endl;
            }

            // Release the row handle
            pIRowset->ReleaseRows(1, pRows, NULL, NULL, NULL);
        }

        std::wcout << L"\n" << rowCount << L" row(s) returned." << std::endl;

        // Clean up
        delete[] pData;
        delete[] rgBindings;
        pIAccessor->ReleaseAccessor(hAccessor, NULL);
        SafeRelease(&pIAccessor);
        CoTaskMemFree(pColumnInfo);
        CoTaskMemFree(pStringBuffer);
        SafeRelease(&pIRowset);
    }

    // Clean up
    SafeRelease(&pIDBCreateCommand);

    // Uninitialize the data source
    if (pIDBInitialize) {
        pIDBInitialize->Uninitialize();
        SafeRelease(&pIDBInitialize);
    }

    CoUninitialize();
    return 0;
}