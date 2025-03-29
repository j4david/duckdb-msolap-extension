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

// Structure for column data when using variants
struct COLUMNDATA {
    DBSTATUS dwStatus;
    DBLENGTH dwLength;
    VARIANT  var;
};

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
    if (serverName.empty()) serverName = L"localhost:53940";

    std::wcout << L"Enter database/cube name (default: eee3df03-7e86-43e6-b29e-b667e6e97ea7): ";
    std::getline(std::wcin, databaseName);
    if (databaseName.empty()) databaseName = L"eee3df03-7e86-43e6-b29e-b667e6e97ea7";

    // Build connection string
    connectionString = L"Provider=MSOLAP.8;Data Source=" + serverName + L";Initial Catalog=" + databaseName;

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
    DBPROP dbProps[3];
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

        // Use DBGUID_DEFAULT instead of DBGUID_DAX
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

        // Get column information using IColumnsInfo
        IColumnsInfo* pIColumnsInfo = NULL;
        hr = pIRowset->QueryInterface(IID_IColumnsInfo, (void**)&pIColumnsInfo);
        if (FAILED(hr)) {
            std::wcerr << L"Failed to get IColumnsInfo: " << GetErrorMessage(hr) << std::endl;
            SafeRelease(&pIRowset);
            continue;
        }

        // Get column information
        DBORDINAL cColumns;
        WCHAR* pStringsBuffer = NULL;
        DBCOLUMNINFO* pColumnInfo = NULL;

        hr = pIColumnsInfo->GetColumnInfo(&cColumns, &pColumnInfo, &pStringsBuffer);
        if (FAILED(hr)) {
            std::wcerr << L"Failed to get column info: " << GetErrorMessage(hr) << std::endl;
            SafeRelease(&pIColumnsInfo);
            SafeRelease(&pIRowset);
            continue;
        }

        std::wcout << L"Number of columns: " << cColumns << std::endl;

        // Display column headers
        std::wcout << L"\nColumn Information:" << std::endl;
        for (DBORDINAL i = 0; i < cColumns; i++) {
            std::wcout << L"Column " << i << L": ";
            if (pColumnInfo[i].pwszName) {
                std::wcout << pColumnInfo[i].pwszName;
            }
            else {
                std::wcout << L"(No Name)";
            }
            std::wcout << L", Type = " << DBTypeToString(pColumnInfo[i].wType)
                << L", Size = " << pColumnInfo[i].ulColumnSize << std::endl;
        }

        // Create bindings using the example provided
        DBBINDING* rgBind = (DBBINDING*)CoTaskMemAlloc(cColumns * sizeof(DBBINDING));
        if (!rgBind) {
            std::wcerr << L"Failed to allocate memory for bindings" << std::endl;
            SafeRelease(&pIColumnsInfo);
            CoTaskMemFree(pColumnInfo);
            CoTaskMemFree(pStringsBuffer);
            SafeRelease(&pIRowset);
            continue;
        }

        // Set up bindings for all columns
        DWORD dwOffset = 0;
        DBCOUNTITEM iBind = 0;
        DBCOUNTITEM cBind = 0;

        for (DBORDINAL iCol = 0; iCol < cColumns; iCol++) {
            rgBind[iBind].iOrdinal = pColumnInfo[iCol].iOrdinal;
            rgBind[iBind].obValue = dwOffset + offsetof(COLUMNDATA, var);
            rgBind[iBind].obLength = dwOffset + offsetof(COLUMNDATA, dwLength);
            rgBind[iBind].obStatus = dwOffset + offsetof(COLUMNDATA, dwStatus);
            rgBind[iBind].pTypeInfo = NULL;
            rgBind[iBind].pObject = NULL;
            rgBind[iBind].pBindExt = NULL;
            rgBind[iBind].cbMaxLen = sizeof(VARIANT);
            rgBind[iBind].dwFlags = 0;
            rgBind[iBind].eParamIO = DBPARAMIO_NOTPARAM;
            rgBind[iBind].dwPart = DBPART_VALUE | DBPART_LENGTH | DBPART_STATUS;
            rgBind[iBind].dwMemOwner = DBMEMOWNER_CLIENTOWNED;
            rgBind[iBind].wType = DBTYPE_VARIANT;
            rgBind[iBind].bPrecision = 0;
            rgBind[iBind].bScale = 0;

            // Increment offset to next structure
            dwOffset += sizeof(COLUMNDATA);
            iBind++;
        }
        cBind = iBind;

        // Create the accessor
        HACCESSOR hAccessor;
        IAccessor* pIAccessor = NULL;
        hr = pIRowset->QueryInterface(IID_IAccessor, (void**)&pIAccessor);
        if (FAILED(hr)) {
            std::wcerr << L"Failed to get IAccessor: " << GetErrorMessage(hr) << std::endl;
            CoTaskMemFree(rgBind);
            SafeRelease(&pIColumnsInfo);
            CoTaskMemFree(pColumnInfo);
            CoTaskMemFree(pStringsBuffer);
            SafeRelease(&pIRowset);
            continue;
        }

        // Create the accessor with the appropriate row size (dwOffset contains the size now)
        hr = pIAccessor->CreateAccessor(
            DBACCESSOR_ROWDATA,    // Accessor for row data
            cBind,                 // Number of bindings
            rgBind,                // Binding array
            dwOffset,              // Size of row
            &hAccessor,            // Accessor handle
            NULL                   // Status array (NULL = no status needed)
        );

        if (FAILED(hr)) {
            std::wcerr << L"Failed to create accessor: " << GetErrorMessage(hr) << std::endl;
            SafeRelease(&pIAccessor);
            CoTaskMemFree(rgBind);
            SafeRelease(&pIColumnsInfo);
            CoTaskMemFree(pColumnInfo);
            CoTaskMemFree(pStringsBuffer);
            SafeRelease(&pIRowset);
            continue;
        }

        // We're done with the column info interface
        SafeRelease(&pIColumnsInfo);

        // Allocate buffer for row data
        BYTE* pRowData = new BYTE[dwOffset];

        // Display column headers for results
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

        // Fetch and display rows
        HROW hRow;
        HROW* pRows = &hRow;
        DBCOUNTITEM cRowsObtained = 0;
        int rowCount = 0;

        while (TRUE) {
            hr = pIRowset->GetNextRows(0, 0, 1, &cRowsObtained, &pRows);
            if (FAILED(hr) || cRowsObtained == 0)
                break;

            // Clear the buffer before getting new data
            memset(pRowData, 0, dwOffset);

            // Get the row data
            hr = pIRowset->GetData(hRow, hAccessor, pRowData);
            if (SUCCEEDED(hr)) {
                rowCount++;

                // Process each column data
                for (DBORDINAL i = 0; i < cColumns; i++) {
                    // Get the COLUMNDATA structure for this column
                    COLUMNDATA* pColData = (COLUMNDATA*)(pRowData + (i * sizeof(COLUMNDATA)));

                    // Check the status
                    if (pColData->dwStatus == DBSTATUS_S_OK) {
                        // Display the variant data
                        VARIANT* pVar = &(pColData->var);

                        // Convert and display the variant value
                        switch (pVar->vt) {
                        case VT_I2:
                            std::wcout << pVar->iVal << L"\t";
                            break;
                        case VT_I4:
                            std::wcout << pVar->lVal << L"\t";
                            break;
                        case VT_I8:
                            std::wcout << pVar->llVal << L"\t";
                            break;
                        case VT_R4:
                            std::wcout << pVar->fltVal << L"\t";
                            break;
                        case VT_R8:
                            std::wcout << pVar->dblVal << L"\t";
                            break;
                        case VT_BOOL:
                            std::wcout << (pVar->boolVal ? L"True" : L"False") << L"\t";
                            break;
                        case VT_BSTR:
                            std::wcout << pVar->bstrVal << L"\t";
                            break;
                        case VT_CY:
                        {
                            WCHAR szCY[64];
                            VarBstrFromCy(pVar->cyVal, LOCALE_USER_DEFAULT, 0, &pVar->bstrVal);
                            std::wcout << pVar->bstrVal << L"\t";
                            SysFreeString(pVar->bstrVal);
                        }
                        break;
                        case VT_DATE:
                        {
                            SYSTEMTIME st;
                            VariantTimeToSystemTime(pVar->date, &st);
                            WCHAR szDate[64];
                            GetDateFormatW(LOCALE_USER_DEFAULT, 0, &st, L"yyyy-MM-dd", szDate, 64);
                            std::wcout << szDate << L"\t";
                        }
                        break;
                        case VT_NULL:
                            std::wcout << L"NULL\t";
                            break;
                        case VT_EMPTY:
                            std::wcout << L"EMPTY\t";
                            break;
                            // Type 19 (0x13) is VT_UI4
                        case VT_UI4:
                            std::wcout << pVar->ulVal << L"\t";
                            break;
                        default:
                            std::wcout << L"[Unsupported variant type: " << pVar->vt << L"]\t";
                            break;
                        }
                    }
                    else if (pColData->dwStatus == DBSTATUS_S_ISNULL) {
                        std::wcout << L"NULL\t";
                    }
                    else {
                        std::wcout << L"[Status: " << pColData->dwStatus << L"]\t";
                    }
                }
                std::wcout << std::endl;

                // Clean up variants to avoid memory leaks
                for (DBORDINAL i = 0; i < cColumns; i++) {
                    COLUMNDATA* pColData = (COLUMNDATA*)(pRowData + (i * sizeof(COLUMNDATA)));
                    if (pColData->dwStatus == DBSTATUS_S_OK) {
                        VariantClear(&(pColData->var));
                    }
                }
            }
            else {
                std::wcerr << L"Failed to get row data: " << GetErrorMessage(hr) << std::endl;
            }

            // Release the row handle
            pIRowset->ReleaseRows(1, pRows, NULL, NULL, NULL);
        }

        std::wcout << L"\n" << rowCount << L" row(s) returned." << std::endl;

        // Clean up
        delete[] pRowData;
        pIAccessor->ReleaseAccessor(hAccessor, NULL);
        SafeRelease(&pIAccessor);
        CoTaskMemFree(rgBind);
        CoTaskMemFree(pColumnInfo);
        CoTaskMemFree(pStringsBuffer);
        SafeRelease(&pIRowset);
    }

    // Clean up remaining resources
    SafeRelease(&pIDBCreateCommand);

    // Uninitialize the data source
    if (pIDBInitialize) {
        pIDBInitialize->Uninitialize();
        SafeRelease(&pIDBInitialize);
    }

    CoUninitialize();
    return 0;
}
