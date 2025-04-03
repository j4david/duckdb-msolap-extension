#include "duckdb.hpp"
#include "msolap_scanner.hpp"
#include "msolap_utils.hpp"
#include <stdexcept>

namespace duckdb {

static unique_ptr<FunctionData> MSOLAPBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
    MSOLAPConnection::InitializeCOM();
    auto result = make_uniq<MSOLAPBindData>();
    
    // Get connection string and DAX query from input
    result->connection_string = input.inputs[0].GetValue<string>();
    result->dax_query = input.inputs[1].GetValue<string>();
    
    try {
        // Connect to MSOLAP and execute query to get column information
        MSOLAPConnection connection = MSOLAPConnection::Connect(result->connection_string);
        IRowset* rowset = connection.ExecuteQuery(result->dax_query);
        
        // Get column information
        if (!connection.GetColumnInfo(rowset, result->names, result->types)) {
            MSOLAPUtils::SafeRelease(&rowset);
            throw std::runtime_error("Failed to get column information");
        }
        
        MSOLAPUtils::SafeRelease(&rowset);
        connection.Close();
        
        // Copy output column names and types
        names.clear();
        return_types.clear();

        for (auto &name : result->names) {
            names.push_back(name);
        }

        for (auto &type : result->types) {
            return_types.push_back(type);
        }
        
    } catch (std::exception &e) {
        throw std::runtime_error("MSOLAP connection failed: " + string(e.what()));
    }
    
    if (names.empty()) {
        throw std::runtime_error("No columns found in DAX query result");
    }
    
    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> MSOLAPInitGlobalState(ClientContext &context,
                                                              TableFunctionInitInput &input) {
    // MSOLAP doesn't support parallel execution
    return make_uniq<MSOLAPGlobalState>(1);
}

static unique_ptr<LocalTableFunctionState>
MSOLAPInitLocalState(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *global_state) {
    MSOLAPConnection::InitializeCOM();
    auto &bind_data = input.bind_data->Cast<MSOLAPBindData>();
    auto result = make_uniq<MSOLAPLocalState>();
    
    try {
        // Connect to MSOLAP
        result->connection = MSOLAPConnection::Connect(bind_data.connection_string);
        
        // Execute the DAX query
        result->rowset = result->connection.ExecuteQuery(bind_data.dax_query);
        
        // Get the IAccessor interface
        HRESULT hr = result->rowset->QueryInterface(IID_IAccessor, (void**)&result->accessor);
        if (FAILED(hr)) {
            throw std::runtime_error("Failed to get IAccessor: " + MSOLAPUtils::GetErrorMessage(hr));
        }
        
        // Create bindings for all columns
        DBORDINAL column_count = bind_data.names.size();
        result->bindings = (DBBINDING*)CoTaskMemAlloc(column_count * sizeof(DBBINDING));
        if (!result->bindings) {
            throw std::runtime_error("Failed to allocate memory for bindings");
        }

        // Get column information using IColumnsInfo
        IColumnsInfo* pIColumnsInfo = NULL;
        hr = result->rowset->QueryInterface(IID_IColumnsInfo, (void**)&pIColumnsInfo);
        if (FAILED(hr)) {
            throw std::runtime_error("Failed to get IColumnsInfo: " + MSOLAPUtils::GetErrorMessage(hr));
        }

        // Get column information
        DBORDINAL cColumns;
        WCHAR* pStringsBuffer = NULL;
        DBCOLUMNINFO* pColumnInfo = NULL;

        hr = pIColumnsInfo->GetColumnInfo(&cColumns, &pColumnInfo, &pStringsBuffer);
        if (FAILED(hr)) {
            MSOLAPUtils::SafeRelease(&pIColumnsInfo);
            throw std::runtime_error("Failed to get column info: " + MSOLAPUtils::GetErrorMessage(hr));
        }
        
        // Set up bindings for all columns
        DWORD dwOffset = 0;
        for (DBORDINAL i = 0; i < column_count; i++) {
            result->bindings[i].iOrdinal = pColumnInfo[i].iOrdinal; // 1-based ordinals
            result->bindings[i].obValue = dwOffset + offsetof(ColumnData, var);
            result->bindings[i].obLength = dwOffset + offsetof(ColumnData, dwLength);
            result->bindings[i].obStatus = dwOffset + offsetof(ColumnData, dwStatus);
            result->bindings[i].pTypeInfo = NULL;
            result->bindings[i].pObject = NULL;
            result->bindings[i].pBindExt = NULL;
            result->bindings[i].cbMaxLen = sizeof(VARIANT);
            result->bindings[i].dwFlags = 0;
            result->bindings[i].eParamIO = DBPARAMIO_NOTPARAM;
            result->bindings[i].dwPart = DBPART_VALUE | DBPART_LENGTH | DBPART_STATUS;
            result->bindings[i].dwMemOwner = DBMEMOWNER_CLIENTOWNED;
            result->bindings[i].wType = DBTYPE_VARIANT;
            result->bindings[i].bPrecision = 0;
            result->bindings[i].bScale = 0;
            
            // Increment offset to next structure
            dwOffset += sizeof(ColumnData);
        }

        // Clean up
        CoTaskMemFree(pColumnInfo);
        CoTaskMemFree(pStringsBuffer);
        MSOLAPUtils::SafeRelease(&pIColumnsInfo);

        // Create the accessor
        hr = result->accessor->CreateAccessor(
            DBACCESSOR_ROWDATA,
            column_count,
            result->bindings,
            dwOffset,
            &result->haccessor,
            NULL
        );
        
        if (FAILED(hr)) {
            throw std::runtime_error("Failed to create accessor: " + MSOLAPUtils::GetErrorMessage(hr));
        }
        
        // Allocate buffer for row data
        result->row_size = dwOffset;
        result->row_data = new BYTE[result->row_size];
        
        result->done = false;
        
    } catch (std::exception &e) {
        throw std::runtime_error("MSOLAP scan initialization failed: " + string(e.what()));
    }
    
    return std::move(result);
}

static void MSOLAPScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &state = data.local_state->Cast<MSOLAPLocalState>();
    auto &bind_data = data.bind_data->Cast<MSOLAPBindData>();
    
    if (state.done) {
        return;
    }
    
    // Process rows in batches
    const DBROWCOUNT batch_size = STANDARD_VECTOR_SIZE;
    HROW* pRows = new HROW[batch_size];
    DBCOUNTITEM cRowsObtained = 0;
    
    HRESULT hr = state.rowset->GetNextRows(0, 0, batch_size, &cRowsObtained, &pRows);
    if (FAILED(hr)) {
        delete[] pRows;
        throw std::runtime_error("Failed to get rows: " + MSOLAPUtils::GetErrorMessage(hr));
    }
    
    if (cRowsObtained == 0) {
        state.done = true;
        delete[] pRows;
        return;
    }
    
    // Prepare vectors for each column type
    const idx_t col_count = output.ColumnCount();
    std::vector<std::vector<Value>> column_values(col_count);
    for (idx_t col = 0; col < col_count; col++) {
        column_values[col].reserve(cRowsObtained);
    }
    
    // Get data for all rows
    for (DBCOUNTITEM i = 0; i < cRowsObtained; i++) {
        // Clear the buffer before getting new data
        memset(state.row_data, 0, state.row_size);
        
        // Get the row data
        hr = state.rowset->GetData(pRows[i], state.haccessor, state.row_data);
        if (FAILED(hr)) {
            // On error, add NULL values for all columns
            for (idx_t col = 0; col < col_count; col++) {
                column_values[col].push_back(Value());
            }
            continue;
        }
        
        // Extract values for each column
        for (idx_t col = 0; col < col_count; col++) {
            // Get the COLUMNDATA structure for this column
            ColumnData* pColData = (ColumnData*)(state.row_data + (col * sizeof(ColumnData)));
            
            if (pColData->dwStatus == DBSTATUS_S_OK) {
                // Convert VARIANT to DuckDB value
                Value val = MSOLAPUtils::ConvertVariantToValue(&(pColData->var));
                column_values[col].push_back(val);
                
                // Clear variant to avoid memory leaks
                VariantClear(&(pColData->var));
            } else {
                // Add NULL for NULL or error values
                column_values[col].push_back(Value());
            }
        }
    }
    
    // Release the row handles
    state.rowset->ReleaseRows(cRowsObtained, pRows, NULL, NULL, NULL);
    delete[] pRows;
    
    // Now use bulk operations to populate the output vectors
    for (idx_t col = 0; col < col_count; col++) {
        auto &out_vec = output.data[col];
        
        // For each column, set all values at once
        for (idx_t row = 0; row < column_values[col].size(); row++) {
            out_vec.SetValue(row, column_values[col][row]);
        }
    }
    
    output.SetCardinality(cRowsObtained);
}

static InsertionOrderPreservingMap<string> MSOLAPToString(TableFunctionToStringInput &input) {
    InsertionOrderPreservingMap<string> result;
    auto &bind_data = input.bind_data->Cast<MSOLAPBindData>();
    
    result["Connection"] = bind_data.connection_string;
    result["Query"] = bind_data.dax_query;
    
    return result;
}

MSOLAPScanFunction::MSOLAPScanFunction()
    : TableFunction("msolap", {LogicalType::VARCHAR, LogicalType::VARCHAR}, MSOLAPScan, MSOLAPBind,
                    MSOLAPInitGlobalState, MSOLAPInitLocalState) {
    to_string = MSOLAPToString;
}

} // namespace duckdb