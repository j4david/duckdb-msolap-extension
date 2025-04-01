#include "msolap_scanner.hpp"
#include "msolap_utils.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

unique_ptr<FunctionData> MSOLAPBind(ClientContext &context,
                                  TableFunctionBindInput &input,
                                  vector<LogicalType> &return_types,
                                  vector<string> &names) {
    MSOLAP_LOG("Begin MSOLAPBind");
    auto result = make_uniq<MSOLAPBindData>();
    
    // Check the number of arguments
    if (input.inputs.size() < 2) {
        MSOLAP_LOG("Not enough arguments");
        throw BinderException("MSOLAP function requires at least two arguments: connection string and DAX query");
    }
    
    // Get the connection string
    if (input.inputs[0].type().id() != LogicalTypeId::VARCHAR) {
        MSOLAP_LOG("Connection string is not VARCHAR");
        throw BinderException("MSOLAP connection string must be a VARCHAR");
    }
    
    // Get the DAX query
    if (input.inputs[1].type().id() != LogicalTypeId::VARCHAR) {
        MSOLAP_LOG("DAX query is not VARCHAR");
        throw BinderException("MSOLAP DAX query must be a VARCHAR");
    }
    
    result->connection_string = input.inputs[0].GetValue<string>();
    result->dax_query = input.inputs[1].GetValue<string>();
    MSOLAP_LOG("Connection string: " + result->connection_string);
    MSOLAP_LOG("DAX query: " + result->dax_query);
    
    try {
        // Connect to OLAP to get column information
        MSOLAPOpenOptions options;
        
        // Check for timeout parameter
        Value timeout_val;
        if (input.named_parameters.count("timeout") > 0) {
            timeout_val = input.named_parameters.at("timeout");
            if (timeout_val.IsNull()) {
                options.timeout_seconds = 60; // Default
                MSOLAP_LOG("Using default timeout: 60");
            } else if (timeout_val.type().id() == LogicalTypeId::INTEGER) {
                options.timeout_seconds = timeout_val.GetValue<int32_t>();
                MSOLAP_LOG("Using provided timeout: " + std::to_string(options.timeout_seconds));
                if (options.timeout_seconds <= 0) {
                    MSOLAP_LOG("Invalid timeout: " + std::to_string(options.timeout_seconds));
                    throw BinderException("MSOLAP timeout must be a positive integer");
                }
            } else {
                MSOLAP_LOG("Timeout is not an integer");
                throw BinderException("MSOLAP timeout must be an integer");
            }
        }
        
        MSOLAP_LOG("Opening database connection");
        auto db = MSOLAPDB::Open(result->connection_string, options);
        MSOLAP_LOG("Preparing statement");
        auto stmt = db.Prepare(result->dax_query);
        
        // Execute the query to get column info
        MSOLAP_LOG("Executing query to get column info");
        stmt.Execute();
        MSOLAP_LOG("Query executed for column info");
        
        // Get column count
        DBORDINAL column_count = stmt.GetColumnCount();
        MSOLAP_LOG("Column count: " + std::to_string(column_count));
        
        // Make sure we have at least one column
        if (column_count == 0) {
            column_count = 1;
        }
        
        // Get column types
        std::vector<LogicalType> column_types;
        for (DBORDINAL i = 0; i < column_count; i++) {
            try {
                DBTYPE type = stmt.GetColumnType(i);
                column_types.push_back(DBTypeToLogicalType(type));
            } catch (...) {
                // Default to VARCHAR for any problematic column
                column_types.push_back(LogicalType::VARCHAR);
            }
        }
        
        // Generate safe column names
        std::vector<std::string> column_names;
        for (DBORDINAL i = 0; i < column_count; i++) {
            column_names.push_back("Column_" + std::to_string(i));
        }
        
        // Clear the DuckDB vectors
        result->types.clear();
        result->names.clear();
        
        // Copy to our result
        for (size_t i = 0; i < column_types.size(); i++) {
            result->types.emplace_back(column_types[i]);
        }
        
        for (size_t i = 0; i < column_names.size(); i++) {
            result->names.emplace_back(column_names[i]);
        }
        
        // Copy to return parameters
        return_types.clear();
        names.clear();
        
        for (size_t i = 0; i < result->types.size(); i++) {
            return_types.emplace_back(result->types[i]);
        }
        
        for (size_t i = 0; i < result->names.size(); i++) {
            names.emplace_back(result->names[i]);
        }
        
        // Close the statement and database
        MSOLAP_LOG("Closing statement and database");
        stmt.Close();
        db.Close();
        
    } catch (const MSOLAPException& e) {
        MSOLAP_LOG("MSOLAP exception during bind: " + std::string(e.what()));
        throw BinderException("MSOLAP error: %s", e.what());
    } catch (const std::exception& e) {
        MSOLAP_LOG("Standard exception during bind: " + std::string(e.what()));
        throw BinderException("MSOLAP error: %s", e.what());
    } catch (...) {
        MSOLAP_LOG("Unknown exception during bind");
        throw BinderException("Unknown error during MSOLAP bind");
    }
    
    result->rows_per_group = optional_idx();
    MSOLAP_LOG("MSOLAPBind completed");
    return std::move(result);
}

unique_ptr<GlobalTableFunctionState> MSOLAPInitGlobalState(ClientContext& context,
                                                         TableFunctionInitInput& input) {
    return make_uniq<MSOLAPGlobalState>(context.db->NumberOfThreads());
}

unique_ptr<LocalTableFunctionState> MSOLAPInitLocalState(ExecutionContext& context,
                                                       TableFunctionInitInput& input,
                                                       GlobalTableFunctionState* global_state) {
    auto& bind_data = input.bind_data->Cast<MSOLAPBindData>();
    auto result = make_uniq<MSOLAPLocalState>();
    
    try {
        // Initialize the connection
        MSOLAPOpenOptions options;
        
        // Set options from bind data if available
        Value timeout_val;
        if (context.client.TryGetCurrentSetting("msolap_timeout", timeout_val)) {
            if (!timeout_val.IsNull() && timeout_val.type().id() == LogicalTypeId::INTEGER) {
                options.timeout_seconds = timeout_val.GetValue<int32_t>();
            }
        }
        
        if (bind_data.global_db) {
            // Use the global connection
            result->db = bind_data.global_db;
        } else {
            // Create a new connection
            result->owned_db = MSOLAPDB::Open(bind_data.connection_string, options);
            result->db = &result->owned_db;
        }
        
        // Prepare the statement
        result->stmt = result->db->Prepare(bind_data.dax_query);
        
        // Execute the query
        result->stmt.Execute();
        
        // Store column IDs for projection pushdown
        result->column_ids = input.column_ids;
    } catch (const MSOLAPException& e) {
        throw InternalException("MSOLAP error during initialization: %s", e.what());
    }
    
    return std::move(result);
}

void MSOLAPScan(ClientContext& context,
              TableFunctionInput& data,
              DataChunk& output) {
    MSOLAP_LOG("Begin MSOLAPScan");
    auto& bind_data = data.bind_data->Cast<MSOLAPBindData>();
    auto& state = data.local_state->Cast<MSOLAPLocalState>();
    
    if (state.done) {
        // No more data
        MSOLAP_LOG("State is done, returning");
        return;
    }
    
    idx_t output_offset = 0;
    
    try {
        // Fetch rows up to the vector size
        MSOLAP_LOG("Fetching up to " + std::to_string(STANDARD_VECTOR_SIZE) + " rows");
        while (output_offset < STANDARD_VECTOR_SIZE) {
            MSOLAP_LOG("Calling Step() for row " + std::to_string(output_offset));
            if (!state.stmt.Step()) {
                // No more rows
                MSOLAP_LOG("No more rows, setting done flag");
                state.done = true;
                break;
            }
            
            // Process all columns for this row
            MSOLAP_LOG("Processing " + std::to_string(state.column_ids.size()) + " columns for row " + std::to_string(output_offset));
            for (idx_t col_idx = 0; col_idx < state.column_ids.size(); col_idx++) {
                auto col_id = state.column_ids[col_idx];
                if (col_id == COLUMN_IDENTIFIER_ROW_ID) {
                    // Row ID column - this is a virtual column
                    MSOLAP_LOG("Setting row ID for column " + std::to_string(col_idx));
                    output.data[col_idx].SetValue(output_offset, Value::BIGINT(output_offset));
                } else {
                    // Regular column - get from the result set
                    auto db_col_idx = col_id;
                    MSOLAP_LOG("Getting value for column " + std::to_string(db_col_idx) + " with type " + bind_data.types[db_col_idx].ToString());
                    auto value = state.stmt.GetValue(db_col_idx, bind_data.types[db_col_idx]);
                    MSOLAP_LOG("Setting value for column " + std::to_string(col_idx));
                    output.data[col_idx].SetValue(output_offset, value);
                }
            }
            
            output_offset++;
        }
    } catch (const MSOLAPException& e) {
        MSOLAP_LOG("Exception during scan: " + std::string(e.what()));
        throw InternalException("MSOLAP error during scan: %s", e.what());
    } catch (const std::exception& e) {
        MSOLAP_LOG("Standard exception during scan: " + std::string(e.what()));
        throw InternalException("MSOLAP error during scan: %s", e.what());
    } catch (...) {
        MSOLAP_LOG("Unknown exception during scan");
        throw InternalException("Unknown error during MSOLAP scan");
    }
    
    // Set the output size
    MSOLAP_LOG("Setting output cardinality to " + std::to_string(output_offset));
    output.SetCardinality(output_offset);
    MSOLAP_LOG("End MSOLAPScan");
}

MSOLAPScanFunction::MSOLAPScanFunction()
    : TableFunction("msolap", {LogicalType::VARCHAR, LogicalType::VARCHAR}, MSOLAPScan, MSOLAPBind,
                    MSOLAPInitGlobalState, MSOLAPInitLocalState) {
    // Set properties
    projection_pushdown = true;
    
    // Add named parameters
    named_parameters["timeout"] = LogicalType::INTEGER;
}

} // namespace duckdb