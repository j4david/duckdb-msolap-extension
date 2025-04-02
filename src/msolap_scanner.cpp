#include "msolap_scanner.hpp"
#include "msolap_utils.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

unique_ptr<FunctionData> MSOLAPBind(ClientContext &context,
                                  TableFunctionBindInput &input,
                                  vector<LogicalType> &return_types,
                                  vector<string> &names) {
    auto result = make_uniq<MSOLAPBindData>();
    
    // Check the number of arguments
    if (input.inputs.size() < 2) {
        throw BinderException("MSOLAP function requires at least two arguments: connection string and DAX query");
    }
    
    // Get the connection string
    if (input.inputs[0].type().id() != LogicalTypeId::VARCHAR) {
        throw BinderException("MSOLAP connection string must be a VARCHAR");
    }
    
    // Get the DAX query
    if (input.inputs[1].type().id() != LogicalTypeId::VARCHAR) {
        throw BinderException("MSOLAP DAX query must be a VARCHAR");
    }
    
    result->connection_string = input.inputs[0].GetValue<string>();
    result->dax_query = input.inputs[1].GetValue<string>();
    
    try {
        // Connect to OLAP to get column information
        MSOLAPOpenOptions options;
        
        // Check for timeout parameter
        Value timeout_val;
        if (input.named_parameters.count("timeout") > 0) {
            timeout_val = input.named_parameters.at("timeout");
            if (timeout_val.IsNull()) {
                options.timeout_seconds = 60; // Default
            } else if (timeout_val.type().id() == LogicalTypeId::INTEGER) {
                options.timeout_seconds = timeout_val.GetValue<int32_t>();
                if (options.timeout_seconds <= 0) {
                    throw BinderException("MSOLAP timeout must be a positive integer");
                }
            } else {
                throw BinderException("MSOLAP timeout must be an integer");
            }
        }
        
        auto db = MSOLAPDB::Open(result->connection_string, options);
        auto stmt = db.Prepare(result->dax_query);
        
        // Execute the query to get column info
        stmt.Execute();
        
        // Get column types and names
        auto column_types = stmt.GetColumnTypes(); // This returns std::vector
        auto column_names = stmt.GetColumnNames(); // This returns std::vector
        
        
        // Clear the DuckDB vectors
        result->types.clear();
        result->names.clear();
        
        // Copy from std::vector to duckdb::vector
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
        stmt.Close();
        db.Close();
    } catch (const MSOLAPException& e) {
        throw BinderException("MSOLAP error: %s", e.what());
    } catch (const std::exception& e) {
        throw BinderException("MSOLAP error: %s", e.what());
    } catch (...) {
        throw BinderException("Unknown error during MSOLAP bind");
    }
    
    result->rows_per_group = optional_idx();
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
    auto& bind_data = data.bind_data->Cast<MSOLAPBindData>();
    auto& state = data.local_state->Cast<MSOLAPLocalState>();
    
    if (state.done) {
        // No more data
        return;
    }
    
    idx_t output_offset = 0;
    
    try {
        // Fetch rows up to the vector size
        while (output_offset < STANDARD_VECTOR_SIZE) {
            if (!state.stmt.Step()) {
                // No more rows
                state.done = true;
                break;
            }
            
            // Process all columns for this row
            for (idx_t col_idx = 0; col_idx < state.column_ids.size(); col_idx++) {
                auto col_id = state.column_ids[col_idx];
                if (col_id == COLUMN_IDENTIFIER_ROW_ID) {
                    // Row ID column - this is a virtual column
                    output.data[col_idx].SetValue(output_offset, Value::BIGINT(output_offset));
                } else {
                    // Regular column - get from the result set
                    auto db_col_idx = col_id;
                    auto value = state.stmt.GetValue(db_col_idx, bind_data.types[db_col_idx]);
                    output.data[col_idx].SetValue(output_offset, value);
                }
            }
            
            output_offset++;
        }
    } catch (const MSOLAPException& e) {
        throw InternalException("MSOLAP error during scan: %s", e.what());
    } catch (const std::exception& e) {
        throw InternalException("MSOLAP error during scan: %s", e.what());
    } catch (...) {
        throw InternalException("Unknown error during MSOLAP scan");
    }
    
    // Set the output size
    output.SetCardinality(output_offset);
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