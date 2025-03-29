#pragma once

#include "duckdb.hpp"
#include "msolap_db.hpp"
#include "msolap_stmt.hpp"
#include <memory>
#include <vector>
#include <string>

namespace duckdb {

struct MSOLAPBindData : public TableFunctionData {
    // Connection string
    std::string connection_string;
    
    // DAX query to execute
    std::string dax_query;
    
    // Column information
    std::vector<std::string> names;
    std::vector<LogicalType> types;
    
    // Optional: Global connection to reuse
    MSOLAPDB* global_db;
    
    MSOLAPBindData() : global_db(nullptr) {}
};

struct MSOLAPLocalState : public LocalTableFunctionState {
    MSOLAPDB* db;
    MSOLAPDB owned_db;
    MSOLAPStatement stmt;
    bool done;
    std::vector<column_t> column_ids;
    
    MSOLAPLocalState() : db(nullptr), done(false) {}
};

struct MSOLAPGlobalState : public GlobalTableFunctionState {
    explicit MSOLAPGlobalState(idx_t max_threads) : max_threads(max_threads) {}
    
    idx_t max_threads;
    
    idx_t MaxThreads() const override {
        return max_threads;
    }
};

// Bind function
static unique_ptr<FunctionData> MSOLAPBind(ClientContext& context,
                                          TableFunctionBindInput& input,
                                          vector<LogicalType>& return_types,
                                          vector<string>& names);

// Init global state function
static unique_ptr<GlobalTableFunctionState> MSOLAPInitGlobalState(ClientContext& context,
                                                                TableFunctionInitInput& input);

// Init local state function
static unique_ptr<LocalTableFunctionState> MSOLAPInitLocalState(ExecutionContext& context,
                                                              TableFunctionInitInput& input,
                                                              GlobalTableFunctionState* global_state);

// Scan function
static void MSOLAPScan(ClientContext& context,
                      TableFunctionInput& data,
                      DataChunk& output);

// Create the MSOLAP table function
class MSOLAPScanFunction : public TableFunction {
public:
    MSOLAPScanFunction();
};

} // namespace duckdb