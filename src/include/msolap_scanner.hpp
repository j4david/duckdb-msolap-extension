#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/common/mutex.hpp"
#include "msolap_db.hpp"
#include "msolap_stmt.hpp"

namespace duckdb {

struct MSOLAPBindData : public TableFunctionData {
    // Connection string to the MSOLAP server
    std::string connection_string;
    
    // DAX query to execute
    std::string dax_query;
    
    // Result column types and names
    std::vector<LogicalType> types;
    std::vector<std::string> names;
    
    // Optional rows per thread for parallel scans
    optional_idx rows_per_group;
    
    // If set, all connections share this database connection
    MSOLAPDB* global_db = nullptr;
};

struct MSOLAPGlobalState : public GlobalTableFunctionState {
    explicit MSOLAPGlobalState(idx_t max_threads) : max_threads(max_threads) {}
    
    mutex lock;
    idx_t max_threads;
    
    idx_t MaxThreads() const override {
        return max_threads;
    }
};

struct MSOLAPLocalState : public LocalTableFunctionState {
    // Database connection
    MSOLAPDB* db;
    MSOLAPDB owned_db;
    
    // Statement handle
    MSOLAPStatement stmt;
    
    // Column IDs for projection pushdown
    vector<column_t> column_ids;
    
    // State tracking
    bool done = false;
    
    ~MSOLAPLocalState() {
        stmt.Close();
    }
};

// MSOLAP scan function definition
class MSOLAPScanFunction : public TableFunction {
public:
    MSOLAPScanFunction();
};

// Function declarations for scanner
unique_ptr<FunctionData> MSOLAPBind(ClientContext &context, TableFunctionBindInput &input,
                                   vector<LogicalType> &return_types, vector<string> &names);

unique_ptr<GlobalTableFunctionState> MSOLAPInitGlobalState(ClientContext &context, TableFunctionInitInput &input);

unique_ptr<LocalTableFunctionState> MSOLAPInitLocalState(ExecutionContext &context, TableFunctionInitInput &input,
                                                       GlobalTableFunctionState *global_state);

void MSOLAPScan(ClientContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace duckdb