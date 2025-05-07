#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

class MsolapExtension : public Extension {
public:
    void Load(DuckDB& db) override {
        LoadInternal(*db.instance);
    }
    
    std::string Name() override {
        return "msolap";
    }
    
    std::string Version() const override {
#ifdef EXT_VERSION_MSOLAP
        return EXT_VERSION_MSOLAP;
#else
        return "";
#endif
    }
};

// Data structure to store the connection and query parameters
struct MSOLAPDummyData : public TableFunctionData {
    std::string connection_string;
    std::string dax_query;
};

// Scan function that returns a message about Windows-only support
static void MSOLAPUnsupportedScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    output.SetCardinality(1);
    auto &col = output.data[0];
    col.SetValue(0, Value("MSOLAP extension is only supported on Windows platforms due to COM/OLEDB dependencies"));
}

// Bind function that captures the input parameters but only sets up a message column
static unique_ptr<FunctionData> MSOLAPUnsupportedBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<MSOLAPDummyData>();
    
    // Get connection string and query from input
    if (input.inputs.size() >= 1) {
        result->connection_string = input.inputs[0].GetValue<string>();
    }
    if (input.inputs.size() >= 2) {
        result->dax_query = input.inputs[1].GetValue<string>();
    }
    
    // Set up a single VARCHAR column for the message
    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("message");
    
    return std::move(result);
}

// Initialize global state
static unique_ptr<GlobalTableFunctionState> MSOLAPUnsupportedInitGlobalState(ClientContext &context,
                                                              TableFunctionInitInput &input) {
    return make_uniq<GlobalTableFunctionState>(1);
}

// Local state initialization is a no-op
static unique_ptr<LocalTableFunctionState>
MSOLAPUnsupportedInitLocalState(ExecutionContext &context, TableFunctionInitInput &input, 
                              GlobalTableFunctionState *global_state) {
    return nullptr;
}

// ToString function to display info about the function
static InsertionOrderPreservingMap<string> MSOLAPUnsupportedToString(TableFunctionToStringInput &input) {
    InsertionOrderPreservingMap<string> result;
    auto &bind_data = input.bind_data->Cast<MSOLAPDummyData>();
    
    result["Connection"] = bind_data.connection_string;
    result["Query"] = bind_data.dax_query;
    result["Platform"] = "Non-Windows (Unsupported)";
    
    return result;
}

// Register the msolap function
static void LoadInternal(DatabaseInstance &instance) {
    TableFunction msolap_dummy("msolap", {LogicalType::VARCHAR, LogicalType::VARCHAR}, 
                              MSOLAPUnsupportedScan, MSOLAPUnsupportedBind,
                              MSOLAPUnsupportedInitGlobalState, MSOLAPUnsupportedInitLocalState);
    msolap_dummy.to_string = MSOLAPUnsupportedToString;
    ExtensionUtil::RegisterFunction(instance, msolap_dummy);
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void msolap_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::MsolapExtension>();
}

DUCKDB_EXTENSION_API const char *msolap_version() {
    return duckdb::DuckDB::LibraryVersion();
}

}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif