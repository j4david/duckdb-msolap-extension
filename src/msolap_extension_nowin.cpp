#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

//-----------------------------------------------------------------------------
// Combined data structure for both binding data and state
//-----------------------------------------------------------------------------

struct MSOLAPDummyData : public TableFunctionData, public GlobalTableFunctionState {
    std::string connection_string;
    std::string dax_query;
    bool data_returned = false;
    
    idx_t MaxThreads() const override {
        return 1;
    }
};

//-----------------------------------------------------------------------------
// Table function implementation for non-Windows platforms
//-----------------------------------------------------------------------------

static unique_ptr<FunctionData> MSOLAPDummyBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<MSOLAPDummyData>();
    
    // Get connection string and DAX query from input
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

static unique_ptr<GlobalTableFunctionState> MSOLAPDummyInitGlobalState(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<MSOLAPDummyData>();
    auto result = make_uniq<MSOLAPDummyData>();
    
    result->connection_string = bind_data.connection_string;
    result->dax_query = bind_data.dax_query;
    result->data_returned = false;
    
    return std::move(result);
}

static void MSOLAPDummyScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &state = data.global_state->Cast<MSOLAPDummyData>();
    
    // If we've already returned the data, we're done
    if (state.data_returned) {
        output.SetCardinality(0);
        return;
    }
    
    // Set cardinality to 1 (one row)
    output.SetCardinality(1);
    
    // Set the message in the first column
    auto &col = output.data[0];
    col.SetValue(0,"MSOLAP extension is only supported on Windows platforms due to COM/OLEDB dependencies");
    
    // Mark that we've returned the data
    state.data_returned = true;
}

static InsertionOrderPreservingMap<string> MSOLAPDummyToString(TableFunctionToStringInput &input) {
    InsertionOrderPreservingMap<string> result;
    auto &bind_data = input.bind_data->Cast<MSOLAPDummyData>();
    
    result["Connection"] = bind_data.connection_string;
    result["Query"] = bind_data.dax_query;
    result["Platform"] = "Non-Windows (Unsupported)";
    
    return result;
}

//-----------------------------------------------------------------------------
// Extension loading
//-----------------------------------------------------------------------------

static void LoadInternal(DatabaseInstance &instance) {
    // Register the MSOLAP dummy function for non-Windows platforms
    TableFunction msolap_function("msolap", {LogicalType::VARCHAR, LogicalType::VARCHAR}, 
                                  MSOLAPDummyScan, MSOLAPDummyBind, MSOLAPDummyInitGlobalState);
    msolap_function.to_string = MSOLAPDummyToString;
    
    ExtensionUtil::RegisterFunction(instance, msolap_function);
}

class MsolapExtension : public Extension {
public:
    void Load(DuckDB &db) override {
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