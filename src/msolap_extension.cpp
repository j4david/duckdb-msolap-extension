#define DUCKDB_EXTENSION_MAIN

#include "msolap_extension.hpp"
#include "msolap_scanner.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
    // Register MSOLAP table function
    MSOLAPScanFunction msolap_scan_fun;
    ExtensionUtil::RegisterFunction(instance, msolap_scan_fun);
}

void MSOLAPExtension::Load(DuckDB &db) {
    LoadInternal(*db.instance);
}

std::string MSOLAPExtension::Name() {
    return "msolap";
}

std::string MSOLAPExtension::Version() const {
#ifdef EXT_VERSION_MSOLAP
    return EXT_VERSION_MSOLAP;
#else
    return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void msolap_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::MSOLAPExtension>();
}

DUCKDB_EXTENSION_API const char *msolap_version() {
    return duckdb::DuckDB::LibraryVersion();
}

}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif