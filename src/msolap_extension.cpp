#include "msolap_extension.hpp"
#include "msolap_scanner.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

void MSOLAPExtension::Load(DuckDB& db) {
    Connection con(db);
    con.BeginTransaction();
    
    // Register MSOLAP table function
    auto msolap_scan_fun = make_uniq<MSOLAPScanFunction>();
    ExtensionUtil::RegisterFunction(*con.context, std::move(msolap_scan_fun));
    
    con.Commit();
}

std::string MSOLAPExtension::Name() {
    return "msolap";
}

extern "C" {

DUCKDB_EXTENSION_API void msolap_init(duckdb::DatabaseInstance& db) {
    LoadInternal(db);
}

DUCKDB_EXTENSION_API const char* msolap_version() {
    return DuckDB::LibraryVersion();
}

}

void LoadInternal(DatabaseInstance& instance) {
    auto& db = *instance.db;
    MSOLAPExtension extension;
    extension.Load(db);
    db.extension_manager->AddExtension(&extension);
}

} // namespace duckdb