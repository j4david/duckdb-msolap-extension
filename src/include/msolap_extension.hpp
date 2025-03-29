#pragma once

#include "duckdb.hpp"

namespace duckdb {

class MSOLAPExtension : public Extension {
public:
    // Attach the extension to the database
    void Load(DuckDB& db) override;
    // Detach the extension from the database
    void Unload(DuckDB& db) override;
    // Return the name of the extension
    std::string Name() override;
};

} // namespace duckdb