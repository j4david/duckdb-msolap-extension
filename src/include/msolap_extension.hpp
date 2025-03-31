#pragma once

#include "duckdb.hpp"

namespace duckdb {

class MSOLAPExtension : public Extension {
public:
    // Attach the extension to the database
    void Load(DuckDB& db) override;
    // Return the name of the extension
    std::string Name() override;
    std::string Version() const override;
};

} // namespace duckdb