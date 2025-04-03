//===----------------------------------------------------------------------===//
//                         DuckDB
//
// msolap_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "msolap_utils.hpp"
#include "msolap_connection.hpp"
#include <memory>

namespace duckdb {

struct MSOLAPBindData : public TableFunctionData {
    std::string connection_string;
    std::string dax_query;
    
    std::vector<std::string> names;
    std::vector<LogicalType> types;
};

struct MSOLAPLocalState : public LocalTableFunctionState {
    MSOLAPConnection connection;
    IRowset* rowset;
    IAccessor* accessor;
    HACCESSOR haccessor;
    DBBINDING* bindings;
    BYTE* row_data;
    DWORD row_size;
    bool done;
    
    MSOLAPLocalState() : rowset(nullptr), accessor(nullptr), haccessor(NULL), 
                        bindings(nullptr), row_data(nullptr), row_size(0), done(false) {}
    
    ~MSOLAPLocalState() {
        // Clean up resources
        if (row_data) {
            delete[] row_data;
            row_data = nullptr;
        }
        
        if (bindings) {
            CoTaskMemFree(bindings);
            bindings = nullptr;
        }
        
        if (accessor && haccessor) {
            accessor->ReleaseAccessor(haccessor, NULL);
            haccessor = NULL;
        }
        
        if (accessor) {
            MSOLAPUtils::SafeRelease(&accessor);
        }
        
        if (rowset) {
            MSOLAPUtils::SafeRelease(&rowset);
        }
    }
};

struct MSOLAPGlobalState : public GlobalTableFunctionState {
    idx_t max_threads;
    
    explicit MSOLAPGlobalState(idx_t max_threads) : max_threads(max_threads) {}
    
    idx_t MaxThreads() const override {
        return max_threads;
    }
};

class MSOLAPScanFunction : public TableFunction {
public:
    MSOLAPScanFunction();
};

} // namespace duckdb