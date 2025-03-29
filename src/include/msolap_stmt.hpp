#pragma once

#include "duckdb.hpp"
#include "msolap_utils.hpp"
#include <windows.h>
#include <oledb.h>
#include <string>
#include <vector>
#include <memory>

namespace duckdb {

// Forward declaration
class MSOLAPDB;

class MSOLAPStatement {
public:
    MSOLAPStatement(MSOLAPDB& db, const std::string& dax_query);
    ~MSOLAPStatement();
    
    // Prevent copying
    MSOLAPStatement(const MSOLAPStatement&) = delete;
    MSOLAPStatement& operator=(const MSOLAPStatement&) = delete;
    
    // Execute the statement and prepare for fetching results
    bool Execute();
    
    // Step to the next row
    bool Step();
    
    // Get column information
    DBORDINAL GetColumnCount() const;
    std::string GetColumnName(DBORDINAL column) const;
    DBTYPE GetColumnType(DBORDINAL column) const;
    
    // Get column values
    Value GetValue(DBORDINAL column, const LogicalType& type);
    
    // Get all column types
    std::vector<LogicalType> GetColumnTypes() const;
    
    // Get all column names
    std::vector<std::string> GetColumnNames() const;
    
    // Close the statement
    void Close();
    
private:
    // MSOLAP statement objects
    ICommand* pICommand;
    ICommandText* pICommandText;
    IRowset* pIRowset;
    IAccessor* pIAccessor;
    
    // Column information
    DBCOLUMNINFO* pColumnInfo;
    WCHAR* pStringsBuffer;
    DBORDINAL cColumns;
    
    // Row handling
    HACCESSOR hAccessor;
    HROW hRow;
    std::vector<DBBINDING> bindings;
    BYTE* pRowData;
    bool has_row;
    bool executed;
    
    // Setup column bindings
    void SetupBindings();
    
    // Free resources
    void FreeResources();
};

} // namespace duckdb