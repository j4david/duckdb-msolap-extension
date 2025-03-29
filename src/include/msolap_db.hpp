#pragma once

#include "duckdb.hpp"
#include "msolap_utils.hpp"
#include <windows.h>
#include <oledb.h>
#include <string>
#include <memory>

namespace duckdb {

// Forward declaration
class MSOLAPStatement;

class MSOLAPDB {
public:
    MSOLAPDB();
    ~MSOLAPDB();
    
    // Prevent copying
    MSOLAPDB(const MSOLAPDB&) = delete;
    MSOLAPDB& operator=(const MSOLAPDB&) = delete;
    
    // Open a connection to SSAS
    static MSOLAPDB Open(const std::string& connection_string);
    
    // Close the connection
    void Close();
    
    // Create a prepared statement for a DAX query
    MSOLAPStatement Prepare(const std::string& dax_query);
    
    // Check if connected
    bool IsConnected() const;
    
private:
    // Initialize the connection
    void Initialize(const std::string& connection_string);
    
    // COM initializer
    std::unique_ptr<COMInitializer> com_initializer;
    
    // MSOLAP connection objects
    IDBInitialize* pIDBInitialize;
    IDBCreateSession* pIDBCreateSession;
    IDBCreateCommand* pIDBCreateCommand;
    ICommand* pICommand;
    
    // Connection state
    bool connected;
};

} // namespace duckdb