#pragma once

#include <string>
#include <memory>
#include <windows.h>
#include <oledb.h>
#include "msolap_utils.hpp"

namespace duckdb {

struct MSOLAPOpenOptions {
    MSOLAPOpenOptions() : timeout_seconds(60) {}
    
    // Timeout in seconds for command execution
    uint32_t timeout_seconds;
};

class MSOLAPStatement;

class MSOLAPDB {
public:
    MSOLAPDB();
    ~MSOLAPDB();

    // Prevent copying
    MSOLAPDB(const MSOLAPDB &) = delete;
    MSOLAPDB &operator=(const MSOLAPDB &) = delete;

    // Allow moving
    MSOLAPDB(MSOLAPDB &&other) noexcept;
    MSOLAPDB &operator=(MSOLAPDB &&other) noexcept;

    // Static method to open a connection
    static MSOLAPDB Open(const std::string &connection_string, const MSOLAPOpenOptions &options = MSOLAPOpenOptions());

    // Prepare a DAX query
    MSOLAPStatement Prepare(const std::string &dax_query);
    
    // Execute a simple DAX query without returning a result set
    void Execute(const std::string &dax_query);
    
    // Check if the connection is open
    bool IsConnected() const;
    
    // Close the connection
    void Close();

    // Friend classes
    friend class MSOLAPStatement;

private:
    // Initialize the connection
    void Initialize(const std::string &connection_string, const MSOLAPOpenOptions &options);

    // COM Initializer to manage COM state
    std::unique_ptr<COMInitializer> com_initializer;
    
    // OLE DB interfaces
    IDBInitialize* pIDBInitialize;
    IDBCreateSession* pIDBCreateSession;
    IDBCreateCommand* pIDBCreateCommand;
    ICommand* pICommand;
    
    // Connection state
    bool connected;
    
    // Timeout setting
    uint32_t timeout_seconds;
};

} // namespace duckdb