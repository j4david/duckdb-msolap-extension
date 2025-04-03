# MSOLAP Scanner Extension for DuckDB

This extension allows DuckDB to connect to Microsoft SQL Server Analysis Services (SSAS) and other OLAP data sources using the MSOLAP provider. It enables querying multidimensional and tabular models with DAX queries directly from DuckDB.

## Features

- Connect to MSOLAP sources (SSAS, Power BI, etc.)
- Scan tables/cubes from OLAP databases
- Execute raw DAX queries

## Requirements

- Windows environment (due to MSOLAP COM dependencies)
- Microsoft OLEDB provider for Analysis Services installed `MSOLAP.8`
- DuckDB development environment

## Building

1. Clone this repository
2. Make sure you have DuckDB development environment set up
3. Build the extension using CMake:

```bash
mkdir build
cd build
cmake ..
cmake --build . --config Release
```
or given repo location `c:/git/hub/duckdb-msolap-extension/`
```bash
make release -e EXT_CONFIG='c:/git/hub/duckdb-msolap-extension/extension_config.cmake'
```
## Installation

```sql
INSTALL msolap FROM community;
LOAD msolap;
```

## Usage

### Executing DAX queries

```sql
-- Execute a custom DAX query
SELECT * FROM msolap('Data Source=localhost;Catalog=AdventureWorks', 'EVALUATE DimProduct');

-- More complex DAX query against PowerBI Desktop instance
SELECT * FROM msolap('Data Source=localhost:61324;Catalog=0ec50266-bdf5-4582-bc8c-82584866bcb7', 
'EVALUATE
SUMMARIZECOLUMNS(
    DimProduct[Color],
    "Total Sales", SUM(FactInternetSales[SalesAmount])
)');
```

## Functions

The extension provides one main function:

1. `msolap(connection_string, dax_query)` - Execute a custom DAX query

The expected `connection_string` format: _"Data Source=localhost;Catalog=AdventureWorks"_

## Limitations

- Windows-only due to COM dependencies
- Limited data type conversion for complex OLAP types
- Limited support for calculated measures and hierarchies
- No authentication (yet)

## License

This extension is provided under the MIT License.