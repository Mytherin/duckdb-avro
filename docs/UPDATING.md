# Extension updating 
When cloning this template, the target version of DuckDB should be the latest stable release of DuckDB. However, there 
will inevitably come a time when a new DuckDB is released and the extension repository needs updating. This process goes
as follows:

- Bump submodules
  - `./duckdb` should be set to latest tagged release
  - `./extension-ci-tools` should be set to updated branch corresponding to latest DuckDB release. So if you're building for DuckDB `v1.1.0` there will be a branch in `extension-ci-tools` named `v1.1.0` to which you should check out. 
- Bump versions in `./github/workflows`
  - `duckdb_version` input in `duckdb-stable-build` job in `MainDistributionPipeline.yml` should be set to latest tagged release
  - `duckdb_version` input in `duckdb-stable-deploy` job in `MainDistributionPipeline.yml` should be set to latest tagged release
  - the reusable workflow `duckdb/extension-ci-tools/.github/workflows/_extension_distribution.yml` for the `duckdb-stable-build` job should be set to latest tagged release

# API changes
This extension is built against DuckDB's stable C++ API (`duckdb/tools/cpp/duckdb_cpp.hpp`), which wraps the V2 C API,
rather than against DuckDB's internal C++ API. It does not include any other DuckDB header.

It uses parts of that API that are not stabilized yet (multi-file functions, column identifiers and file metadata,
batch claiming, written statistics of `COPY ... TO`, the external file cache flag). These are marked "Unstable API" in
`duckdb_cpp.hpp`, and until they are stabilized the extension is built as a *pinned* extension
(`build_static_extension_capi_unstable` / `build_loadable_extension_capi_unstable` in `CMakeLists.txt`): a build only
loads into the exact DuckDB version it was built against. Once these functions are stabilized, the build can switch to
the versioned helpers (`build_loadable_extension_capi_v2`), and a single build then loads into every later DuckDB
version.

When updating the DuckDB version, check the "Unstable API" parts of `duckdb_cpp.hpp` for changes - everything else
this extension uses is stable.
