//===----------------------------------------------------------------------===//
//                         DuckDB
//
// avro_copy.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "avro_common.hpp"

namespace duckdb_avro {

struct AvroCopyFunction {
	//! COPY ... TO ... (FORMAT avro)
	static void Register(cxx::Extension &extension);
};

} // namespace duckdb_avro
