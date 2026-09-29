//===----------------------------------------------------------------------===//
//                         DuckDB
//
// avro_common.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_cpp.hpp"

#include <cstdio>
#include <string>
#include <utility>

namespace duckdb_avro {

namespace cxx = duckdb::cxx;
using cxx::idx_t;
using cxx::LogicalTypeId;
using std::string;

//! The error codes of the C API an error is reported with, which decide the kind of error the query fails with
enum class ErrorCode : int {
	IO = 1004,
	INVALID_INPUT = 2001,
	OUT_OF_RANGE = 2003,
	BINDER = 5003,
	NOT_IMPLEMENTED = 5009,
	INVALID_CONFIGURATION = 7002,
	INTERNAL = 8001,
};

template <class... ARGS>
string FormatString(const char *format, ARGS... args) {
	auto size = std::snprintf(nullptr, 0, format, args...);
	string result(static_cast<size_t>(size), '\0');
	std::snprintf(&result[0], result.size() + 1, format, args...);
	return result;
}

[[noreturn]] inline void ThrowError(ErrorCode code, const string &message) {
	throw cxx::Exception(static_cast<int>(code), message);
}

[[noreturn]] inline void ThrowInvalidInput(const string &message) {
	ThrowError(ErrorCode::INVALID_INPUT, message);
}

[[noreturn]] inline void ThrowNotImplemented(const string &message) {
	ThrowError(ErrorCode::NOT_IMPLEMENTED, message);
}

//! Throws the error avro-c reported for the last call that failed
[[noreturn]] void ThrowAvroError();

} // namespace duckdb_avro
