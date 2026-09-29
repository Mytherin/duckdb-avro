//===----------------------------------------------------------------------===//
//                         DuckDB
//
// avro_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "avro_common.hpp"

#include <avro.h>

#include <cstdint>
#include <limits>
#include <vector>

namespace duckdb_avro {

//! An Avro schema, together with the DuckDB type it is read as
struct AvroType {
public:
	AvroType() = default;
	AvroType(avro_type_t avro_type_p, LogicalTypeId duckdb_type_p)
	    : duckdb_type(duckdb_type_p), avro_type(avro_type_p) {
	}

public:
	bool HasFieldId() const {
		return field_id != NO_FIELD_ID;
	}
	int32_t GetFieldId() const {
		return field_id;
	}

public:
	static constexpr int32_t NO_FIELD_ID = std::numeric_limits<int32_t>::max();

	LogicalTypeId duckdb_type = LogicalTypeId::INVALID;
	avro_type_t avro_type = AVRO_NULL;
	//! The children of a record, array, map or union - by name
	std::vector<std::pair<string, AvroType>> children;
	//! For unions: the member of the DuckDB UNION every branch is read into, or -1 for the "null" branch
	std::vector<int64_t> union_child_map;
	int32_t field_id = NO_FIELD_ID;
	//! Whether the value is stored in milliseconds rather than microseconds (time-millis, timestamp-millis)
	bool is_timestamp_millis = false;
	//! For DECIMAL: the width and scale
	uint8_t decimal_width = 0;
	uint8_t decimal_scale = 0;
	//! For ENUM: the symbols
	std::vector<string> enum_values;
};

//! Converts an Avro schema into the AvroType it is read as
AvroType TransformAvroSchema(avro_schema_t avro_schema);

//! A column of the DuckDB schema an Avro schema is read as
struct AvroColumnDefinition {
	string name;
	cxx::LogicalType type;
	//! The Avro field id of the column, if it has one
	int32_t field_id = AvroType::NO_FIELD_ID;
	//! The nested fields of the column - addressed the way table functions address them (a STRUCT field by its
	//! index, the elements of a LIST by 0, the keys and values of a MAP by 0 and 1, a UNION member by its index)
	std::vector<AvroColumnDefinition> children;

	//! The storage type of the values of the column (e.g. the integer type a DECIMAL or ENUM is stored as)
	LogicalTypeId physical_type = LogicalTypeId::INVALID;
};

//! Converts an AvroType into the column it is read as. Unions with "null" lose that member, and unions of a
//! single other member are read as that member
AvroColumnDefinition TransformAvroType(cxx::Context &context, const string &name, const AvroType &avro_type);

} // namespace duckdb_avro
