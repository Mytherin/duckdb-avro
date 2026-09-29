#include "avro_type.hpp"

#include <cstring>
#include <unordered_set>

namespace duckdb_avro {

void ThrowAvroError() {
	ThrowInvalidInput(avro_strerror());
}

//===--------------------------------------------------------------------===//
// Avro schema -> AvroType
//===--------------------------------------------------------------------===//
static AvroType AvroLogicalTypeToAvroType(avro_schema_t avro_schema, avro_type_t avro_type) {
	auto logical_type_raw = avro_schema_logical_type(avro_schema);
	if (!logical_type_raw) {
		return AvroType();
	}
	// any nested types are handled in TransformSchema
	switch (avro_typeof(avro_schema)) {
	case AVRO_ARRAY:
	case AVRO_ENUM:
	case AVRO_MAP:
	case AVRO_RECORD:
		return AvroType();
	default:
		break;
	}
	string logical_type = logical_type_raw;
	if (logical_type == "date") {
		return AvroType(avro_type, LogicalTypeId::DATE);
	}
	if (logical_type == "decimal") {
		AvroType result(avro_type, LogicalTypeId::DECIMAL);
		result.decimal_width = static_cast<uint8_t>(avro_schema_precision(avro_schema));
		result.decimal_scale = static_cast<uint8_t>(avro_schema_scale(avro_schema));
		return result;
	}
	if (logical_type == "time-micros") {
		return AvroType(avro_type, LogicalTypeId::TIME);
	}
	if (logical_type == "timestamp-micros") {
		auto adjust_to_utc = avro_schema_adjust_to_utc(avro_schema);
		// -1 doesn't exist
		if (adjust_to_utc > 0) {
			return AvroType(avro_type, LogicalTypeId::TIMESTAMP_TZ);
		}
		return AvroType(avro_type, LogicalTypeId::TIMESTAMP);
	}
	if (logical_type == "timestamp-nanos") {
		auto adjust_to_utc = avro_schema_adjust_to_utc(avro_schema);
		if (adjust_to_utc > 0) {
			ThrowNotImplemented("Avro timestamp-nanos with adjust_to_utc not supported");
		}
		return AvroType(avro_type, LogicalTypeId::TIMESTAMP_NS);
	}
	if (logical_type == "uuid") {
		auto size = avro_schema_fixed_size(avro_schema);
		if (size != 16) {
			ThrowError(ErrorCode::INVALID_CONFIGURATION, "logical type is uuid, but size != 16");
		}
		return AvroType(avro_type, LogicalTypeId::UUID);
	}
	if (logical_type == "time-millis") {
		AvroType result(avro_type, LogicalTypeId::TIME);
		result.is_timestamp_millis = true;
		return result;
	}
	if (logical_type == "timestamp-millis") {
		auto adjust_to_utc = avro_schema_adjust_to_utc(avro_schema);
		AvroType result(avro_type, adjust_to_utc > 0 ? LogicalTypeId::TIMESTAMP_TZ : LogicalTypeId::TIMESTAMP);
		result.is_timestamp_millis = true;
		return result;
	}
	if (logical_type == "local-timestamp-millis") {
		AvroType result(avro_type, LogicalTypeId::TIMESTAMP);
		result.is_timestamp_millis = true;
		return result;
	}
	ThrowNotImplemented(FormatString("Unknown Avro logical type %s", logical_type.c_str()));
}

static AvroType TransformSchema(avro_schema_t avro_schema, std::unordered_set<string> parent_schema_names) {
	auto avro_type = avro_typeof(avro_schema);
	auto logical_type = AvroLogicalTypeToAvroType(avro_schema, avro_type);
	auto primitive = [&](LogicalTypeId type) {
		return logical_type.duckdb_type != LogicalTypeId::INVALID ? logical_type : AvroType(avro_type, type);
	};

	switch (avro_type) {
	case AVRO_NULL:
		return AvroType(AVRO_NULL, LogicalTypeId::SQLNULL);
	case AVRO_BOOLEAN:
		return AvroType(AVRO_BOOLEAN, LogicalTypeId::BOOLEAN);
	case AVRO_INT32:
		return primitive(LogicalTypeId::INTEGER);
	case AVRO_INT64:
		return primitive(LogicalTypeId::BIGINT);
	case AVRO_FLOAT:
		return primitive(LogicalTypeId::FLOAT);
	case AVRO_DOUBLE:
		return primitive(LogicalTypeId::DOUBLE);
	case AVRO_BYTES:
		return primitive(LogicalTypeId::BLOB);
	case AVRO_STRING:
		return primitive(LogicalTypeId::VARCHAR);
	case AVRO_FIXED:
		return primitive(LogicalTypeId::BLOB);
	case AVRO_UNION: {
		auto num_children = avro_schema_union_size(avro_schema);
		AvroType result(AVRO_UNION, LogicalTypeId::UNION);
		int64_t non_null_child_idx = 0;
		for (idx_t child_idx = 0; child_idx < num_children; child_idx++) {
			auto child_schema = avro_schema_union_branch(avro_schema, child_idx);
			auto child_type = TransformSchema(child_schema, parent_schema_names);
			bool is_null = child_type.duckdb_type == LogicalTypeId::SQLNULL;
			result.children.emplace_back(FormatString("u%llu", static_cast<unsigned long long>(child_idx)),
			                             std::move(child_type));
			result.union_child_map.push_back(is_null ? -1 : non_null_child_idx++);
		}
		return result;
	}
	case AVRO_RECORD: {
		auto schema_name = string(avro_schema_name(avro_schema));
		if (parent_schema_names.find(schema_name) != parent_schema_names.end()) {
			ThrowInvalidInput(FormatString("Recursive Avro types not supported: %s", schema_name.c_str()));
		}
		parent_schema_names.insert(schema_name);

		auto num_children = avro_schema_record_size(avro_schema);
		if (num_children == 0) {
			// this we just ignore but we need a marker so we don't get our offsets wrong
			return AvroType(AVRO_RECORD, LogicalTypeId::SQLNULL);
		}
		AvroType result(AVRO_RECORD, LogicalTypeId::STRUCT);
		for (idx_t child_idx = 0; child_idx < num_children; child_idx++) {
			auto child_schema = avro_schema_record_field_get_by_index(avro_schema, child_idx);
			auto child_type = TransformSchema(child_schema, parent_schema_names);
			child_type.field_id = avro_schema_record_field_id(avro_schema, child_idx);
			auto child_name = avro_schema_record_field_name(avro_schema, child_idx);
			if (!child_name || strlen(child_name) == 0) {
				ThrowInvalidInput("Empty avro field name");
			}
			result.children.emplace_back(child_name, std::move(child_type));
		}
		return result;
	}
	case AVRO_ENUM: {
		AvroType result(AVRO_ENUM, LogicalTypeId::ENUM);
		auto size = avro_schema_enum_number_of_symbols(avro_schema);
		for (idx_t enum_idx = 0; enum_idx < static_cast<idx_t>(size); enum_idx++) {
			result.enum_values.emplace_back(avro_schema_enum_get(avro_schema, enum_idx));
		}
		return result;
	}
	case AVRO_ARRAY: {
		auto child_schema = avro_schema_array_items(avro_schema);
		auto child_type = TransformSchema(child_schema, parent_schema_names);
		child_type.field_id = avro_schema_array_element_id(avro_schema);
		AvroType result(AVRO_ARRAY, avro_schema_array_is_map(avro_schema) ? LogicalTypeId::MAP : LogicalTypeId::LIST);
		result.children.emplace_back("list_entry", std::move(child_type));
		return result;
	}
	case AVRO_MAP: {
		auto child_schema = avro_schema_map_values(avro_schema);

		AvroType key_type(AVRO_STRING, LogicalTypeId::VARCHAR);
		key_type.field_id = avro_schema_map_key_id(avro_schema);
		auto value_type = TransformSchema(child_schema, parent_schema_names);
		value_type.field_id = avro_schema_map_value_id(avro_schema);

		AvroType result(AVRO_MAP, LogicalTypeId::MAP);
		result.children.emplace_back("key_entry", std::move(key_type));
		result.children.emplace_back("value_entry", std::move(value_type));
		return result;
	}
	case AVRO_LINK: {
		auto target = avro_schema_link_target(avro_schema);
		return TransformSchema(target, parent_schema_names);
	}
	default:
		ThrowNotImplemented(FormatString("Unknown Avro Type %s", avro_schema_type_name(avro_schema)));
	}
}

AvroType TransformAvroSchema(avro_schema_t avro_schema) {
	return TransformSchema(avro_schema, {});
}

//===--------------------------------------------------------------------===//
// AvroType -> DuckDB columns
//===--------------------------------------------------------------------===//
static cxx::Value TypeValue(cxx::Context &context, const cxx::LogicalType &type) {
	return cxx::Value::Create(context, type);
}

static AvroColumnDefinition MakeColumn(const string &name, cxx::LogicalType type,
                                       std::vector<AvroColumnDefinition> children = {}) {
	AvroColumnDefinition result {name, std::move(type), AvroType::NO_FIELD_ID, std::move(children)};
	switch (result.type.GetTypeId()) {
	case LogicalTypeId::DECIMAL:
		result.physical_type = result.type.GetDecimalInternalTypeId();
		break;
	case LogicalTypeId::ENUM:
		result.physical_type = result.type.GetEnumInternalTypeId();
		break;
	default:
		result.physical_type = result.type.GetTypeId();
		break;
	}
	return result;
}

static cxx::LogicalType CreateStructType(cxx::Context &context, const std::vector<AvroColumnDefinition> &children,
                                         LogicalTypeId type_id) {
	std::vector<cxx::TypeParam> params;
	for (auto &child : children) {
		params.emplace_back(child.name, TypeValue(context, child.type));
	}
	return context.CreateType(type_id, params);
}

static AvroColumnDefinition CreatePrimitiveColumn(cxx::Context &context, const string &name,
                                                  const AvroType &avro_type) {
	switch (avro_type.duckdb_type) {
	case LogicalTypeId::DECIMAL: {
		std::vector<cxx::TypeParam> params;
		params.emplace_back(cxx::Value::Create(context, avro_type.decimal_width));
		params.emplace_back(cxx::Value::Create(context, avro_type.decimal_scale));
		return MakeColumn(name, context.CreateType(LogicalTypeId::DECIMAL, params));
	}
	case LogicalTypeId::ENUM: {
		std::vector<cxx::TypeParam> params;
		for (auto &value : avro_type.enum_values) {
			params.emplace_back(cxx::Value::Create(context, cxx::varchar_t(value)));
		}
		return MakeColumn(name, context.CreateType(LogicalTypeId::ENUM, params));
	}
	case LogicalTypeId::SQLNULL:
		// the NULL type cannot be created from its id
		return MakeColumn(name, context.ParseType("\"NULL\""));
	default:
		return MakeColumn(name, context.CreateType(avro_type.duckdb_type));
	}
}

static AvroColumnDefinition TransformNestedType(cxx::Context &context, const string &name, const AvroType &avro_type) {
	std::vector<AvroColumnDefinition> children;
	auto id = avro_type.duckdb_type;
	switch (id) {
	case LogicalTypeId::STRUCT: {
		for (auto &child : avro_type.children) {
			children.push_back(TransformAvroType(context, child.first, child.second));
		}
		auto type = CreateStructType(context, children, LogicalTypeId::STRUCT);
		return MakeColumn(name, std::move(type), std::move(children));
	}
	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST: {
		std::vector<cxx::TypeParam> params;
		if (avro_type.avro_type == AVRO_ARRAY) {
			auto element = TransformAvroType(context, "list", avro_type.children[0].second);
			if (id == LogicalTypeId::LIST) {
				params.emplace_back(TypeValue(context, element.type));
				children.push_back(std::move(element));
				return MakeColumn(name, context.CreateType(LogicalTypeId::LIST, params), std::move(children));
			}
			// an array of key-value records that is marked as a map - the record holds the keys and values
			children = std::move(element.children);
		} else {
			children.push_back(TransformAvroType(context, "key", avro_type.children[0].second));
			children.push_back(TransformAvroType(context, "value", avro_type.children[1].second));
		}
		params.emplace_back(TypeValue(context, children[0].type));
		params.emplace_back(TypeValue(context, children[1].type));
		return MakeColumn(name, context.CreateType(LogicalTypeId::MAP, params), std::move(children));
	}
	case LogicalTypeId::UNION: {
		for (auto &child : avro_type.children) {
			if (child.second.duckdb_type == LogicalTypeId::SQLNULL) {
				continue;
			}
			children.push_back(TransformAvroType(context, child.first, child.second));
		}
		if (children.size() == 1) {
			// a union of a single member (besides "null") is read as that member
			auto result = std::move(children[0]);
			result.name = name;
			return result;
		}
		if (children.empty()) {
			if (avro_type.children.empty()) {
				ThrowInvalidInput("Empty union type");
			}
			return CreatePrimitiveColumn(context, name, AvroType(AVRO_NULL, LogicalTypeId::SQLNULL));
		}
		auto type = CreateStructType(context, children, LogicalTypeId::UNION);
		return MakeColumn(name, std::move(type), std::move(children));
	}
	default:
		return CreatePrimitiveColumn(context, name, avro_type);
	}
}

AvroColumnDefinition TransformAvroType(cxx::Context &context, const string &name, const AvroType &avro_type) {
	auto result = TransformNestedType(context, name, avro_type);
	if (avro_type.HasFieldId()) {
		result.field_id = avro_type.GetFieldId();
	}
	return result;
}

} // namespace duckdb_avro
