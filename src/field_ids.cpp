#include "field_ids.hpp"

#include <algorithm>
#include <cctype>
#include <unordered_set>

namespace duckdb_avro {

constexpr const char *FieldID::DUCKDB_FIELD_ID;
constexpr const char *FieldID::DUCKDB_NULLABLE_ID;

static string Lower(string str) {
	std::transform(str.begin(), str.end(), str.begin(), [](unsigned char c) { return std::tolower(c); });
	return str;
}

[[noreturn]] static void ThrowBinderError(const string &message) {
	ThrowError(ErrorCode::BINDER, message);
}

std::map<string, FieldID> &ChildFieldIDs::Ids() {
	if (!ids) {
		ids = std::make_shared<std::map<string, FieldID>>();
	}
	return *ids;
}

FieldID::FieldID() : set(false) {
}

FieldID::FieldID(int32_t field_id_p, bool nullable) : set(true), field_id(field_id_p), nullable(nullable) {
}

int32_t FieldID::GetFieldId() const {
	return field_id;
}

//! The nested fields a FIELD_IDS entry can be given for, by lowercase name
using NameToTypeMap = std::map<string, cxx::LogicalType>;

static NameToTypeMap GetChildNameToTypeMap(const cxx::LogicalType &type) {
	NameToTypeMap name_to_type_map;
	switch (type.GetTypeId()) {
	case LogicalTypeId::LIST:
		name_to_type_map.emplace("list", type.GetListChildType());
		break;
	case LogicalTypeId::MAP:
		name_to_type_map.emplace("key", type.GetMapKeyType());
		name_to_type_map.emplace("value", type.GetMapValueType());
		break;
	case LogicalTypeId::STRUCT:
		for (idx_t i = 0; i < type.GetStructChildCount(); i++) {
			auto child_name = type.GetStructChildName(i);
			if (child_name == FieldID::DUCKDB_FIELD_ID) {
				ThrowBinderError(
				    FormatString("Cannot have column named \"%s\" with FIELD_IDS", FieldID::DUCKDB_FIELD_ID));
			}
			name_to_type_map.emplace(Lower(child_name), type.GetStructChildType(i));
		}
		break;
	default:
		ThrowError(ErrorCode::INTERNAL, "Unexpected type in GetChildNameToTypeMap");
	}
	return name_to_type_map;
}

static void GetFieldIDs(cxx::Context &context, const cxx::Value &field_ids_value, ChildFieldIDs &field_ids_p,
                        std::unordered_set<uint32_t> &unique_field_ids, const NameToTypeMap &name_to_type_map) {
	auto struct_type = field_ids_value.GetLogicalType();
	if (struct_type.GetTypeId() != LogicalTypeId::STRUCT) {
		ThrowBinderError(FormatString(
		    "Expected FIELD_IDS to be a STRUCT, e.g., {col1: 42, col2: {%s: 43, nested_col: 44}, col3: 44}",
		    FieldID::DUCKDB_FIELD_ID));
	}
	auto &field_ids = field_ids_p.Ids();
	for (idx_t i = 0; i < struct_type.GetStructChildCount(); i++) {
		const auto col_name = Lower(struct_type.GetStructChildName(i));
		if (col_name == FieldID::DUCKDB_FIELD_ID || col_name == FieldID::DUCKDB_NULLABLE_ID) {
			continue;
		}

		auto it = name_to_type_map.find(col_name);
		if (it == name_to_type_map.end()) {
			string names;
			for (const auto &name : name_to_type_map) {
				if (!names.empty()) {
					names += ", ";
				}
				names += name.first;
			}
			ThrowBinderError(FormatString(
			    "Column name \"%s\" specified in FIELD_IDS not found. Consider using WRITE_PARTITION_COLUMNS if this "
			    "column is a partition column. Available column names: [%s]",
			    col_name.c_str(), names.c_str()));
		}

		auto child_value = field_ids_value.GetChild(i);
		auto child_type = child_value.GetLogicalType();
		std::unique_ptr<cxx::Value> field_id_value;
		std::unique_ptr<cxx::Value> field_id_nullable;
		bool has_nested_field_ids = false;

		if (child_type.GetTypeId() == LogicalTypeId::STRUCT) {
			for (idx_t nested_i = 0; nested_i < child_type.GetStructChildCount(); nested_i++) {
				const auto field_id_or_nested_col = child_type.GetStructChildName(nested_i);
				if (field_id_or_nested_col == FieldID::DUCKDB_FIELD_ID) {
					field_id_value = std::make_unique<cxx::Value>(child_value.GetChild(nested_i));
				} else if (field_id_or_nested_col == FieldID::DUCKDB_NULLABLE_ID) {
					field_id_nullable = std::make_unique<cxx::Value>(child_value.GetChild(nested_i));
				} else {
					has_nested_field_ids = true;
				}
			}
		} else {
			field_id_value = std::make_unique<cxx::Value>(std::move(child_value));
		}

		FieldID field_id;
		if (field_id_value) {
			auto integer_type = context.CreateType(LogicalTypeId::INTEGER);
			const auto field_id_int = field_id_value->Cast(context, integer_type).Get<int32_t>();
			if (!unique_field_ids.insert(static_cast<uint32_t>(field_id_int)).second) {
				ThrowBinderError(FormatString("Duplicate field_id %d found in FIELD_IDS", field_id_int));
			}
			if (field_id_nullable) {
				auto boolean_type = context.CreateType(LogicalTypeId::BOOLEAN);
				field_id = FieldID(field_id_int, field_id_nullable->Cast(context, boolean_type).Get<bool>());
			} else {
				field_id = FieldID(field_id_int);
			}
		}
		auto inserted = field_ids.emplace(col_name, std::move(field_id));

		if (has_nested_field_ids) {
			const auto &col_type = it->second;
			auto col_type_id = col_type.GetTypeId();
			if (col_type_id != LogicalTypeId::LIST && col_type_id != LogicalTypeId::MAP &&
			    col_type_id != LogicalTypeId::STRUCT) {
				ThrowBinderError(FormatString("Column \"%s\" with type \"%s\" cannot have a nested FIELD_IDS "
				                              "specification",
				                              col_name.c_str(), string(col_type.GetName()).c_str()));
			}
			GetFieldIDs(context, field_ids_value.GetChild(i), inserted.first->second.children, unique_field_ids,
			            GetChildNameToTypeMap(col_type));
		}
	}
}

ChildFieldIDs FieldIDUtils::ParseFieldIds(cxx::Context &context, const cxx::Value &input,
                                          const std::vector<string> &names,
                                          const std::vector<cxx::LogicalType> &types) {
	std::unordered_set<uint32_t> unique_field_ids;
	NameToTypeMap name_to_type_map;
	for (idx_t col_idx = 0; col_idx < names.size(); col_idx++) {
		if (names[col_idx] == FieldID::DUCKDB_FIELD_ID) {
			ThrowBinderError(
			    FormatString("Cannot have a column named \"%s\" when writing FIELD_IDS", FieldID::DUCKDB_FIELD_ID));
		}
		name_to_type_map.emplace(Lower(names[col_idx]), types[col_idx].Copy());
	}

	ChildFieldIDs result;
	GetFieldIDs(context, input, result, unique_field_ids, name_to_type_map);
	return result;
}

} // namespace duckdb_avro
