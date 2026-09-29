#include "avro_copy.hpp"

#include "field_ids.hpp"

#include <avro.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <map>
#include <memory>
#include <set>
#include <vector>

namespace duckdb_avro {

//===--------------------------------------------------------------------===//
// JSON
//===--------------------------------------------------------------------===//
//! A JSON value, written the way yyjson writes it (compact, keys in insertion order)
struct JSONValue {
public:
	enum class Kind : uint8_t { STRING, NUMBER, BOOLEAN, ARRAY, OBJECT };

	static JSONValue String(string value) {
		JSONValue result(Kind::STRING);
		result.text = std::move(value);
		return result;
	}
	static JSONValue Number(int64_t value) {
		JSONValue result(Kind::NUMBER);
		result.text = std::to_string(value);
		return result;
	}
	static JSONValue Boolean(bool value) {
		JSONValue result(Kind::BOOLEAN);
		result.text = value ? "true" : "false";
		return result;
	}
	static JSONValue Array() {
		return JSONValue(Kind::ARRAY);
	}
	static JSONValue Object() {
		return JSONValue(Kind::OBJECT);
	}

public:
	bool IsObject() const {
		return kind == Kind::OBJECT;
	}
	void Add(const string &key, JSONValue value) {
		fields.emplace_back(key, std::move(value));
	}
	void Append(JSONValue value) {
		items.push_back(std::move(value));
	}

	void Write(string &out) const {
		switch (kind) {
		case Kind::STRING:
			WriteString(out, text);
			break;
		case Kind::NUMBER:
		case Kind::BOOLEAN:
			out += text;
			break;
		case Kind::ARRAY:
			out += '[';
			for (idx_t i = 0; i < items.size(); i++) {
				if (i > 0) {
					out += ',';
				}
				items[i].Write(out);
			}
			out += ']';
			break;
		case Kind::OBJECT:
			out += '{';
			for (idx_t i = 0; i < fields.size(); i++) {
				if (i > 0) {
					out += ',';
				}
				WriteString(out, fields[i].first);
				out += ':';
				fields[i].second.Write(out);
			}
			out += '}';
			break;
		}
	}

	string ToString() const {
		string result;
		Write(result);
		return result;
	}

private:
	explicit JSONValue(Kind kind) : kind(kind) {
	}

	static void WriteString(string &out, const string &str) {
		out += '"';
		for (auto c : str) {
			switch (c) {
			case '"':
				out += "\\\"";
				break;
			case '\\':
				out += "\\\\";
				break;
			case '\b':
				out += "\\b";
				break;
			case '\f':
				out += "\\f";
				break;
			case '\n':
				out += "\\n";
				break;
			case '\r':
				out += "\\r";
				break;
			case '\t':
				out += "\\t";
				break;
			default:
				if (static_cast<unsigned char>(c) < 0x20) {
					out += FormatString("\\u%04x", static_cast<unsigned int>(static_cast<unsigned char>(c)));
				} else {
					out += c;
				}
				break;
			}
		}
		out += '"';
	}

private:
	Kind kind;
	string text;
	std::vector<JSONValue> items;
	std::vector<std::pair<string, JSONValue>> fields;
};

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//
static constexpr const char *EMPTY_STRUCT_MARKER = "__duckdb_empty_struct_marker";

[[noreturn]] static void ThrowCantConvert(const cxx::LogicalType &type) {
	ThrowNotImplemented(FormatString("Can't convert logical type '%s' to Avro type", type.ToText().c_str()));
}

static string ConvertTypeToAvro(const cxx::LogicalType &type) {
	switch (type.GetTypeId()) {
	case LogicalTypeId::VARCHAR:
		return "string";
	case LogicalTypeId::BLOB:
		return "bytes";
	case LogicalTypeId::INTEGER:
		return "int";
	case LogicalTypeId::BIGINT:
		return "long";
	case LogicalTypeId::FLOAT:
		return "float";
	case LogicalTypeId::DOUBLE:
		return "double";
	case LogicalTypeId::BOOLEAN:
		return "boolean";
	case LogicalTypeId::SQLNULL:
		return "null";
	case LogicalTypeId::STRUCT:
		return "record";
	case LogicalTypeId::DATE:
		return "int";
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_TZ:
		return "long";
	case LogicalTypeId::UUID:
	case LogicalTypeId::DECIMAL:
		return "fixed";
	case LogicalTypeId::LIST:
		return "array";
	case LogicalTypeId::MAP:
		//! This uses a 'logicalType': map, and a struct as 'items'
		return "array";
	default:
		//! FIXME: ENUM should be implemented at some point
		ThrowCantConvert(type);
	}
}

static bool IsTemporal(LogicalTypeId type_id) {
	switch (type_id) {
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
		return true;
	default:
		return false;
	}
}

static string GetTemporalLogicalType(const cxx::LogicalType &type) {
	switch (type.GetTypeId()) {
	case LogicalTypeId::DATE:
		return "date";
	case LogicalTypeId::TIME:
		return "time-micros";
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_TZ:
		return "timestamp-micros";
	case LogicalTypeId::TIMESTAMP_NS:
		return "timestamp-nanos";
	default:
		ThrowNotImplemented(
		    FormatString("Can't convert logical type '%s' to Avro temporal type", type.ToText().c_str()));
	}
}

uint32_t MinBytesRequiredForDecimal(int32_t precision) {
	// Number of bits needed: ceil(precision * log2(10)) + 1 (sign bit)
	static constexpr int32_t BITS_REQUIRED[] = {
	    0,  4,  7,  10, 14, 17, 20, 24, 27, 30, 34,  37,  40,  44,  47,  50,  54,  57,  60,  64,
	    67, 70, 74, 77, 80, 84, 87, 90, 94, 97, 100, 103, 107, 110, 113, 117, 120, 123, 127,
	};
	return static_cast<uint32_t>((BITS_REQUIRED[precision] + 7) / 8); // ceil(bits / 8)
}

static bool IsNamedSchema(LogicalTypeId type_id) {
	switch (type_id) {
	//! NOTE: 'fixed' is also part of this, but we don't have that type in DuckDB
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::ENUM:
	case LogicalTypeId::UUID:
	case LogicalTypeId::DECIMAL:
		return true;
	default:
		return false;
	}
}

//! The options of a COPY ... TO, by lowercase name
using CopyOptions = std::map<string, cxx::Value>;

static string Lower(string str) {
	std::transform(str.begin(), str.end(), str.begin(), [](unsigned char c) { return std::tolower(c); });
	return str;
}

//! A bare option (e.g. "CODEC" rather than "CODEC 'deflate'") is passed as the BOOLEAN true
static bool IsBareOption(const cxx::Value &value) {
	return !value.IsNull() && value.GetLogicalType().GetTypeId() == LogicalTypeId::BOOLEAN && value.Get<bool>();
}

struct JSONSchemaGenerator {
private:
	struct MapKeyValueIds {
		int64_t key_id;
		int64_t value_id;
	};

public:
	JSONSchemaGenerator(const std::vector<string> &names, const std::vector<cxx::LogicalType> &types)
	    : names(names), types(types) {
	}

public:
	void ParseFieldIds(cxx::Context &context, const CopyOptions &options, std::set<string> &recognized) {
		auto it = options.find("field_ids");
		if (it == options.end()) {
			return;
		}
		if (IsBareOption(it->second)) {
			ThrowInvalidInput("FIELD_IDS can not be provided without a value");
		}
		field_ids = FieldIDUtils::ParseFieldIds(context, it->second, names, types);
		recognized.insert(it->first);
	}
	void ParseRootName(const CopyOptions &options, std::set<string> &recognized) {
		auto it = options.find("root_name");
		if (it == options.end()) {
			return;
		}
		if (IsBareOption(it->second)) {
			ThrowInvalidInput("ROOT_NAME can not be provided without a value");
		}
		auto &value = it->second;
		if (value.GetLogicalType().GetTypeId() != LogicalTypeId::VARCHAR) {
			ThrowInvalidInput("'ROOT_NAME' is expected to be provided as VARCHAR, this is used for the name "
			                  "of the top level 'record'");
		}
		root_name = string(value.Get<cxx::varchar_t>().view());
		recognized.insert(it->first);
	}

public:
	void VerifyAvroName(const string &name) {
		for (idx_t i = 0; i < name.size(); i++) {
			auto c = static_cast<unsigned char>(name[i]);
			if (!(isalpha(c) || c == '_' || (i && isdigit(c)))) {
				ThrowInvalidInput(FormatString("'%s' is not a valid Avro identifier\nThe identifier has to match the "
				                               "regex: [A-Za-z_][A-Za-z0-9_]*",
				                               name.c_str()));
			}
		}
	}

	string GenerateSchemaName(const string &base) {
		auto res = base + std::to_string(generated_name_id++);
		VerifyAvroName(res);
		VerifyNamedSchemaUniqueness(res);
		return res;
	}

	JSONValue WrapTypeInObject(JSONValue type_val) {
		if (type_val.IsObject()) {
			return type_val;
		}
		auto object = JSONValue::Object();
		object.Add("type", std::move(type_val));
		return object;
	}

	JSONValue CreateJSONType(const cxx::LogicalType &type, const FieldID *field_id,
	                         const char *preset_schema_name = nullptr) {
		auto avro_type_str = ConvertTypeToAvro(type);
		auto type_val = JSONValue::String(avro_type_str);
		auto type_id = type.GetTypeId();

		if (type_id == LogicalTypeId::STRUCT) {
			type_val = WrapTypeInObject(std::move(type_val));
			auto fields = JSONValue::Array();
			for (idx_t i = 0; i < type.GetStructChildCount(); i++) {
				auto child_name = type.GetStructChildName(i);
				if (child_name == EMPTY_STRUCT_MARKER) {
					continue;
				}
				auto child_field_id = GetChildFieldIdByName(field_id, child_name);
				fields.Append(CreateStructField(child_name, type.GetStructChildType(i), child_field_id));
			}
			type_val.Add("fields", std::move(fields));
		} else if (type_id == LogicalTypeId::LIST) {
			type_val = WrapTypeInObject(std::move(type_val));

			auto list_child = type.GetListChildType();
			auto element_field_id = GetChildFieldIdByName(field_id, "list");
			JSONValue items_type_val = JSONValue::Object();
			if (list_child.GetTypeId() == LogicalTypeId::STRUCT && element_field_id) {
				auto list_schema_name = "r" + std::to_string(element_field_id->GetFieldId());
				items_type_val = CreateJSONType(list_child, element_field_id, list_schema_name.c_str());
			} else {
				items_type_val = CreateJSONType(list_child, element_field_id);
			}

			if (!element_field_id || element_field_id->nullable) {
				auto union_array = JSONValue::Array();
				union_array.Append(JSONValue::String("null"));
				union_array.Append(std::move(items_type_val));
				items_type_val = std::move(union_array);
			}
			type_val.Add("items", std::move(items_type_val));

			if (element_field_id) {
				type_val.Add("element-id", JSONValue::Number(element_field_id->GetFieldId()));
			}
		} else if (type_id == LogicalTypeId::MAP) {
			type_val = WrapTypeInObject(std::move(type_val));
			type_val.Add("logicalType", JSONValue::String("map"));

			// the entries of a MAP are written as records of the key and the value
			MapKeyValueIds key_value_ids;
			JSONValue map_items_val = JSONValue::Object();
			if (!GetMapKeyValueIds(field_id, key_value_ids)) {
				map_items_val = CreateMapEntryType(type, field_id);
			} else {
				auto map_schema_name =
				    "k" + std::to_string(key_value_ids.key_id) + "_v" + std::to_string(key_value_ids.value_id);
				map_items_val = CreateMapEntryType(type, field_id, map_schema_name.c_str());
			}
			type_val.Add("items", std::move(map_items_val));
		} else if (IsTemporal(type_id)) {
			type_val = WrapTypeInObject(std::move(type_val));
			type_val.Add("logicalType", JSONValue::String(GetTemporalLogicalType(type)));
			if (type_id == LogicalTypeId::TIMESTAMP_TZ) {
				type_val.Add("adjust-to-utc", JSONValue::Boolean(true));
			}
		} else if (type_id == LogicalTypeId::DECIMAL) {
			type_val = WrapTypeInObject(std::move(type_val));
			auto scale = type.GetDecimalScale();
			auto width = type.GetDecimalWidth();
			type_val.Add("logicalType", JSONValue::String("decimal"));
			type_val.Add("scale", JSONValue::Number(scale));
			type_val.Add("precision", JSONValue::Number(width));
			type_val.Add("size", JSONValue::Number(MinBytesRequiredForDecimal(width)));
		} else if (type_id == LogicalTypeId::UUID) {
			type_val = WrapTypeInObject(std::move(type_val));
			type_val.Add("logicalType", JSONValue::String("uuid"));
			type_val.Add("size", JSONValue::Number(16));
		}

		if (IsNamedSchema(type_id)) {
			if (preset_schema_name) {
				VerifyAvroName(preset_schema_name);
				VerifyNamedSchemaUniqueness(preset_schema_name);
				type_val.Add("name", JSONValue::String(preset_schema_name));
			} else {
				auto named_schema = GenerateSchemaName(avro_type_str);
				type_val.Add("name", JSONValue::String(named_schema));
			}
		}
		return type_val;
	}

	string GenerateJSON() {
		VerifyAvroName(root_name);
		VerifyNamedSchemaUniqueness(root_name);
		auto root_object = JSONValue::Object();
		root_object.Add("type", JSONValue::String("record"));
		root_object.Add("name", JSONValue::String(root_name));
		auto array = JSONValue::Array();
		//! Add all the fields
		for (idx_t i = 0; i < names.size(); i++) {
			auto &name = names[i];
			const FieldID *field_id = nullptr;
			auto &children = field_ids.Ids();
			auto it = children.find(Lower(name));
			if (it != children.end()) {
				field_id = &it->second;
			}
			array.Append(CreateStructField(name, types[i], field_id));
		}
		root_object.Add("fields", std::move(array));
		return root_object.ToString();
	}

private:
	void VerifyNamedSchemaUniqueness(const string &name) {
		auto res = named_schemas.insert(name);
		if (!res.second) {
			ThrowError(ErrorCode::BINDER,
			           FormatString("Avro schema by the name of '%s' already exists, names of 'record', 'enum' and "
			                        "'fixed' types have to be distinct",
			                        name.c_str()));
		}
	}

	//! The record the entries of a MAP are written as - the key and the value as its fields
	JSONValue CreateMapEntryType(const cxx::LogicalType &map_type, const FieldID *field_id,
	                             const char *preset_schema_name = nullptr) {
		auto type_val = JSONValue::Object();
		type_val.Add("type", JSONValue::String("record"));
		auto fields = JSONValue::Array();
		fields.Append(CreateStructField("key", map_type.GetMapKeyType(), GetChildFieldIdByName(field_id, "key")));
		fields.Append(CreateStructField("value", map_type.GetMapValueType(), GetChildFieldIdByName(field_id, "value")));
		type_val.Add("fields", std::move(fields));
		if (preset_schema_name) {
			VerifyAvroName(preset_schema_name);
			VerifyNamedSchemaUniqueness(preset_schema_name);
			type_val.Add("name", JSONValue::String(preset_schema_name));
		} else {
			type_val.Add("name", JSONValue::String(GenerateSchemaName("record")));
		}
		return type_val;
	}

	JSONValue CreateStructField(const string &name, const cxx::LogicalType &type, const FieldID *field_id) {
		auto struct_field = JSONValue::Object();
		auto schema_name = name;
		if (type.GetTypeId() == LogicalTypeId::STRUCT && field_id) {
			schema_name = "r" + std::to_string(field_id->GetFieldId());
		}
		auto struct_field_type = CreateJSONType(type, field_id, schema_name.c_str());
		if (!field_id || field_id->nullable) {
			auto union_array = JSONValue::Array();
			union_array.Append(JSONValue::String("null"));
			union_array.Append(std::move(struct_field_type));
			struct_field_type = std::move(union_array);
		}
		struct_field.Add("type", std::move(struct_field_type));
		if (field_id) {
			struct_field.Add("field-id", JSONValue::Number(field_id->GetFieldId()));
		}
		struct_field.Add("name", JSONValue::String(name));
		return struct_field;
	}

	const FieldID *GetChildFieldIdByName(const FieldID *parent, const string &name) {
		if (!parent || !parent->children.ids) {
			return nullptr;
		}
		auto &children = *parent->children.ids;
		auto it = children.find(Lower(name));
		if (it != children.end()) {
			return &it->second;
		}
		return nullptr;
	}

	bool GetMapKeyValueIds(const FieldID *field_id, MapKeyValueIds &ids) {
		if (!field_id) {
			return false;
		}
		auto key = GetChildFieldIdByName(field_id, "key");
		auto value = GetChildFieldIdByName(field_id, "value");
		if (key && value) {
			ids.key_id = key->GetFieldId();
			ids.value_id = value->GetFieldId();
			return true;
		}
		return false;
	}

public:
	const std::vector<string> &names;
	const std::vector<cxx::LogicalType> &types;

	string root_name = "root";
	ChildFieldIDs field_ids;
	idx_t generated_name_id = 0;
	std::set<string> named_schemas;
};

static string CreateJSONMetadata(const CopyOptions &options, std::set<string> &recognized) {
	auto it = options.find("metadata");
	if (it == options.end()) {
		return "";
	}
	auto &value = it->second;
	if (IsBareOption(value)) {
		ThrowInvalidInput("METADATA can not be provided without a value");
	}
	auto type = value.GetLogicalType();
	if (type.GetTypeId() != LogicalTypeId::STRUCT) {
		ThrowInvalidInput("'METADATA' is expected to be provided as a STRUCT of key-value string metadata");
	}
	recognized.insert(it->first);

	auto root_object = JSONValue::Object();
	std::set<string> keys;
	for (idx_t i = 0; i < type.GetStructChildCount(); i++) {
		auto key = type.GetStructChildName(i);
		if (!keys.insert(key).second) {
			continue;
		}
		root_object.Add(key, JSONValue::String(value.GetChild(i).ToText()));
	}
	return root_object.ToString();
}

//! Parse the CODEC option (Avro object-container compression codec). Validates the value is a non-empty VARCHAR and
//! returns the lowercased codec name; empty string when unset (writer defaults to "null"). The codec name itself is
//! validated by avro-c when the writer is created, so this stays in lock-step with avro-c's actual capabilities.
static string ParseCodec(const CopyOptions &options, std::set<string> &recognized) {
	auto it = options.find("codec");
	if (it == options.end()) {
		return "";
	}
	auto &value = it->second;
	if (IsBareOption(value)) {
		ThrowInvalidInput("CODEC can not be provided without a value");
	}
	if (value.GetLogicalType().GetTypeId() != LogicalTypeId::VARCHAR) {
		ThrowInvalidInput("'CODEC' is expected to be provided as VARCHAR (e.g. 'deflate', 'null')");
	}
	recognized.insert(it->first);
	return Lower(string(value.Get<cxx::varchar_t>().view()));
}

//===--------------------------------------------------------------------===//
// Bind
//===--------------------------------------------------------------------===//
struct WriteAvroBindData {
	WriteAvroBindData(cxx::CopyFunction::CopyToBindInput &input) {
		for (idx_t i = 0; i < input.GetColumnCount(); i++) {
			names.push_back(input.GetColumnName(i));
			types.push_back(input.GetColumnType(i));
		}

		CopyOptions options;
		for (idx_t i = 0; i < input.GetOptionCount(); i++) {
			options.emplace(Lower(input.GetOptionName(i)), input.GetOptionValue(i));
		}

		std::set<string> recognized;
		json_metadata = CreateJSONMetadata(options, recognized);
		JSONSchemaGenerator generator(names, types);
		auto context = input.GetContext();
		generator.ParseFieldIds(context, options, recognized);
		generator.ParseRootName(options, recognized);
		json_schema = generator.GenerateJSON();
		codec = ParseCodec(options, recognized);

		string unrecognized_options;
		for (auto &option : options) {
			if (recognized.count(option.first)) {
				continue;
			}
			if (!unrecognized_options.empty()) {
				unrecognized_options += ", ";
			}
			unrecognized_options += "key: \"" + option.first + "\"";
			if (!IsBareOption(option.second)) {
				unrecognized_options += " with value: '" + option.second.ToText() + "'";
			}
		}
		if (!unrecognized_options.empty()) {
			ThrowError(ErrorCode::INVALID_CONFIGURATION,
			           "The following option(s) are not recognized: " + unrecognized_options);
		}
		if (avro_schema_from_json_length(json_schema.c_str(), json_schema.size(), &schema)) {
			ThrowAvroError();
		}
		interface = avro_generic_class_from_schema(schema);
	}
	~WriteAvroBindData() {
		if (schema) {
			avro_schema_decref(schema);
		}
		if (interface) {
			avro_value_iface_decref(interface);
		}
	}
	WriteAvroBindData(const WriteAvroBindData &) = delete;
	WriteAvroBindData &operator=(const WriteAvroBindData &) = delete;

	bool operator==(const WriteAvroBindData &other) const {
		return json_schema == other.json_schema && json_metadata == other.json_metadata && codec == other.codec;
	}

public:
	std::vector<string> names;
	std::vector<cxx::LogicalType> types;

	//! The schema of the file to write
	avro_schema_t schema = nullptr;
	string json_schema;

	string json_metadata;
	//! Avro object-container compression codec ("null", "deflate", "snappy", "zstandard", ...). Empty means unset ->
	//! the writer defaults to "null" (uncompressed). Passed through to avro-c's codec so the COPY writer compresses
	//! natively (no post-processing).
	string codec;
	//! The interface through which new avro values are created
	avro_value_iface_t *interface = nullptr;
};

static void WriteAvroBind(cxx::CopyFunction::CopyToBindInput &input) {
	input.SetBindData<WriteAvroBindData>(input);
}

//===--------------------------------------------------------------------===//
// Column Writers
//===--------------------------------------------------------------------===//
class AvroColumnWriter {
public:
	virtual ~AvroColumnWriter() = default;

	virtual void Prepare(cxx::Vector &vector) = 0;
	virtual idx_t Write(avro_value_t *target, idx_t row) = 0;

protected:
	static idx_t WriteNull(avro_value_t *target, const cxx::LogicalType &type) {
		auto union_value = *target;
		avro_value_set_branch(&union_value, 0, target);
		auto schema_type = avro_value_get_type(target);
		if (schema_type != AVRO_NULL) {
			ThrowInvalidInput(
			    FormatString("Cannot insert NULL to non-nullable field of type %s", string(type.GetName()).c_str()));
		}
		avro_value_set_null(target);
		return 1;
	}

	static avro_value_t *GetNonNullTarget(avro_value_t *target) {
		auto union_value = *target;
		avro_value_set_branch(&union_value, 1, target);
		return target;
	}
};

template <class T>
static void WriteDecimalAsFixedBytes(T value, uint8_t *bytes, uint32_t bytes_needed) {
	auto type_bytes = static_cast<int>(sizeof(T));
	auto start = type_bytes - static_cast<int>(bytes_needed);
	for (int i = start; i < type_bytes; i++) {
		bytes[i - start] = static_cast<uint8_t>(value >> ((type_bytes - i - 1) * 8));
	}
}

static void WriteHugeintAsFixedBytes(const cxx::int128_t &value, uint8_t *bytes, uint32_t bytes_needed) {
	// byte i of the big-endian representation, counting from the least significant byte
	auto get_byte = [&](uint32_t i) -> uint8_t {
		if (i < 8) {
			return static_cast<uint8_t>(value.lower >> (i * 8));
		}
		return static_cast<uint8_t>(static_cast<uint64_t>(value.upper) >> ((i - 8) * 8));
	};
	for (uint32_t i = 0; i < bytes_needed; i++) {
		bytes[i] = get_byte(bytes_needed - i - 1);
	}
}

using PrimitiveWriteFunction = idx_t (*)(avro_value_t *target, const void *data, idx_t index,
                                         const cxx::LogicalType &type);

template <class T>
static const T &Element(const void *data, idx_t index) {
	return static_cast<const T *>(data)[index];
}

static idx_t WriteBooleanValue(avro_value_t *target, const void *data, idx_t index, const cxx::LogicalType &) {
	avro_value_set_boolean(target, Element<bool>(data, index));
	return sizeof(bool);
}

static idx_t WriteBlobValue(avro_value_t *target, const void *data, idx_t index, const cxx::LogicalType &) {
	auto &value = Element<cxx::blob_t>(data, index);
	avro_value_set_bytes(target, const_cast<char *>(value.data()), value.size());
	return value.size();
}

static idx_t WriteDoubleValue(avro_value_t *target, const void *data, idx_t index, const cxx::LogicalType &) {
	avro_value_set_double(target, Element<double>(data, index));
	return sizeof(double);
}

static idx_t WriteFloatValue(avro_value_t *target, const void *data, idx_t index, const cxx::LogicalType &) {
	avro_value_set_float(target, Element<float>(data, index));
	return sizeof(float);
}

static idx_t WriteIntegerValue(avro_value_t *target, const void *data, idx_t index, const cxx::LogicalType &) {
	avro_value_set_int(target, Element<int32_t>(data, index));
	return sizeof(int32_t);
}

static idx_t WriteBigIntValue(avro_value_t *target, const void *data, idx_t index, const cxx::LogicalType &) {
	avro_value_set_long(target, Element<int64_t>(data, index));
	return sizeof(int64_t);
}

static idx_t WriteStringValue(avro_value_t *target, const void *data, idx_t index, const cxx::LogicalType &) {
	auto &value = Element<cxx::varchar_t>(data, index);
	avro_value_set_string_len(target, value.data(), value.size());
	return value.size();
}

static idx_t WriteUUIDValue(avro_value_t *target, const void *data, idx_t index, const cxx::LogicalType &) {
	auto bytes = Element<cxx::uuid_t>(data, index).Decode();
	avro_value_set_fixed(target, bytes.bytes, sizeof(bytes.bytes));
	return sizeof(bytes.bytes);
}

template <class T>
static idx_t WriteDecimalValue(avro_value_t *target, const void *data, idx_t index, const cxx::LogicalType &type) {
	uint8_t bytes[16];
	auto bytes_needed = MinBytesRequiredForDecimal(type.GetDecimalWidth());
	WriteDecimalAsFixedBytes<T>(Element<T>(data, index), bytes, bytes_needed);
	avro_value_set_fixed(target, bytes, bytes_needed);
	return bytes_needed;
}

static idx_t WriteHugeintDecimalValue(avro_value_t *target, const void *data, idx_t index,
                                      const cxx::LogicalType &type) {
	uint8_t bytes[16];
	auto bytes_needed = MinBytesRequiredForDecimal(type.GetDecimalWidth());
	WriteHugeintAsFixedBytes(Element<cxx::int128_t>(data, index), bytes, bytes_needed);
	avro_value_set_fixed(target, bytes, bytes_needed);
	return bytes_needed;
}

class PrimitiveAvroColumnWriter : public AvroColumnWriter {
public:
	PrimitiveAvroColumnWriter(cxx::LogicalType type, PrimitiveWriteFunction write_function)
	    : type(std::move(type)), write_function(write_function) {
	}

	void Prepare(cxx::Vector &vector) override {
		view = vector.GetView();
	}

	idx_t Write(avro_value_t *target, idx_t row) override {
		auto index = view.SelAt(row);
		if (!view.RowIsValid(index)) {
			return WriteNull(target, type);
		}
		return write_function(GetNonNullTarget(target), view.data, index, type);
	}

private:
	cxx::LogicalType type;
	PrimitiveWriteFunction write_function;
	cxx::VectorView view {};
};

class NullAvroColumnWriter : public AvroColumnWriter {
public:
	explicit NullAvroColumnWriter(cxx::LogicalType type) : type(std::move(type)) {
	}

	void Prepare(cxx::Vector &) override {
	}

	idx_t Write(avro_value_t *target, idx_t) override {
		return WriteNull(target, type);
	}

private:
	cxx::LogicalType type;
};

class StructAvroColumnWriter : public AvroColumnWriter {
public:
	StructAvroColumnWriter(cxx::LogicalType type, std::vector<idx_t> child_indexes,
	                       std::vector<std::unique_ptr<AvroColumnWriter>> children)
	    : type(std::move(type)), child_indexes(std::move(child_indexes)), children(std::move(children)) {
	}

	void Prepare(cxx::Vector &vector) override {
		vector.Flatten();
		view = vector.GetView();
		child_vectors.clear();
		for (idx_t i = 0; i < children.size(); i++) {
			child_vectors.push_back(vector.GetChild(child_indexes[i]));
			children[i]->Prepare(child_vectors.back());
		}
	}

	idx_t Write(avro_value_t *target, idx_t row) override {
		if (!view.IsValid(row)) {
			return WriteNull(target, type);
		}
		auto *non_null_target = GetNonNullTarget(target);
		idx_t struct_value_size = 0;
		for (idx_t i = 0; i < children.size(); i++) {
			const char *unused_name;
			avro_value_t field;
			if (avro_value_get_by_index(non_null_target, i, &field, &unused_name)) {
				ThrowAvroError();
			}
			struct_value_size += children[i]->Write(&field, row);
		}
		return struct_value_size + 1;
	}

private:
	cxx::LogicalType type;
	std::vector<idx_t> child_indexes;
	std::vector<std::unique_ptr<AvroColumnWriter>> children;
	std::vector<cxx::Vector> child_vectors;
	cxx::VectorView view {};
};

class ListAvroColumnWriter : public AvroColumnWriter {
public:
	ListAvroColumnWriter(cxx::LogicalType type, std::vector<std::unique_ptr<AvroColumnWriter>> child_writers)
	    : type(std::move(type)), child_writers(std::move(child_writers)) {
	}

	void Prepare(cxx::Vector &vector) override {
		vector.Flatten();
		view = vector.GetView();
		child_vectors.clear();
		for (idx_t i = 0; i < child_writers.size(); i++) {
			child_vectors.push_back(vector.GetChild(i));
			child_writers[i]->Prepare(child_vectors.back());
		}
	}

	idx_t Write(avro_value_t *target, idx_t row) override {
		auto index = view.SelAt(row);
		if (!view.RowIsValid(index)) {
			return WriteNull(target, type);
		}

		auto *non_null_target = GetNonNullTarget(target);
		const auto &entry = view.Data<cxx::list_entry_t>()[index];
		idx_t list_value_size = 0;
		for (idx_t i = 0; i < entry.length; i++) {
			avro_value_t item;
			size_t unused_new_index;
			if (avro_value_append(non_null_target, &item, &unused_new_index)) {
				ThrowAvroError();
			}
			if (child_writers.size() == 1) {
				list_value_size += child_writers[0]->Write(&item, entry.offset + i);
				continue;
			}
			// the entry of a MAP is a record of the key and the value
			for (idx_t field_idx = 0; field_idx < child_writers.size(); field_idx++) {
				const char *unused_name;
				avro_value_t field;
				if (avro_value_get_by_index(&item, field_idx, &field, &unused_name)) {
					ThrowAvroError();
				}
				list_value_size += child_writers[field_idx]->Write(&field, entry.offset + i);
			}
			list_value_size++;
		}
		return list_value_size + 1;
	}

private:
	cxx::LogicalType type;
	std::vector<std::unique_ptr<AvroColumnWriter>> child_writers;
	std::vector<cxx::Vector> child_vectors;
	cxx::VectorView view {};
};

static std::unique_ptr<AvroColumnWriter> CreateAvroColumnWriter(const cxx::LogicalType &type) {
	auto primitive = [&](PrimitiveWriteFunction function) {
		return std::make_unique<PrimitiveAvroColumnWriter>(type.Copy(), function);
	};
	switch (type.GetTypeId()) {
	case LogicalTypeId::BOOLEAN:
		return primitive(WriteBooleanValue);
	case LogicalTypeId::BLOB:
		return primitive(WriteBlobValue);
	case LogicalTypeId::DOUBLE:
		return primitive(WriteDoubleValue);
	case LogicalTypeId::FLOAT:
		return primitive(WriteFloatValue);
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::DATE:
		return primitive(WriteIntegerValue);
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
		return primitive(WriteBigIntValue);
	case LogicalTypeId::VARCHAR:
		return primitive(WriteStringValue);
	case LogicalTypeId::UUID:
		return primitive(WriteUUIDValue);
	case LogicalTypeId::DECIMAL:
		switch (type.GetDecimalInternalTypeId()) {
		case LogicalTypeId::SMALLINT:
			return primitive(WriteDecimalValue<int16_t>);
		case LogicalTypeId::INTEGER:
			return primitive(WriteDecimalValue<int32_t>);
		case LogicalTypeId::BIGINT:
			return primitive(WriteDecimalValue<int64_t>);
		case LogicalTypeId::HUGEINT:
			return primitive(WriteHugeintDecimalValue);
		default:
			ThrowNotImplemented("Unsupported decimal physical type");
		}
	case LogicalTypeId::SQLNULL:
		return std::make_unique<NullAvroColumnWriter>(type.Copy());
	case LogicalTypeId::STRUCT: {
		std::vector<idx_t> child_indexes;
		std::vector<std::unique_ptr<AvroColumnWriter>> child_writers;
		for (idx_t i = 0; i < type.GetStructChildCount(); i++) {
			if (type.GetStructChildName(i) == EMPTY_STRUCT_MARKER) {
				continue;
			}
			child_indexes.push_back(i);
			child_writers.push_back(CreateAvroColumnWriter(type.GetStructChildType(i)));
		}
		return std::make_unique<StructAvroColumnWriter>(type.Copy(), std::move(child_indexes),
		                                                std::move(child_writers));
	}
	case LogicalTypeId::LIST: {
		std::vector<std::unique_ptr<AvroColumnWriter>> child_writers;
		child_writers.push_back(CreateAvroColumnWriter(type.GetListChildType()));
		return std::make_unique<ListAvroColumnWriter>(type.Copy(), std::move(child_writers));
	}
	case LogicalTypeId::MAP: {
		std::vector<std::unique_ptr<AvroColumnWriter>> child_writers;
		child_writers.push_back(CreateAvroColumnWriter(type.GetMapKeyType()));
		child_writers.push_back(CreateAvroColumnWriter(type.GetMapValueType()));
		return std::make_unique<ListAvroColumnWriter>(type.Copy(), std::move(child_writers));
	}
	case LogicalTypeId::ENUM:
		ThrowNotImplemented("Can't convert ENUM Value to Avro yet");
	default:
		ThrowNotImplemented(FormatString("PopulateValue not implemented for type %s", type.ToText().c_str()));
	}
}

//===--------------------------------------------------------------------===//
// Writing Files
//===--------------------------------------------------------------------===//
static idx_t NextPowerOfTwo(idx_t v) {
	idx_t result = 1;
	while (result < v) {
		result *= 2;
	}
	return result;
}

struct AvroInMemoryBuffer {
public:
	void Resize(idx_t new_capacity) {
		data.reset(new char[new_capacity]);
		capacity = new_capacity;
	}
	void ResizeAndCopy(idx_t new_capacity) {
		std::unique_ptr<char[]> new_data(new char[new_capacity]);
		if (capacity) {
			memcpy(new_data.get(), data.get(), capacity);
		}
		data = std::move(new_data);
		capacity = new_capacity;
	}
	char *GetData() {
		return data.get();
	}
	idx_t GetCapacity() const {
		return capacity;
	}

private:
	std::unique_ptr<char[]> data;
	idx_t capacity = 0;
};

//! The state of an Avro file being written
struct WriteAvroGlobalState {
	static constexpr idx_t BUFFER_SIZE = 1024;
	//! Size of the bytes written to mark the end of a section (header/datablock)
	static constexpr idx_t SYNC_SIZE = 16;
	//! Avro uses a handrolled varint that they assert can only be 10 bytes
	static constexpr idx_t MAX_ROW_COUNT_BYTES = 10;

public:
	WriteAvroGlobalState(cxx::Context &context, const WriteAvroBindData &bind_data, const string &file_path)
	    : handle(
	          context.GetFileSystem().OpenFile(file_path, {cxx::FileFlags::WRITE, cxx::FileFlags::FILE_CREATE_NEW})) {
		//! Guess how big the "header" of the Avro file needs to be
		idx_t capacity = std::max<idx_t>(
		    BUFFER_SIZE, NextPowerOfTwo(bind_data.json_schema.size() + SYNC_SIZE + MAX_ROW_COUNT_BYTES));
		memory_buffer.Resize(capacity);

		int ret;
		writer = avro_writer_memory(memory_buffer.GetData(), static_cast<int64_t>(memory_buffer.GetCapacity()));
		datum_writer = avro_writer_memory(datum_buffer.GetData(), static_cast<int64_t>(datum_buffer.GetCapacity()));

		const char *json_metadata = bind_data.json_metadata.empty() ? nullptr : bind_data.json_metadata.c_str();
		//! Pass the compression codec straight to avro-c so the object container is written compressed natively.
		//! Empty -> nullptr -> avro-c default ("null"/uncompressed).
		const char *codec = bind_data.codec.empty() ? nullptr : bind_data.codec.c_str();

		while ((ret = avro_file_writer_create_from_writers_with_metadata_and_codec(
		            writer, datum_writer, bind_data.schema, &file_writer, json_metadata, codec)) == ENOSPC) {
			auto current_capacity = memory_buffer.GetCapacity();
			memory_buffer.Resize(NextPowerOfTwo(current_capacity * 2));
			// re-initialize writer to use correct data location
			avro_file_writer_close(file_writer);
			writer = avro_writer_memory(memory_buffer.GetData(), static_cast<int64_t>(memory_buffer.GetCapacity()));
			datum_writer = avro_writer_memory(datum_buffer.GetData(), static_cast<int64_t>(datum_buffer.GetCapacity()));
		}
		if (ret) {
			ThrowAvroError();
		}
		file_writer_created = true;

		auto written_bytes = avro_writer_tell(writer);
		WriteData(memory_buffer.GetData(), static_cast<idx_t>(written_bytes));
		avro_writer_memory_set_dest(writer, memory_buffer.GetData(), static_cast<int64_t>(memory_buffer.GetCapacity()));

		if (avro_generic_value_new(bind_data.interface, &value)) {
			ThrowAvroError();
		}
		value_created = true;
		for (auto &type : bind_data.types) {
			column_writers.push_back(CreateAvroColumnWriter(type));
		}
	}
	~WriteAvroGlobalState() {
		if (value_created) {
			avro_value_decref(&value);
		}
		//! NOTE: the 'writer' and 'datum_writer' do not need to be closed, they are owned by the file_writer
		if (file_writer_created) {
			avro_file_writer_close(file_writer);
		}
	}
	WriteAvroGlobalState(const WriteAvroGlobalState &) = delete;
	WriteAvroGlobalState &operator=(const WriteAvroGlobalState &) = delete;

public:
	void WriteData(const char *data, idx_t size) {
		handle.Write(data, size);
		bytes_written += size;
	}

	//! Writes the rows of a chunk as a block of the file
	void WriteChunk(cxx::DataChunk &chunk) {
		idx_t count = chunk.GetRowCount();
		std::vector<cxx::Vector> vectors;
		for (idx_t col_idx = 0; col_idx < chunk.GetVectorCount(); col_idx++) {
			vectors.push_back(chunk.GetVector(col_idx));
			column_writers[col_idx]->Prepare(vectors.back());
		}

		idx_t offset_in_datum_buffer = 0;
		for (idx_t i = 0; i < count; i++) {
			//! Populate our avro value, estimating the size of the value as we go
			idx_t value_size = 0;
			for (idx_t col_idx = 0; col_idx < vectors.size(); col_idx++) {
				const char *unused_name;
				avro_value_t column;
				if (avro_value_get_by_index(&value, col_idx, &column, &unused_name)) {
					ThrowAvroError();
				}
				value_size += column_writers[col_idx]->Write(&column, i);
			}

			//! Prepare the datum buffer for this row
			idx_t length = datum_buffer.GetCapacity() - offset_in_datum_buffer;
			if (value_size > length) {
				//! This value is too big to fit into the remaining portion of the buffer
				idx_t new_capacity = datum_buffer.GetCapacity();
				new_capacity += value_size;
				datum_buffer.ResizeAndCopy(NextPowerOfTwo(new_capacity));
			}
			avro_writer_memory_set_dest_with_offset(datum_writer, datum_buffer.GetData(),
			                                        static_cast<int64_t>(datum_buffer.GetCapacity()),
			                                        static_cast<int64_t>(offset_in_datum_buffer));

			int ret;
			while ((ret = avro_file_writer_append_value(file_writer, &value)) == ENOSPC) {
				auto current_capacity = datum_buffer.GetCapacity();
				datum_buffer.ResizeAndCopy(NextPowerOfTwo(current_capacity * 2));
				avro_writer_memory_set_dest_with_offset(datum_writer, datum_buffer.GetData(),
				                                        static_cast<int64_t>(datum_buffer.GetCapacity()),
				                                        static_cast<int64_t>(offset_in_datum_buffer));
			}
			if (ret) {
				ThrowAvroError();
			}

			offset_in_datum_buffer = static_cast<idx_t>(avro_writer_tell(datum_writer));
			avro_value_reset(&value);
		}

		auto &buffer = memory_buffer;
		auto expected_size = static_cast<idx_t>(avro_writer_tell(datum_writer));
		expected_size += SYNC_SIZE + MAX_ROW_COUNT_BYTES;
		if (expected_size > buffer.GetCapacity()) {
			//! Resize the buffer in advance, to prevent any need for resizing below
			buffer.Resize(NextPowerOfTwo(expected_size));
			avro_writer_memory_set_dest(writer, buffer.GetData(), static_cast<int64_t>(buffer.GetCapacity()));
		}

		//! Flush the contents to the buffer, if it fails, resize the buffer and try again
		int ret;
		while ((ret = avro_file_writer_flush(file_writer)) == ENOSPC) {
			auto current_capacity = buffer.GetCapacity();
			buffer.Resize(NextPowerOfTwo(current_capacity * 2));
			avro_writer_memory_set_dest(writer, buffer.GetData(), static_cast<int64_t>(buffer.GetCapacity()));
		}
		if (ret) {
			ThrowAvroError();
		}

		auto written_bytes = avro_writer_tell(writer);
		WriteData(buffer.GetData(), static_cast<idx_t>(written_bytes));
		avro_writer_memory_set_dest(writer, buffer.GetData(), static_cast<int64_t>(buffer.GetCapacity()));
		row_count += count;
	}

public:
	cxx::FileHandle handle;
	AvroInMemoryBuffer memory_buffer;
	AvroInMemoryBuffer datum_buffer;

	//! The writer for the file
	avro_writer_t writer;
	avro_writer_t datum_writer;
	avro_file_writer_t file_writer;
	bool file_writer_created = false;

	//! Avro value representing a row of the schema, and the writers that fill it
	avro_value_t value {};
	bool value_created = false;
	std::vector<std::unique_ptr<AvroColumnWriter>> column_writers;

	//! Running total of bytes written to the file, reported as the file size - so the size is known without
	//! re-stat-ing the file, which would be a network round trip on object stores
	idx_t bytes_written = 0;
	//! Number of rows written
	idx_t row_count = 0;
};

//! A batch of rows to write, taken from the batch callback until the flush callback writes it
struct WriteAvroBatch {
	explicit WriteAvroBatch(cxx::ColumnDataCollection collection_p) : collection(std::move(collection_p)) {
	}

	cxx::ColumnDataCollection collection;
};

static void WriteAvroInit(cxx::CopyFunction::CopyToInitInput &input) {
	auto context = input.GetContext();
	auto &bind_data = input.GetBindData<WriteAvroBindData>();
	input.SetInitData<WriteAvroGlobalState>(context, bind_data, input.GetFilePath());
}

static void WriteAvroPrepareBatch(cxx::CopyFunction::CopyToBatchInput &input) {
	input.SetBatchData<WriteAvroBatch>(input.TakeBatch());
}

static void WriteAvroFlushBatch(cxx::CopyFunction::CopyToFlushInput &input) {
	auto context = input.GetContext();
	auto &bind_data = input.GetBindData<WriteAvroBindData>();
	auto &global_state = input.GetInitData<WriteAvroGlobalState>();
	auto &batch = input.GetBatchData<WriteAvroBatch>();

	//! every chunk is written as a block of its own
	cxx::DataChunk chunk(context, bind_data.types);
	auto shared_state = batch.collection.CreateSharedScanState();
	auto worker_state = batch.collection.CreateWorkerScanState();
	while (batch.collection.Scan(shared_state, worker_state, chunk)) {
		if (chunk.GetRowCount() == 0) {
			break;
		}
		global_state.WriteChunk(chunk);
	}
}

static void WriteAvroFinalize(cxx::CopyFunction::CopyToFinalizeInput &input) {
	auto &global_state = input.GetInitData<WriteAvroGlobalState>();
	//! Every block has already been written to the file handle - close it, which is required for stores that commit
	//! on Close() (e.g. Azure DFS / OneLake)
	global_state.handle.Close();
}

static void WriteAvroStatistics(cxx::CopyFunction::CopyToStatisticsInput &input) {
	auto &global_state = input.GetInitData<WriteAvroGlobalState>();
	input.SetRowCount(global_state.row_count);
	input.SetFileSize(global_state.bytes_written);
}

void AvroCopyFunction::Register(cxx::Extension &extension) {
	auto function = cxx::CopyFunction::Create(extension);
	function.SetName("avro");
	function.SetCopyToBindCallback(WriteAvroBind);
	function.SetCopyToInitCallback(WriteAvroInit);
	function.SetCopyToBatchCallback(WriteAvroPrepareBatch);
	function.SetCopyToFlushCallback(WriteAvroFlushBatch);
	function.SetCopyToFinalizeCallback(WriteAvroFinalize);
	function.SetCopyToStatisticsCallback(WriteAvroStatistics);
	function.Register();
}

} // namespace duckdb_avro
