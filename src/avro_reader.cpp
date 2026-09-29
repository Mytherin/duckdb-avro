#include "avro_reader.hpp"

#include <algorithm>
#include <atomic>
#include <cstring>

namespace duckdb_avro {

static constexpr int64_t MICROS_PER_MSEC = 1000;

//===--------------------------------------------------------------------===//
// Avro File
//===--------------------------------------------------------------------===//
AvroFileBuffer::AvroFileBuffer(cxx::Context &context, const string &path) {
	auto fs = context.GetFileSystem();
	auto handle = fs.OpenFile(path, {cxx::FileFlags::READ, cxx::FileFlags::EXTERNAL_FILE_CACHE});
	data.resize(handle.Size());
	if (!data.empty()) {
		handle.ReadAt(data.data(), data.size(), 0);
	}
}

AvroFile::AvroFile(cxx::Context &context, const string &path_p) : path(path_p), buffer(context, path) {
	if (avro_file_reader_memory(buffer.data.data(), static_cast<int64_t>(buffer.data.size()), &reader)) {
		ThrowAvroError();
	}
	size_t file_block_count;
	if (avro_file_reader_get_block_count(reader, &file_block_count)) {
		ThrowAvroError();
	}
	block_count = file_block_count;

	auto avro_schema = avro_file_reader_get_writer_schema(reader);
	auto schema_name = avro_schema_name(avro_schema);
	root_name = schema_name ? schema_name : "avro_schema";
	try {
		avro_type = TransformAvroSchema(avro_schema);
	} catch (...) {
		avro_schema_decref(avro_schema);
		throw;
	}
	value_iface = avro_generic_class_from_schema(avro_schema);
	avro_schema_decref(avro_schema);
	if (!value_iface) {
		ThrowAvroError();
	}
}

AvroFile::~AvroFile() {
	if (value_iface) {
		avro_value_iface_decref(value_iface);
	}
	if (reader) {
		avro_file_reader_close(reader);
	}
}

//===--------------------------------------------------------------------===//
// Vector Writer
//===--------------------------------------------------------------------===//
//! Writes the decoded values of an Avro type into a vector. Pointers into the vector are cached, and forgotten
//! whenever a list the vector is (nested) inside of grows, as that can move the vector's memory
class AvroVectorWriter {
public:
	AvroVectorWriter(const AvroType &avro_type, std::shared_ptr<cxx::Vector> vector_p)
	    : type(avro_type), vector(std::move(vector_p)) {
		InitializeChildren();
	}
	AvroVectorWriter(const AvroType &avro_type, cxx::Vector vector_p)
	    : AvroVectorWriter(avro_type, std::make_shared<cxx::Vector>(std::move(vector_p))) {
	}

public:
	void Write(avro_value_t &value, idx_t row) {
		switch (type.duckdb_type) {
		case LogicalTypeId::SQLNULL:
			SetNull(row);
			break;
		case LogicalTypeId::BOOLEAN: {
			int bool_val;
			if (avro_value_get_boolean(&value, &bool_val)) {
				ThrowAvroError();
			}
			Data<bool>()[row] = bool_val != 0;
			break;
		}
		case LogicalTypeId::DATE:
		case LogicalTypeId::INTEGER: {
			if (avro_value_get_int(&value, &Data<int32_t>()[row])) {
				ThrowAvroError();
			}
			break;
		}
		case LogicalTypeId::TIME: {
			int64_t result;
			if (type.is_timestamp_millis) {
				// time-millis: stored as int32 (ms since midnight), scale to µs
				int32_t raw_val;
				if (avro_value_get_int(&value, &raw_val)) {
					ThrowAvroError();
				}
				result = static_cast<int64_t>(raw_val) * MICROS_PER_MSEC;
			} else if (avro_value_get_long(&value, &result)) {
				ThrowAvroError();
			}
			Data<int64_t>()[row] = result;
			break;
		}
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_TZ:
		case LogicalTypeId::TIMESTAMP_NS:
		case LogicalTypeId::BIGINT: {
			int64_t raw_val;
			if (avro_value_get_long(&value, &raw_val)) {
				ThrowAvroError();
			}
			Data<int64_t>()[row] = type.is_timestamp_millis ? MillisToMicros(raw_val) : raw_val;
			break;
		}
		case LogicalTypeId::FLOAT:
			if (avro_value_get_float(&value, &Data<float>()[row])) {
				ThrowAvroError();
			}
			break;
		case LogicalTypeId::DOUBLE:
			if (avro_value_get_double(&value, &Data<double>()[row])) {
				ThrowAvroError();
			}
			break;
		case LogicalTypeId::UUID: {
			size_t fixed_size = 16;
			const void *fixed_data;
			if (avro_value_get_fixed(&value, &fixed_data, &fixed_size)) {
				ThrowAvroError();
			}
			cxx::uuid_t::Decoded bytes;
			memcpy(bytes.bytes, fixed_data, sizeof(bytes.bytes));
			Data<cxx::uuid_t>()[row] = cxx::uuid_t::Encode(bytes);
			break;
		}
		case LogicalTypeId::DECIMAL:
			WriteDecimal(value, row);
			break;
		case LogicalTypeId::BLOB:
			WriteBlob(value, row);
			break;
		case LogicalTypeId::VARCHAR:
			WriteString(value, row);
			break;
		case LogicalTypeId::STRUCT:
			WriteStruct(value, row);
			break;
		case LogicalTypeId::UNION:
			WriteUnion(value, row);
			break;
		case LogicalTypeId::ENUM:
			WriteEnum(value, row);
			break;
		case LogicalTypeId::LIST:
		case LogicalTypeId::MAP:
			WriteList(value, row);
			break;
		default:
			ThrowNotImplemented(FormatString("Reading Avro values as type %d", static_cast<int>(type.duckdb_type)));
		}
	}

	void SetNull(idx_t row) {
		if (type.duckdb_type == LogicalTypeId::UNION && !tags) {
			// the union is read as its single non-null member (if it has one)
			for (auto &child : children) {
				if (child) {
					child->SetNull(row);
					return;
				}
			}
		}
		if (IsNested()) {
			vector->SetNull(row);
			return;
		}
		if (!validity) {
			validity = vector->GetValidityMutable().words;
		}
		cxx::ValidityMask {validity}.SetInvalid(row);
	}

	//! Sets the final size of every list written into
	void Finalize() {
		if (IsList()) {
			vector->SetListSize(list_size);
		}
		for (auto &child : children) {
			if (child) {
				child->Finalize();
			}
		}
	}

	//! Forgets the pointers into the vector and its children
	void Invalidate() {
		data = nullptr;
		validity = nullptr;
		tag_data = nullptr;
		heap.reset();
		for (auto &child : children) {
			if (child) {
				child->Invalidate();
			}
		}
	}

private:
	bool IsNested() const {
		switch (type.duckdb_type) {
		case LogicalTypeId::STRUCT:
		case LogicalTypeId::LIST:
		case LogicalTypeId::MAP:
		case LogicalTypeId::UNION:
		case LogicalTypeId::SQLNULL:
			return true;
		default:
			return false;
		}
	}
	bool IsList() const {
		return type.duckdb_type == LogicalTypeId::LIST || type.duckdb_type == LogicalTypeId::MAP;
	}
	//! Whether the union is read as a DuckDB UNION - rather than as its single non-null member
	bool IsRealUnion() const {
		idx_t member_count = 0;
		for (auto member : type.union_child_map) {
			if (member >= 0) {
				member_count++;
			}
		}
		return member_count > 1;
	}

	void AddChild(const AvroType &child_type, idx_t child_index) {
		children.push_back(std::make_unique<AvroVectorWriter>(child_type, vector->GetChild(child_index)));
	}

	void InitializeChildren() {
		switch (type.duckdb_type) {
		case LogicalTypeId::STRUCT:
			for (idx_t i = 0; i < type.children.size(); i++) {
				AddChild(type.children[i].second, i);
			}
			break;
		case LogicalTypeId::UNION: {
			bool real_union = IsRealUnion();
			for (idx_t i = 0; i < type.children.size(); i++) {
				auto member = type.union_child_map[i];
				if (member < 0) {
					children.push_back(nullptr);
				} else if (real_union) {
					// child 0 holds the tags, the members follow
					AddChild(type.children[i].second, static_cast<idx_t>(member) + 1);
				} else {
					// the union is read as this member - it writes into the same vector
					children.push_back(std::make_unique<AvroVectorWriter>(type.children[i].second, vector));
				}
			}
			if (real_union) {
				tags = std::make_unique<cxx::Vector>(vector->GetChild(0));
			}
			break;
		}
		case LogicalTypeId::LIST:
			AddChild(type.children[0].second, 0);
			break;
		case LogicalTypeId::MAP:
			if (type.avro_type == AVRO_ARRAY) {
				// an array of key-value records marked as a map - the keys and values are the fields of the records
				auto &entry_type = type.children[0].second;
				if (entry_type.duckdb_type != LogicalTypeId::STRUCT || entry_type.children.size() != 2) {
					ThrowNotImplemented("An Avro array marked as map must hold records of a key and a value");
				}
				AddChild(entry_type.children[0].second, 0);
				AddChild(entry_type.children[1].second, 1);
			} else {
				AddChild(type.children[0].second, 0);
				AddChild(type.children[1].second, 1);
			}
			break;
		default:
			break;
		}
	}

	template <class T>
	T *Data() {
		if (!data) {
			data = vector->GetDataMutable();
		}
		return static_cast<T *>(data);
	}

	cxx::Arena &Heap() {
		if (!heap) {
			heap = std::make_unique<cxx::Arena>(vector->GetHeap());
		}
		return *heap;
	}

	static int64_t MillisToMicros(int64_t millis) {
		int64_t result;
		if (__builtin_mul_overflow(millis, MICROS_PER_MSEC, &result)) {
			ThrowError(ErrorCode::OUT_OF_RANGE,
			           FormatString("Overflow in multiplication of INT64 (%lld * %lld)!",
			                        static_cast<long long>(millis), static_cast<long long>(MICROS_PER_MSEC)));
		}
		return result;
	}

	void WriteDecimal(avro_value_t &value, idx_t row) {
		// decimals should never be more than 16
		const uint8_t bytes_data[16] = {};
		const void *ptr = bytes_data;
		size_t bytes_size;
		avro_wrapped_buffer bytes_buf = AVRO_WRAPPED_BUFFER_EMPTY;

		if (type.avro_type == AVRO_BYTES) {
			if (avro_value_grab_bytes(&value, &bytes_buf)) {
				ThrowAvroError();
			}
			ptr = bytes_buf.buf;
			bytes_size = bytes_buf.size;
		} else { // AVRO_FIXED
			if (avro_value_get_fixed(&value, &ptr, &bytes_size)) {
				ThrowAvroError();
			}
		}

		auto raw = static_cast<const uint8_t *>(ptr);
		// Sign bit is in the MSB of the first byte
		bool negative = bytes_size > 0 && (raw[0] & 0x80);

		auto width = type.decimal_width;
		if (width <= 4) {
			uint16_t result = negative ? 0xFFFF : 0;
			for (idx_t i = 0; i < bytes_size; i++) {
				result = static_cast<uint16_t>((result << 8) | raw[i]);
			}
			Data<int16_t>()[row] = static_cast<int16_t>(result);
		} else if (width <= 9) {
			uint32_t result = negative ? 0xFFFFFFFF : 0;
			for (idx_t i = 0; i < bytes_size; i++) {
				result = (result << 8) | raw[i];
			}
			Data<int32_t>()[row] = static_cast<int32_t>(result);
		} else if (width <= 18) {
			uint64_t result = negative ? ~0ULL : 0;
			for (idx_t i = 0; i < bytes_size; i++) {
				result = (result << 8) | raw[i];
			}
			Data<int64_t>()[row] = static_cast<int64_t>(result);
		} else {
			Data<cxx::int128_t>()[row] = ReadHugeint(raw, bytes_size);
		}

		if (type.avro_type == AVRO_BYTES) {
			bytes_buf.free(&bytes_buf);
		}
	}

	static cxx::int128_t ReadHugeint(const uint8_t *raw, idx_t bytes_size) {
		if (bytes_size == 8) {
			int64_t lower_val_signed = 0;
			for (idx_t i = 0; i < bytes_size; i++) {
				int64_t raw_byte = raw[i];
				lower_val_signed |= (raw_byte << ((7 - i) * 8));
			}
			return cxx::int128_t {static_cast<uint64_t>(lower_val_signed), lower_val_signed < 0 ? -1 : 0};
		}
		int64_t upper_val = 0;
		uint64_t lower_val = 0;
		// Calculate how many bytes go into upper and lower parts
		idx_t upper_bytes = (bytes_size <= sizeof(uint64_t)) ? bytes_size : (bytes_size - sizeof(uint64_t));

		// Read upper part (big-endian)
		for (idx_t i = 0; i < upper_bytes; i++) {
			upper_val = (upper_val << 8) | raw[i];
		}
		// Handle sign extension for negative numbers
		if (bytes_size > 0 && (raw[0] & 0x80)) {
			// Fill remaining bytes with 1s for negative numbers
			if (upper_bytes < sizeof(int64_t)) {
				int64_t mask = ((int64_t)1 << ((sizeof(int64_t) - upper_bytes) * 8)) - 1;
				mask = mask << (upper_bytes * 8);
				upper_val |= mask;
			}
		}
		// Read lower part if there are remaining bytes
		if (bytes_size > sizeof(int64_t)) {
			for (idx_t i = upper_bytes; i < bytes_size; i++) {
				lower_val = (lower_val << 8) | raw[i];
			}
		}
		return cxx::int128_t {lower_val, upper_val};
	}

	void WriteBlob(avro_value_t &value, idx_t row) {
		switch (type.avro_type) {
		case AVRO_FIXED: {
			size_t fixed_size;
			const void *fixed_data;
			if (avro_value_get_fixed(&value, &fixed_data, &fixed_size)) {
				ThrowAvroError();
			}
			auto blob = Heap().AddBlob(std::string_view(static_cast<const char *>(fixed_data), fixed_size));
			Data<cxx::blob_t>()[row] = blob;
			break;
		}
		case AVRO_BYTES: {
			avro_wrapped_buffer blob_buf = AVRO_WRAPPED_BUFFER_EMPTY;
			if (avro_value_grab_bytes(&value, &blob_buf)) {
				ThrowAvroError();
			}
			auto blob = Heap().AddBlob(std::string_view(static_cast<const char *>(blob_buf.buf), blob_buf.size));
			blob_buf.free(&blob_buf);
			Data<cxx::blob_t>()[row] = blob;
			break;
		}
		default:
			ThrowNotImplemented("Unknown Avro blob type");
		}
	}

	void WriteString(avro_value_t &value, idx_t row) {
		avro_wrapped_buffer str_buf = AVRO_WRAPPED_BUFFER_EMPTY;
		if (avro_value_grab_string(&value, &str_buf)) {
			ThrowAvroError();
		}
		// avro strings are null-terminated
		std::string_view str(static_cast<const char *>(str_buf.buf), str_buf.size - 1);
		try {
			cxx::ValidateUTF8(str);
		} catch (cxx::Exception &) {
			str_buf.free(&str_buf);
			ThrowInvalidInput("Avro file contains invalid unicode string");
		}
		auto result = Heap().AddStringUnsafe(str);
		str_buf.free(&str_buf);
		Data<cxx::varchar_t>()[row] = result;
	}

	void WriteStruct(avro_value_t &value, idx_t row) {
		size_t child_count;
		if (avro_value_get_size(&value, &child_count)) {
			ThrowAvroError();
		}
		for (idx_t child_idx = 0; child_idx < child_count; child_idx++) {
			avro_value_t child_value;
			if (avro_value_get_by_index(&value, child_idx, &child_value, nullptr)) {
				ThrowAvroError();
			}
			children[child_idx]->Write(child_value, row);
		}
	}

	void WriteUnion(avro_value_t &value, idx_t row) {
		int discriminant = 0;
		avro_value_t union_value;
		if (avro_value_get_discriminant(&value, &discriminant) || avro_value_get_current_branch(&value, &union_value)) {
			ThrowAvroError();
		}
		if (discriminant < 0 || static_cast<size_t>(discriminant) >= type.children.size()) {
			ThrowInvalidInput("Invalid union tag");
		}
		auto member = type.union_child_map[discriminant];
		if (member < 0) {
			// the "null" branch
			SetNull(row);
			return;
		}
		auto &member_writer = *children[static_cast<idx_t>(discriminant)];
		if (!tags) {
			// the union is read as its single non-null member
			member_writer.Write(union_value, row);
			return;
		}
		if (!tag_data) {
			tag_data = static_cast<uint8_t *>(tags->GetDataMutable());
		}
		tag_data[row] = static_cast<uint8_t>(member);
		for (idx_t i = 0; i < children.size(); i++) {
			if (children[i] && static_cast<int>(i) != discriminant) {
				children[i]->SetNull(row);
			}
		}
		member_writer.Write(union_value, row);
	}

	void WriteEnum(avro_value_t &value, idx_t row) {
		int enum_val;
		if (avro_value_get_enum(&value, &enum_val)) {
			ThrowAvroError();
		}
		auto size = type.enum_values.size();
		if (enum_val < 0 || static_cast<idx_t>(enum_val) >= size) {
			ThrowInvalidInput("Enum value out of range");
		}
		// the dictionary index is stored in the smallest unsigned integer type that can hold every index
		if (size <= std::numeric_limits<uint8_t>::max()) {
			Data<uint8_t>()[row] = static_cast<uint8_t>(enum_val);
		} else if (size <= std::numeric_limits<uint16_t>::max()) {
			Data<uint16_t>()[row] = static_cast<uint16_t>(enum_val);
		} else {
			Data<uint32_t>()[row] = static_cast<uint32_t>(enum_val);
		}
	}

	//! Makes room for at least the given number of elements in the lists
	void ReserveList(idx_t required) {
		if (required <= list_capacity) {
			return;
		}
		auto new_capacity = list_capacity == 0 ? 64 : list_capacity;
		while (new_capacity < required) {
			new_capacity *= 2;
		}
		vector->SetListSize(new_capacity);
		list_capacity = new_capacity;
		// growing the elements can move them
		for (auto &child : children) {
			child->Invalidate();
		}
	}

	void WriteList(avro_value_t &value, idx_t row) {
		size_t list_len;
		if (avro_value_get_size(&value, &list_len)) {
			ThrowAvroError();
		}
		auto offset = list_size;
		ReserveList(offset + list_len);
		auto entries = Data<cxx::list_entry_t>();
		entries[row].offset = offset;
		entries[row].length = list_len;

		if (type.avro_type == AVRO_ARRAY && type.duckdb_type == LogicalTypeId::LIST) {
			auto &child_writer = *children[0];
			for (idx_t child_idx = 0; child_idx < list_len; child_idx++) {
				avro_value_t child_value;
				if (avro_value_get_by_index(&value, child_idx, &child_value, nullptr)) {
					ThrowAvroError();
				}
				child_writer.Write(child_value, offset + child_idx);
			}
		} else if (type.avro_type == AVRO_ARRAY) {
			// an array of key-value records marked as a map
			for (idx_t entry_idx = 0; entry_idx < list_len; entry_idx++) {
				avro_value_t entry_value;
				if (avro_value_get_by_index(&value, entry_idx, &entry_value, nullptr)) {
					ThrowAvroError();
				}
				for (idx_t i = 0; i < 2; i++) {
					avro_value_t field_value;
					if (avro_value_get_by_index(&entry_value, i, &field_value, nullptr)) {
						ThrowAvroError();
					}
					children[i]->Write(field_value, offset + entry_idx);
				}
			}
		} else {
			auto &key_writer = *children[0];
			auto &value_writer = *children[1];
			for (idx_t entry_idx = 0; entry_idx < list_len; entry_idx++) {
				avro_value_t child_value;
				const char *map_key;
				if (avro_value_get_by_index(&value, entry_idx, &child_value, &map_key)) {
					ThrowAvroError();
				}
				key_writer.WriteKey(map_key, offset + entry_idx);
				value_writer.Write(child_value, offset + entry_idx);
			}
		}
		list_size = offset + list_len;
	}

	void WriteKey(const char *key, idx_t row) {
		auto result = Heap().AddString(key);
		Data<cxx::varchar_t>()[row] = result;
	}

private:
	const AvroType &type;
	std::shared_ptr<cxx::Vector> vector;
	std::vector<std::unique_ptr<AvroVectorWriter>> children;
	//! The tags of a UNION
	std::unique_ptr<cxx::Vector> tags;

	void *data = nullptr;
	uint64_t *validity = nullptr;
	uint8_t *tag_data = nullptr;
	std::unique_ptr<cxx::Arena> heap;
	//! For lists: the number of elements written, and the number there is room for
	idx_t list_size = 0;
	idx_t list_capacity = 0;
};

//===--------------------------------------------------------------------===//
// read_single_avro_file
//===--------------------------------------------------------------------===//
struct AvroBindData {
	std::shared_ptr<AvroFile> file;
	//! Whether the columns are the fields of the root record, rather than the root itself
	bool root_is_record = false;
};

struct AvroGlobalState {
	std::atomic<idx_t> next_block {0};
};

struct AvroLocalState {
	explicit AvroLocalState(AvroFile &file) {
		if (avro_file_block_reader_create(file.reader, &block_reader)) {
			ThrowAvroError();
		}
		if (avro_generic_value_new(file.value_iface, &value)) {
			avro_file_block_reader_close(block_reader);
			ThrowAvroError();
		}
	}
	~AvroLocalState() {
		avro_value_decref(&value);
		avro_file_block_reader_close(block_reader);
	}
	AvroLocalState(const AvroLocalState &) = delete;
	AvroLocalState &operator=(const AvroLocalState &) = delete;

	avro_file_block_reader_t block_reader = nullptr;
	avro_value_t value;
	//! The block claimed for this thread, which is only read (and decompressed) once the scan starts on it
	idx_t block_index = 0;
	bool block_selected = false;
};

//! Attaches the field ids of a column and its nested fields to the result column
static void SetFieldIds(cxx::TableFunction::BindInput &input, idx_t column_index, std::vector<idx_t> &path,
                        const AvroColumnDefinition &column) {
	auto context = input.GetContext();
	if (column.field_id != AvroType::NO_FIELD_ID) {
		input.SetColumnIdentifier(column_index, path, cxx::Value::Create(context, column.field_id));
	}
	for (idx_t i = 0; i < column.children.size(); i++) {
		path.push_back(i);
		SetFieldIds(input, column_index, path, column.children[i]);
		path.pop_back();
	}
}

static void ReadAvroBind(cxx::TableFunction::BindInput &input) {
	auto context = input.GetContext();
	auto path = string(input.GetArgument(0).Get<cxx::varchar_t>().view());
	auto file = std::make_shared<AvroFile>(context, path);

	auto root = TransformAvroType(context, file->root_name, file->avro_type);
	std::vector<AvroColumnDefinition> columns;
	bool root_is_record = root.type.GetTypeId() == LogicalTypeId::STRUCT;
	if (root_is_record) {
		// the fields of the root record are the columns
		columns = std::move(root.children);
	} else {
		columns.push_back(std::move(root));
	}
	for (idx_t i = 0; i < columns.size(); i++) {
		input.AddResultColumn(columns[i].name, columns[i].type);
		std::vector<idx_t> child_path;
		SetFieldIds(input, i, child_path, columns[i]);
	}

	size_t metadata_count = 0;
	if (avro_file_reader_get_metadata_count(file->reader, &metadata_count)) {
		ThrowInvalidInput("Failed to get metadata count");
	}
	for (idx_t i = 0; i < metadata_count; i++) {
		const char *key = nullptr;
		const char *value = nullptr;
		size_t value_size = 0;
		if (avro_file_reader_get_metadata_by_index(file->reader, i, &key, &value, &value_size)) {
			ThrowInvalidInput(FormatString("Failed to get metadata at index %llu", static_cast<unsigned long long>(i)));
		}
		if (!key) {
			continue;
		}
		auto value_str = value ? string(value, value_size) : string();
		input.AddFileMetadata(key, cxx::Value::Create(context, cxx::varchar_t(value_str)));
	}

	input.SetBindData<AvroBindData>(AvroBindData {std::move(file), root_is_record});
}

static void ReadAvroInitGlobal(cxx::TableFunction::InitGlobalInput &input) {
	auto &bind_data = input.GetBindData<AvroBindData>();
	input.SetGlobalState<AvroGlobalState>();
	// every block of the file can be read by a thread of its own
	input.SetMaxThreads(std::max<idx_t>(bind_data.file->block_count, 1));
}

static void ReadAvroInitLocal(cxx::TableFunction::InitLocalInput &input) {
	auto &bind_data = input.GetBindData<AvroBindData>();
	input.SetLocalState<AvroLocalState>(*bind_data.file);
}

//! Every block of the file is a batch of its own. Batches are claimed while the scan hands out its work to the
//! threads, so the block is only reserved here - it is read by the thread that claimed it, in the exec callback
static void ReadAvroClaimBatch(cxx::TableFunction::ClaimBatchInput &input) {
	auto &bind_data = input.GetBindData<AvroBindData>();
	auto &global_state = input.GetGlobalState<AvroGlobalState>();
	auto &local_state = input.GetLocalState<AvroLocalState>();
	auto block_index = global_state.next_block++;
	if (block_index >= bind_data.file->block_count) {
		return;
	}
	local_state.block_index = block_index;
	local_state.block_selected = false;
	input.SetClaimed(true);
}

static void ReadAvroExec(cxx::TableFunction::ExecInput &input) {
	auto &bind_data = input.GetBindData<AvroBindData>();
	auto &local_state = input.GetLocalState<AvroLocalState>();
	auto &root_type = bind_data.file->avro_type;
	auto chunk = input.GetOutputChunk();
	if (!local_state.block_selected) {
		avro_value_reset(&local_state.value);
		if (avro_file_block_reader_select_block(local_state.block_reader, local_state.block_index)) {
			ThrowAvroError();
		}
		local_state.block_selected = true;
	}

	static const idx_t max_rows = cxx::StandardVectorSize();

	// set up a writer for every column the scan reads - the vectors are sized to hold as many rows as they can, and
	// sized down to the rows read at the end
	auto column_count = input.GetColumnCount();
	std::vector<idx_t> column_indexes;
	std::vector<std::unique_ptr<AvroVectorWriter>> writers;
	for (idx_t i = 0; i < column_count; i++) {
		auto column_index = input.GetColumnIndex(i);
		auto &column_type = bind_data.root_is_record ? root_type.children[column_index].second : root_type;
		column_indexes.push_back(column_index);
		auto vector = chunk.GetVector(i);
		vector.SetSize(max_rows);
		writers.push_back(std::make_unique<AvroVectorWriter>(column_type, std::move(vector)));
	}

	idx_t row = 0;
	int ret;
	while (row < max_rows &&
	       (ret = avro_file_block_reader_read_value(local_state.block_reader, &local_state.value)) == 0) {
		for (idx_t i = 0; i < writers.size(); i++) {
			if (!bind_data.root_is_record) {
				writers[i]->Write(local_state.value, row);
				continue;
			}
			avro_value_t column_value;
			if (avro_value_get_by_index(&local_state.value, column_indexes[i], &column_value, nullptr)) {
				ThrowAvroError();
			}
			writers[i]->Write(column_value, row);
		}
		row++;
	}
	if (row < max_rows && ret != EOF) {
		ThrowAvroError();
	}
	for (auto &writer : writers) {
		writer->Finalize();
	}
	if (column_count > 0) {
		chunk.GetVector(0).SetSize(row);
	}
}

void AvroReaderFunctions::RegisterSingleFileFunction(cxx::Extension &extension, cxx::Context &context) {
	auto function = cxx::TableFunction::Create(extension);
	function.SetName("read_single_avro_file");
	function.WithSignature([&](cxx::FunctionSignature &signature) {
		signature.AddParameter("path", context.CreateType(LogicalTypeId::VARCHAR));
	});
	function.SetBindCallback(ReadAvroBind);
	function.SetInitGlobalCallback(ReadAvroInitGlobal);
	function.SetInitLocalCallback(ReadAvroInitLocal);
	function.SetClaimBatchCallback(ReadAvroClaimBatch);
	function.SetExecCallback(ReadAvroExec);
	function.SetProjectionPushdown(true);
	function.Register();
}

void AvroReaderFunctions::RegisterMultiFileFunction(cxx::Extension &extension, cxx::Context &context) {
	auto function = cxx::MultiFileFunction::Create(extension);
	function.SetName("read_avro");
	function.SetSingleFileFunction("read_single_avro_file");
	function.SetReaderType("Avro");
	function.Register();
}

//===--------------------------------------------------------------------===//
// avro_metadata
//===--------------------------------------------------------------------===//
struct AvroMetadataBindData {
	string path;
};

struct AvroMetadataGlobalState {
	std::vector<std::pair<string, string>> entries;
	idx_t offset = 0;
};

static void AvroMetadataBind(cxx::TableFunction::BindInput &input) {
	auto context = input.GetContext();
	input.AddResultColumn("key", context.CreateType(LogicalTypeId::VARCHAR));
	input.AddResultColumn("value", context.CreateType(LogicalTypeId::VARCHAR));
	input.SetBindData<AvroMetadataBindData>(
	    AvroMetadataBindData {string(input.GetArgument(0).Get<cxx::varchar_t>().view())});
}

static void AvroMetadataInitGlobal(cxx::TableFunction::InitGlobalInput &input) {
	auto &bind_data = input.GetBindData<AvroMetadataBindData>();
	auto context = input.GetContext();
	AvroFileBuffer buffer(context, bind_data.path);

	auto reader = avro_reader_memory(buffer.data.data(), static_cast<int64_t>(buffer.data.size()));
	avro_file_reader_t file_reader;
	if (avro_reader_reader(reader, &file_reader)) {
		ThrowInvalidInput(FormatString("Failed to read Avro file: %s", avro_strerror()));
	}
	struct ReaderGuard {
		avro_file_reader_t reader;
		~ReaderGuard() {
			avro_file_reader_close(reader);
		}
	} guard {file_reader};

	size_t metadata_count = 0;
	if (avro_file_reader_get_metadata_count(file_reader, &metadata_count)) {
		ThrowInvalidInput("Failed to get metadata count");
	}
	AvroMetadataGlobalState result;
	for (idx_t i = 0; i < metadata_count; i++) {
		const char *key = nullptr;
		const char *value = nullptr;
		size_t value_size = 0;
		if (avro_file_reader_get_metadata_by_index(file_reader, i, &key, &value, &value_size)) {
			ThrowInvalidInput(FormatString("Failed to get metadata at index %llu", static_cast<unsigned long long>(i)));
		}
		result.entries.emplace_back(key ? string(key) : string(), value ? string(value, value_size) : string());
	}
	input.SetGlobalState<AvroMetadataGlobalState>(std::move(result));
}

static void AvroMetadataExec(cxx::TableFunction::ExecInput &input) {
	auto &state = input.GetGlobalState<AvroMetadataGlobalState>();
	auto chunk = input.GetOutputChunk();
	auto keys = chunk.GetVector(0);
	auto values = chunk.GetVector(1);
	static const idx_t max_rows = cxx::StandardVectorSize();
	idx_t count = 0;
	while (state.offset < state.entries.size() && count < max_rows) {
		auto &entry = state.entries[state.offset];
		keys.AssignString(count, entry.first);
		values.AssignString(count, entry.second);
		state.offset++;
		count++;
	}
	keys.SetSize(count);
}

void AvroReaderFunctions::RegisterMetadataFunction(cxx::Extension &extension, cxx::Context &context) {
	auto function = cxx::TableFunction::Create(extension);
	function.SetName("avro_metadata");
	function.WithSignature([&](cxx::FunctionSignature &signature) {
		signature.AddParameter("path", context.CreateType(LogicalTypeId::VARCHAR));
	});
	function.SetBindCallback(AvroMetadataBind);
	function.SetInitGlobalCallback(AvroMetadataInitGlobal);
	function.SetExecCallback(AvroMetadataExec);
	function.Register();
}

} // namespace duckdb_avro
