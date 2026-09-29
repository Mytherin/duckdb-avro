//===----------------------------------------------------------------------===//
//                         DuckDB
//
// avro_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "avro_common.hpp"
#include "avro_type.hpp"

#include <avro.h>

#include <memory>
#include <vector>

namespace duckdb_avro {

//! The contents of an Avro object container file, read into memory
class AvroFileBuffer {
public:
	AvroFileBuffer(cxx::Context &context, const string &path);

public:
	std::vector<char> data;
};

//! An Avro object container file opened for reading
class AvroFile {
public:
	AvroFile(cxx::Context &context, const string &path);
	~AvroFile();

	AvroFile(const AvroFile &) = delete;
	AvroFile &operator=(const AvroFile &) = delete;

public:
	string path;
	AvroFileBuffer buffer;
	avro_file_reader_t reader = nullptr;
	avro_value_iface_t *value_iface = nullptr;
	idx_t block_count = 0;
	//! The writer schema of the file, and the type it is read as
	string root_name;
	AvroType avro_type;
};

struct AvroReaderFunctions {
	//! read_single_avro_file(path) - reads a single Avro file
	static void RegisterSingleFileFunction(cxx::Extension &extension, cxx::Context &context);
	//! read_avro(...) - the multi-file reader on top of read_single_avro_file
	static void RegisterMultiFileFunction(cxx::Extension &extension, cxx::Context &context);
	//! avro_metadata(path) - the key-value metadata of an Avro file
	static void RegisterMetadataFunction(cxx::Extension &extension, cxx::Context &context);
};

} // namespace duckdb_avro
