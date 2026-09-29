#include "duckdb_cpp_extension.hpp"

#include "avro_copy.hpp"
#include "avro_reader.hpp"

using namespace duckdb_avro;

DUCKDB_CPP_EXTENSION_ENTRYPOINT(duckdb::cxx::Extension &extension, duckdb::cxx::Context &context) {
	AvroReaderFunctions::RegisterSingleFileFunction(extension, context);
	AvroReaderFunctions::RegisterMultiFileFunction(extension, context);
	AvroReaderFunctions::RegisterMetadataFunction(extension, context);
	AvroCopyFunction::Register(extension);
}
