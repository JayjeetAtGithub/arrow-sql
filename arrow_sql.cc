#include <chrono>
#include <cstdlib>
#include <iostream>

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"

#include <arrow/api.h>
#include "arrow/array.h"
#include "arrow/array/builder_primitive.h"
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/discovery.h>
#include <arrow/dataset/expression.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/file_rados_parquet.h>
#include <arrow/dataset/scanner.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/path_util.h>

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/arrow_wrapper.hpp"

#include <memory>

using std::chrono::high_resolution_clock;
using std::chrono::duration_cast;
using std::chrono::duration;
using std::chrono::milliseconds;

struct Configuration {
  size_t repeat = 1;
  arrow::dataset::InspectOptions inspect_options{};
  arrow::dataset::FinishOptions finish_options{};
} conf;

struct SimpleFactory {
	std::shared_ptr<arrow::TableBatchReader> reader;

	SimpleFactory(std::shared_ptr<arrow::TableBatchReader> reader) : reader(std::move(reader)) {}

	static std::unique_ptr<duckdb::ArrowArrayStreamWrapper> CreateStream(uintptr_t this_ptr) {
		auto &factory = *reinterpret_cast<SimpleFactory *>(this_ptr);
		auto stream_wrapper = duckdb::make_unique<duckdb::ArrowArrayStreamWrapper>();
		stream_wrapper->arrow_array_stream.release = nullptr;
		auto maybe_ok = arrow::ExportRecordBatchReader(factory.reader, &stream_wrapper->arrow_array_stream);
		if (!maybe_ok.ok()) {
			if (stream_wrapper->arrow_array_stream.release) {
				stream_wrapper->arrow_array_stream.release(&stream_wrapper->arrow_array_stream);
			}
			return nullptr;
		}
		return stream_wrapper;
	}
};

std::shared_ptr<arrow::dataset::RadosParquetFileFormat> GetFormat() {
  std::string ceph_config_path = "/etc/ceph/ceph.conf";
  std::string data_pool = "cephfs_data";
  std::string user_name = "client.admin";
  std::string cluster_name = "ceph";
  return std::make_shared<arrow::dataset::RadosParquetFileFormat>(
      ceph_config_path, data_pool, user_name, cluster_name);
}

std::shared_ptr<arrow::fs::FileSystem> GetFileSystemFromUri(const std::string& uri,
                                                     std::string* path) {
  return arrow::fs::FileSystemFromUri(uri, path).ValueOrDie();
}

std::shared_ptr<arrow::dataset::Dataset> GetDatasetFromDirectory(
    std::shared_ptr<arrow::fs::FileSystem> fs, std::shared_ptr<arrow::dataset::ParquetFileFormat> format,
    std::string dir) {

  arrow::fs::FileSelector s;
  s.base_dir = dir;
  s.recursive = true;
  arrow::dataset::FileSystemFactoryOptions options;
  auto factory = arrow::dataset::FileSystemDatasetFactory::Make(fs, s, format, options).ValueOrDie();
  auto schema = factory->Inspect(conf.inspect_options).ValueOrDie();
  auto child = factory->Finish(conf.finish_options).ValueOrDie();
  arrow::dataset::DatasetVector children{conf.repeat, child};
  auto dataset = arrow::dataset::UnionDataset::Make(std::move(schema), std::move(children));
  return dataset.ValueOrDie();
}

std::shared_ptr<arrow::dataset::Dataset> GetParquetDatasetFromMetadata(
    std::shared_ptr<arrow::fs::FileSystem> fs, std::shared_ptr<arrow::dataset::ParquetFileFormat> format,
    std::string metadata_path) {
  arrow::dataset::ParquetFactoryOptions options;
  auto factory =
      arrow::dataset::ParquetDatasetFactory::Make(metadata_path, fs, format, options).ValueOrDie();
  return factory->Finish().ValueOrDie();
}

std::shared_ptr<arrow::dataset::Dataset> GetDatasetFromFile(
    std::shared_ptr<arrow::fs::FileSystem> fs, std::shared_ptr<arrow::dataset::ParquetFileFormat> format,
    std::string file) {
  arrow::dataset::FileSystemFactoryOptions options;
  auto factory =
      arrow::dataset::FileSystemDatasetFactory::Make(fs, {file}, format, options).ValueOrDie();
  auto schema = factory->Inspect(conf.inspect_options).ValueOrDie();
  auto child = factory->Finish(conf.finish_options).ValueOrDie();
  arrow::dataset::DatasetVector children;
  children.resize(conf.repeat, child);
  auto dataset = arrow::dataset::UnionDataset::Make(std::move(schema), std::move(children));
  return dataset.ValueOrDie();
}

std::shared_ptr<arrow::dataset::Dataset> GetDatasetFromPath(
    std::shared_ptr<arrow::fs::FileSystem> fs, std::shared_ptr<arrow::dataset::ParquetFileFormat> format,
    std::string path) {
  auto info = fs->GetFileInfo(path).ValueOrDie();
  if (info.IsDirectory()) {
    return GetDatasetFromDirectory(fs, format, path);
  }
  auto dirname_basename = arrow::fs::internal::GetAbstractPathParent(path);
  auto basename = dirname_basename.second;
  if (basename == "_metadata") {
    return GetParquetDatasetFromMetadata(fs, format, path);
  }
  return GetDatasetFromFile(fs, format, path);
}

std::shared_ptr<arrow::Table> QueryDataset(std::shared_ptr<arrow::dataset::Dataset> dataset) {
  auto scanner_builder = dataset->NewScan().ValueOrDie();
  scanner_builder->UseThreads(true);
  scanner_builder->Filter(arrow::dataset::greater(arrow::dataset::field_ref("total_amount"), arrow::dataset::literal(27.0f)));
  auto scanner = scanner_builder->Finish().ValueOrDie();
  return scanner->ToTable().ValueOrDie();
}

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cout << "Usage: ./arrow_sql [format(pq/rpq)] [query] [file:///path/to/dataset]\n";
        exit(1);
    }
    std::string fmt = argv[1];
    std::string query = argv[2];
    std::cout << "Running Query: " << query << "\n";
    auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    if (fmt == "rpq") {
            format = GetFormat();
    }

    std::string path;
    auto fs = GetFileSystemFromUri(argv[3], &path);
    auto dataset = GetDatasetFromPath(fs, format, path);

    auto t1 = high_resolution_clock::now();    
    auto table = QueryDataset(dataset);
    auto table_reader = std::make_shared<arrow::TableBatchReader>(*table);
    SimpleFactory factory {table_reader};
    duckdb::DuckDB db;
    duckdb::Connection conn {db};
    duckdb::vector<duckdb::Value> params;
    params.push_back(duckdb::Value::POINTER((uintptr_t)&factory));
    params.push_back(duckdb::Value::POINTER((uintptr_t)&SimpleFactory::CreateStream));
    params.push_back(duckdb::Value::UBIGINT(5000000));
    auto relation = conn.TableFunction("arrow_scan", params)->CreateView("dataset", true, true);

    auto query_result = relation->Query(query);    
    auto t2 = high_resolution_clock::now();
    duration<double, std::milli> ms_double = t2 - t1;
    std::cout << ms_double.count() << "ms\n";

    std::cout << query_result->ToString() << "\n";
    
    return EXIT_SUCCESS;
}

/*
./arrow_sql pq 'SELECT sum(total_amount) FROM dataset' file:///mnt/cephfs/one
*/
