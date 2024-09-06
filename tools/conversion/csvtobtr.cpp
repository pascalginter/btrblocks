// ------------------------------------------------------------------------------
// This program takes a csv file and its yaml schema and converts it to
// btrblocks files
//
// Example call: ./csvtobtr -create_btr
//                          -csv pbi/Arade/Arade_1.csv
//                          -yaml pbi/Arade/Arade_1.yaml
// ------------------------------------------------------------------------------
// Standard libs
#include <filesystem>
#include <fstream>
// ------------------------------------------------------------------------------
// External libs
#include <gflags/gflags.h>
#include <yaml-cpp/yaml.h>
#include <spdlog/spdlog.h>
#include <tbb/task_scheduler_init.h>
// ------------------------------------------------------------------------------
// Btr internal includes
#include "common/Utils.hpp"
#include "storage/Relation.hpp"
#include "scheme/SchemePool.hpp"
#include "compression/Datablock.hpp"
#include "compression/BtrReader.hpp"
#include "cache/ThreadCache.hpp"
// ------------------------------------------------------------------------------
// Btrfiles include
#include "btrfiles.hpp"
// ------------------------------------------------------------------------------
// Define command line flags
// TODO make the required flags mandatory/positional
DEFINE_string(btr, "btr", "Directory for btr output");
DEFINE_string(binary, "binary", "Directory for binary output");
DEFINE_string(yaml, "schema.yaml", "Schema in YAML format");
DEFINE_string(csv, "data.csv", "Original data in CSV format");
DEFINE_string(stats, "stats.txt", "File where stats are being stored");
DEFINE_string(compressionout,"compressionout.txt", "File where compressin times are being stored");
DEFINE_string(typefilter, "", "Only include columns with the selected type");
DEFINE_bool(create_binary, false, "Set if binary files are supposed to be created");
DEFINE_bool(create_btr, false, "If false will exit after binary creation");
DEFINE_bool(verify, true, "Verify that decompression works");
DEFINE_int32(chunk, -1, "Select a specific chunk to measure");
DEFINE_int32(column, -1, "Select a specific column to measure");
DEFINE_uint32(threads, 8, "");
// ------------------------------------------------------------------------------
using namespace btrblocks;
// ------------------------------------------------------------------------------

// ------------------------------------------------------------------------------
int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    std::string binary_path = FLAGS_binary + "/";
    // This seems necessary to be
    SchemePool::refresh();

    // Init TBB TODO: is that actually still necessary ?
    tbb::task_scheduler_init init(FLAGS_threads);

    // Load schema
    const auto schema = YAML::LoadFile(FLAGS_yaml);

    uint64_t binary_creation_time = 0;
    if (FLAGS_create_binary) {
        spdlog::info("Creating binary files in " + FLAGS_binary);
        // Load and parse CSV
        auto start_time = std::chrono::steady_clock::now();
        std::ifstream csv(FLAGS_csv);
        if (!csv.good()) {
            throw Generic_Exception("Unable to open specified csv file");
        }
        // parse writes the binary files
        files::convertCSV(FLAGS_csv, schema, FLAGS_binary);
        auto end_time = std::chrono::steady_clock::now();
        binary_creation_time = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
    }

    if (!FLAGS_create_btr) {
        return 0;
    }

    spdlog::info("Creating btr files in " + FLAGS_btr);


    ColumnType typefilter;
    if (FLAGS_typefilter.empty()) {
        typefilter = ColumnType::UNDEFINED;
    } else if (FLAGS_typefilter == "integer") {
        typefilter = ColumnType::INTEGER;
    } else if (FLAGS_typefilter == "double") {
        typefilter = ColumnType::DOUBLE;
    } else if (FLAGS_typefilter == "string") {
        typefilter = ColumnType::STRING;
    } else {
        throw std::runtime_error("typefilter must be one of [integer, double, string]");
    }

    if (typefilter != ColumnType::UNDEFINED) {
        spdlog::info("Only considering columns with type " + FLAGS_typefilter);
    }

    // Create relation
    Relation relation = files::readDirectory(schema, FLAGS_binary.back() == '/' ? FLAGS_binary : FLAGS_binary + "/");
    std::filesystem::path yaml_path = FLAGS_yaml;
    relation.name = yaml_path.stem();

    btrblocks::files::writeDirectory(relation, FLAGS_btr, FLAGS_stats, FLAGS_compressionout, typefilter,
                                     FLAGS_column, FLAGS_chunk, FLAGS_verify, binary_creation_time);

    return 0;
}
