/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "catch.hpp"
#include "op/scan/paimon/paimon_admitted_pool.hpp"
#include "op/scan/paimon/paimon_batch_cursor.hpp"
#include "op/scan/paimon/paimon_native_read.hpp"
#include "scan/paimon_native_test_fixture.hpp"
#include "scan_manager/preparation_ledger.hpp"
#include "scan_manager/preparation_test_support.hpp"

#include <duckdb/common/arrow/arrow.hpp>
#include <paimon/fs/file_system_factory.h>
#include <paimon/memory/memory_pool.h>
#include <paimon/scan_context.h>
#include <paimon/table/source/data_split.h>
#include <paimon/table/source/plan.h>
#include <paimon/table/source/table_scan.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <exception>
#include <functional>
#include <mutex>
#include <new>
#include <random>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <system_error>
#include <typeinfo>
#include <utility>
#include <vector>

using namespace sirius::test::paimon;
namespace {
class fault_pool final : public paimon::MemoryPool {
 public:
  explicit fault_pool(bool reject) : reject(reject), upstream(paimon::GetMemoryPool()) {}
  bool reject;
  unsigned attempts = 0;
  void* Malloc(uint64_t n, uint64_t alignment) override
  {
    if (reject) {
      ++attempts;
      throw std::bad_alloc();
    }
    return upstream->Malloc(n, alignment);
  }
  void* Realloc(void* p, size_t old_size, size_t new_size, uint64_t alignment) override
  {
    if (reject) {
      ++attempts;
      throw std::bad_alloc();
    }
    return upstream->Realloc(p, old_size, new_size, alignment);
  }
  void Free(void* p, uint64_t n) override { upstream->Free(p, n); }
  void Free(void* p, uint64_t n, uint64_t alignment) override { upstream->Free(p, n, alignment); }
  uint64_t CurrentUsage() const override { return upstream->CurrentUsage(); }
  uint64_t MaxMemoryUsage() const override { return upstream->MaxMemoryUsage(); }

 private:
  std::unique_ptr<paimon::MemoryPool> upstream;
};

struct parquet_stream_counts {
  std::mutex mutex;
  size_t live = 0;
  size_t peak = 0;
};

// A raw Parquet file reader in the stock SDK retains this input stream until close.
class counted_parquet_stream final : public paimon::InputStream {
 public:
  counted_parquet_stream(std::unique_ptr<paimon::InputStream> input,
                         std::shared_ptr<parquet_stream_counts> counts)
    : _input(std::move(input)), _counts(std::move(counts))
  {
    std::lock_guard lock(_counts->mutex);
    _counts->peak = std::max(_counts->peak, ++_counts->live);
  }

  ~counted_parquet_stream() override
  {
    _input.reset();
    retire();
  }

  paimon::Status Close() override
  {
    auto status = _input->Close();
    if (status.ok()) retire();
    return status;
  }

  paimon::Status Seek(int64_t offset, paimon::SeekOrigin origin) override
  {
    return _input->Seek(offset, origin);
  }
  paimon::Result<int64_t> GetPos() const override { return _input->GetPos(); }
  paimon::Result<int64_t> Read(char* buffer, int64_t size) override
  {
    return _input->Read(buffer, size);
  }
  paimon::Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) override
  {
    return _input->Read(buffer, size, offset);
  }
  void ReadAsync(char* buffer,
                 int64_t size,
                 int64_t offset,
                 std::function<void(paimon::Status)>&& callback) override
  {
    _input->ReadAsync(buffer, size, offset, std::move(callback));
  }
  paimon::Result<std::string> GetUri() const override { return _input->GetUri(); }
  paimon::Result<int64_t> Length() const override { return _input->Length(); }

 private:
  void retire()
  {
    if (_counts) {
      auto counts = std::move(_counts);
      std::lock_guard lock(counts->mutex);
      --counts->live;
    }
  }

  std::unique_ptr<paimon::InputStream> _input;
  std::shared_ptr<parquet_stream_counts> _counts;
};

class counted_parquet_filesystem final : public paimon::FileSystem {
 public:
  explicit counted_parquet_filesystem(std::shared_ptr<paimon::FileSystem> upstream)
    : _upstream(std::move(upstream))
  {
  }

  std::pair<size_t, size_t> live_and_peak() const
  {
    std::lock_guard lock(_counts->mutex);
    return {_counts->live, _counts->peak};
  }

  paimon::Result<std::unique_ptr<paimon::InputStream>> Open(std::string const& path) const override
  {
    return track(path, _upstream->Open(path));
  }
  paimon::Result<std::unique_ptr<paimon::InputStream>> Open(
    paimon::FileStatus const& status) const override
  {
    return track(status.GetPath(), _upstream->Open(status));
  }
  paimon::Result<std::unique_ptr<paimon::OutputStream>> Create(std::string const& path,
                                                               bool overwrite) const override
  {
    return _upstream->Create(path, overwrite);
  }
  paimon::Status Mkdirs(std::string const& path) const override { return _upstream->Mkdirs(path); }
  paimon::Status Rename(std::string const& from, std::string const& to) const override
  {
    return _upstream->Rename(from, to);
  }
  paimon::Status Delete(std::string const& path, bool recursive) const override
  {
    return _upstream->Delete(path, recursive);
  }
  paimon::Result<paimon::FileStatus> GetFileStatus(std::string const& path) const override
  {
    return _upstream->GetFileStatus(path);
  }
  paimon::Status ListDir(std::string const& path,
                         std::vector<paimon::BasicFileStatus>* entries) const override
  {
    return _upstream->ListDir(path, entries);
  }
  paimon::Status ListFileStatus(std::string const& path,
                                std::vector<paimon::FileStatus>* entries) const override
  {
    return _upstream->ListFileStatus(path, entries);
  }
  paimon::Result<bool> Exists(std::string const& path) const override
  {
    return _upstream->Exists(path);
  }

 private:
  paimon::Result<std::unique_ptr<paimon::InputStream>> track(
    std::string const& path, paimon::Result<std::unique_ptr<paimon::InputStream>> input) const
  {
    if (!input.ok() || !path.ends_with(".parquet")) return input;
    return std::unique_ptr<paimon::InputStream>(
      std::make_unique<counted_parquet_stream>(std::move(input).value(), _counts));
  }

  std::shared_ptr<paimon::FileSystem> _upstream;
  std::shared_ptr<parquet_stream_counts> _counts = std::make_shared<parquet_stream_counts>();
};

void append_ids(ArrowSchema const& schema, ArrowArray const& array, std::vector<int64_t>& actual)
{
  REQUIRE(std::string(schema.format) == "+s");
  REQUIRE(schema.n_children == array.n_children);
  REQUIRE(array.offset == 0);
  for (int64_t c = 0; c < schema.n_children; ++c) {
    auto const& field = *schema.children[c];
    if (!field.name || std::string(field.name) != "id") continue;
    auto const& column = *array.children[c];
    REQUIRE(std::string(field.format) == "l");
    REQUIRE(column.offset == 0);
    REQUIRE(column.length == array.length);
    REQUIRE(column.null_count == 0);
    REQUIRE(column.n_buffers == 2);
    auto data = static_cast<int64_t const*>(column.buffers[1]);
    actual.insert(actual.end(), data, data + column.length);
    return;
  }
  FAIL("Missing projected id field");
}
}  // namespace

TEST_CASE("Paimon cursor reads retained snapshot from real append files",
          "[paimon_cursor][paimon_native]")
{
  auto owners            = std::make_shared<native_fixture>();
  auto pool              = owners->pool;
  auto const snapshot_id = GENERATE(1, 2);
  {
    auto metadata        = fixture_metadata();
    metadata.snapshot_id = snapshot_id;
    std::shared_ptr<paimon::FileSystem> filesystem;
    if (snapshot_id == 2) {
      auto local = paimon::FileSystemFactory::Get("local", metadata.path, {});
      REQUIRE(local.ok());
      filesystem = std::move(local).value();
      // The explicitly supplied filesystem must serve both planning and data reads.
      metadata.options["file-system"]              = "unavailable-for-paimon-native-test";
      metadata.options["source.split.target-size"] = "1B";
      auto options                                 = metadata.options;
      options["scan.mode"]                         = "from-snapshot-full";
      options["scan.snapshot-id"]                  = "2";
      auto context                                 = paimon::ScanContextBuilder(metadata.path)
                       .SetOptions(options)
                       .SetTableSchema(metadata.schema_json)
                       .WithFileSystem(filesystem)
                       .WithMemoryPool(pool)
                       .Finish();
      REQUIRE(context.ok());
      auto scan = paimon::TableScan::Create(std::move(context).value());
      REQUIRE(scan.ok());
      auto plan = scan.value()->CreatePlan();
      REQUIRE(plan.ok());
      REQUIRE(plan.value()->SnapshotId() == 2);
      REQUIRE(plan.value()->Splits().size() == 2);
    }
    auto cursor =
      sirius::op::scan::open_paimon_read(metadata, {0}, pool, owners, 1, {}, filesystem);
    REQUIRE(cursor);
    std::vector<int64_t> actual;
    size_t batches = 0;
    auto accept    = [&](ArrowSchema const& schema, ArrowArray const& array) {
      REQUIRE(++batches <= 8);
      append_ids(schema, array, actual);
    };
    while (cursor->consume({}, 1, accept)) {}
    REQUIRE(batches > 1);
    // Independent corpus expectations: append_a at snapshot 1, append_b at snapshot 2.
    std::sort(actual.begin(), actual.end());
    auto const expected =
      snapshot_id == 1 ? std::vector<int64_t>{1, 1, 2} : std::vector<int64_t>{1, 1, 2, 3};
    REQUIRE(actual == expected);
    REQUIRE_FALSE(cursor->consume({}, 1, accept));
  }
  owners.reset();
  REQUIRE(pool->CurrentUsage() == 0);
}

TEST_CASE("Paimon native cursor prepares one file at a time within a packed split",
          "[paimon_cursor][paimon_native]")
{
  auto owners                                  = std::make_shared<native_fixture>();
  auto metadata                                = fixture_metadata();
  metadata.snapshot_id                         = 2;
  metadata.options["source.split.target-size"] = "128MB";
  metadata.options["read.preparation-window"]  = "4";
  auto local = paimon::FileSystemFactory::Get("local", metadata.path, {});
  REQUIRE(local.ok());
  auto filesystem      = std::make_shared<counted_parquet_filesystem>(std::move(local).value());
  auto options         = metadata.options;
  options["scan.mode"] = "from-snapshot-full";
  options["scan.snapshot-id"] = "2";
  auto context                = paimon::ScanContextBuilder(metadata.path)
                   .SetOptions(options)
                   .SetTableSchema(metadata.schema_json)
                   .WithFileSystem(filesystem)
                   .WithMemoryPool(owners->pool)
                   .Finish();
  REQUIRE(context.ok());
  auto scan = paimon::TableScan::Create(std::move(context).value());
  REQUIRE(scan.ok());
  auto plan = scan.value()->CreatePlan();
  REQUIRE(plan.ok());
  REQUIRE(plan.value()->SnapshotId() == 2);
  REQUIRE(plan.value()->Splits().size() == 1);
  auto split = std::dynamic_pointer_cast<paimon::DataSplit>(plan.value()->Splits().front());
  REQUIRE(split);
  auto const files = split->GetFileList();
  REQUIRE(files.size() == 2);
  REQUIRE(files[0].file_path.ends_with(".parquet"));
  REQUIRE(files[1].file_path.ends_with(".parquet"));
  REQUIRE(files[0].file_path != files[1].file_path);

  auto cursor =
    sirius::op::scan::open_paimon_read(metadata, {0}, owners->pool, owners, 1, {}, filesystem);
  REQUIRE(cursor);
  std::vector<int64_t> actual;
  size_t first_batch_live = 0;
  size_t batches          = 0;
  auto accept             = [&](ArrowSchema const& schema, ArrowArray const& array) {
    if (++batches == 1) first_batch_live = filesystem->live_and_peak().first;
    REQUIRE(batches <= 4);
    append_ids(schema, array, actual);
  };
  while (cursor->consume({}, 1, accept)) {}
  std::sort(actual.begin(), actual.end());
  REQUIRE(actual == std::vector<int64_t>{1, 1, 2, 3});
  auto const [live, peak] = filesystem->live_and_peak();
  REQUIRE(live == 0);
  CHECK(first_batch_live == 1);
  CHECK(peak == 1);
}

TEST_CASE("Paimon native binding preserves empty and refuses inconsistent selectors",
          "[paimon_cursor][paimon_native]")
{
  auto owners   = std::make_shared<native_fixture>();
  auto metadata = fixture_metadata();
  SECTION("empty binding never plans latest")
  {
    metadata.snapshot_id.reset();
    REQUIRE_FALSE(sirius::op::scan::open_paimon_read(metadata, {0}, owners->pool, owners, 1, {}));
  }
  SECTION("missing retained snapshot is an error")
  {
    metadata.snapshot_id = 9999;
    REQUIRE_THROWS(sirius::op::scan::open_paimon_read(metadata, {0}, owners->pool, owners, 1, {}));
  }
  SECTION("conflicting branch selector")
  {
    metadata.options["branch"] = "other";
    REQUIRE_THROWS_AS(
      sirius::op::scan::open_paimon_read(metadata, {0}, owners->pool, owners, 1, {}),
      std::invalid_argument);
  }
  SECTION("conflicting numeric selector")
  {
    metadata.options["scan.snapshot-id"] = "2";
    REQUIRE_THROWS_AS(
      sirius::op::scan::open_paimon_read(metadata, {0}, owners->pool, owners, 1, {}),
      std::invalid_argument);
  }
  SECTION("non-numeric selector cannot override retained binding")
  {
    metadata.options["scan.timestamp-millis"] = "1";
    REQUIRE_THROWS_AS(
      sirius::op::scan::open_paimon_read(metadata, {0}, owners->pool, owners, 1, {}),
      std::invalid_argument);
  }
  SECTION("empty binding still validates projection")
  {
    metadata.snapshot_id.reset();
    REQUIRE_THROWS(sirius::op::scan::open_paimon_read(metadata, {99}, owners->pool, owners, 1, {}));
  }
  SECTION("empty binding still validates schema")
  {
    metadata.snapshot_id.reset();
    metadata.schema_json = "{}";
    REQUIRE_THROWS(sirius::op::scan::open_paimon_read(metadata, {0}, owners->pool, owners, 1, {}));
  }
  SECTION("stop precedes native initialization")
  {
    std::stop_source source;
    source.request_stop();
    REQUIRE_THROWS_AS(sirius::op::scan::open_paimon_read(
                        metadata,
                        {0},
                        owners->pool,
                        owners,
                        1,
                        sirius::scan_manager::preparation_stop_token(source.get_token())),
                      std::system_error);
  }
  REQUIRE(owners->pool->CurrentUsage() == 0);
}

TEST_CASE("Paimon native planning allocation refusal cannot become EOF",
          "[paimon_cursor][paimon_native]")
{
  auto owners   = std::make_shared<native_fixture>();
  auto metadata = fixture_metadata();
  auto pool     = std::make_shared<fault_pool>(true);
  REQUIRE_THROWS(sirius::op::scan::open_paimon_read(metadata, {0}, pool, owners, 1, {}));
  REQUIRE(pool->attempts > 0);
  REQUIRE(pool->CurrentUsage() == 0);
  // The failed attempt must not poison a subsequent healthy reader.
  auto cursor = sirius::op::scan::open_paimon_read(metadata, {0}, owners->pool, owners, 1, {});
  REQUIRE(cursor);
  REQUIRE(cursor->consume({}, 1, [](ArrowSchema const&, ArrowArray const&) {}));
  cursor->close();
  cursor.reset();
  REQUIRE(owners->pool->CurrentUsage() == 0);
}

TEST_CASE("Paimon native read allocation refusal is terminal and drains",
          "[paimon_cursor][paimon_native]")
{
  auto owners   = std::make_shared<native_fixture>();
  auto metadata = fixture_metadata();
  auto pool     = std::make_shared<fault_pool>(false);
  auto cursor   = sirius::op::scan::open_paimon_read(metadata, {0}, pool, owners, 1, {});
  REQUIRE(cursor);
  pool->reject = true;
  REQUIRE_THROWS(cursor->consume({}, 1, [](ArrowSchema const&, ArrowArray const&) {}));
  REQUIRE(pool->attempts > 0);
  auto failures = pool->attempts;
  REQUIRE_THROWS(cursor->consume({}, 1, [](ArrowSchema const&, ArrowArray const&) {}));
  REQUIRE(pool->attempts == failures);
  cursor->close();
  cursor.reset();
  REQUIRE(pool->CurrentUsage() == 0);
}

TEST_CASE("Stock Paimon native readers draw column buffers from retained statement backing",
          "[paimon_cursor][paimon_native][paimon_admitted_pool]")
{
  namespace sm = sirius::scan_manager;
  using sirius::op::scan::open_paimon_read;
  using sirius::op::scan::detail::paimon_admitted_pool;
  // Stock planning needs a 1 MiB Avro input buffer plus arena overhead and other live allocations.
  constexpr uint64_t bytes = 4 << 20;
  constexpr sm::unit_key first{1, 0}, second{2, 0};
  sm::test::test_reservation_provider provider;
  provider.grant_bytes = 2 * bytes;
  sm::preparation_ledger ledger(provider);
  std::array scans{sm::scan_envelope{{{{}, bytes}}, {}, true},
                   sm::scan_envelope{{{{}, bytes}}, {}, true}};
  auto decision = ledger.admit(scans, sm::memory_space_id{cucascade::memory::Tier::HOST, 0});
  REQUIRE(decision.deferred);
  REQUIRE(decision.sigma_retained == 2 * bytes);

  auto make_pool = [&](sm::unit_key key) {
    auto permit = ledger.try_acquire_unit(key, bytes);
    REQUIRE(permit);
    auto backing = ledger.allocator(permit).allocate_retained(bytes);
    auto begin   = reinterpret_cast<uintptr_t>(backing.data());
    return std::pair{paimon_admitted_pool::create(std::move(backing)), begin};
  };
  auto [left_pool, left_begin]   = make_pool(first);
  auto [right_pool, right_begin] = make_pool(second);
  REQUIRE(ledger.permits_in_flight() == 0);
  REQUIRE(provider.seen->allocated_bytes == 2 * bytes);
  {
    auto permit = ledger.acquire_permit(second);
    REQUIRE(permit);
    try {
      ledger.allocator(permit).allocate_retained(1);
      FAIL("second source exceeded its retained grant");
    } catch (sm::preparation_resource_error const& error) {
      REQUIRE(error.bound_exceeded);
    }
    CHECK(ledger.bound_exceeded() == 1);
    CHECK(provider.seen->allocations == 2);
  }

  auto modules              = std::make_shared<native_fixture>();
  auto metadata             = fixture_metadata();
  metadata.snapshot_id      = 2;
  auto const planning_start = std::chrono::steady_clock::now();
  auto left                 = open_paimon_read(metadata, {0}, left_pool, modules, 1, {});
  auto right                = open_paimon_read(metadata, {0}, right_pool, modules, 1, {});
  REQUIRE(left);
  REQUIRE(right);
  auto const planning_elapsed = std::chrono::steady_clock::now() - planning_start;
  auto const planning_peak    = right_pool->peak_live_allocations();
  auto const reading_start    = std::chrono::steady_clock::now();

  left_pool.reset();
  std::vector<int64_t> left_values, right_values;
  auto read = [&](auto& cursor, uintptr_t begin, std::vector<int64_t>& values) {
    return cursor.consume({}, 1, [&](ArrowSchema const& schema, ArrowArray const& array) {
      REQUIRE(array.length == 1);
      for (int64_t i = 0; i < schema.n_children; ++i) {
        if (std::string_view(schema.children[i]->name) != "id") continue;
        auto const& column = *array.children[i];
        REQUIRE(column.offset == 0);
        REQUIRE(column.n_buffers == 2);
        auto* value  = static_cast<int64_t const*>(column.buffers[1]);
        auto address = reinterpret_cast<uintptr_t>(value);
        REQUIRE(address >= begin);
        REQUIRE(address - begin <= bytes - sizeof(*value));
        CHECK(ledger.charged_bytes(first) + ledger.charged_bytes(second) == 2 * bytes);
        CHECK(provider.seen->allocated_bytes == 2 * bytes);
        REQUIRE(values.size() < 4);
        values.push_back(*value);
        return;
      }
      FAIL("stock Paimon batch had no retained id field");
    });
  };
  REQUIRE(read(*left, left_begin, left_values));

  SECTION("both live readers return the exact snapshot and retire their own backing")
  {
    REQUIRE(read(*right, right_begin, right_values));
    while (read(*left, left_begin, left_values)) {}
    while (read(*right, right_begin, right_values)) {}
  }
  SECTION("physical exhaustion before the second reader can deliver a batch")
  {
    struct hold {
      paimon_admitted_pool& pool;
      uint64_t size;
      void* data;
      hold(paimon_admitted_pool& pool, uint64_t size)
        : pool(pool), size(size), data(pool.Malloc(size))
      {
      }
      ~hold() { pool.Free(data, size); }
    };
    std::vector<std::unique_ptr<hold>> pressure;
    for (uint64_t size = bytes; size; size /= 2) {
      try {
        for (;;)
          pressure.emplace_back(std::make_unique<hold>(*right_pool, size));
      } catch (std::bad_alloc const&) {
      }
    }
    auto refused          = right_pool->capacity_refusals();
    unsigned publications = 0;
    auto consume          = [&] {
      std::exception_ptr failure;
      std::string diagnostic;
      try {
        (void)right->consume({}, 1, [&](ArrowSchema const&, ArrowArray const&) { ++publications; });
      } catch (std::bad_alloc const& error) {
        // The stock reader lets this allocation refusal escape directly from NextBatch.
        REQUIRE(typeid(error) == typeid(std::bad_alloc));
        failure    = std::current_exception();
        diagnostic = error.what();
      }
      REQUIRE(failure);
      return std::pair{failure, diagnostic};
    };
    auto first = consume();
    REQUIRE(right_pool->capacity_refusals() > refused);
    REQUIRE(publications == 0);
    refused     = right_pool->capacity_refusals();
    auto repeat = consume();
    REQUIRE(repeat.first == first.first);
    REQUIRE(repeat.second == first.second);
    REQUIRE(right_pool->capacity_refusals() == refused);
    REQUIRE(publications == 0);
    REQUIRE(provider.seen->allocations == 2);
    pressure.clear();

    // The terminal reader cannot retry. An independent source and a fresh cursor can.
    while (read(*left, left_begin, left_values)) {}
    right->close();
    right.reset();
    REQUIRE(right_pool->CurrentUsage() == 0);
    right = open_paimon_read(metadata, {0}, right_pool, modules, 1, {});
    REQUIRE(right);
    while (read(*right, right_begin, right_values)) {}
  }

  for (auto* values : {&left_values, &right_values}) {
    std::sort(values->begin(), values->end());
    REQUIRE(*values == std::vector<int64_t>{1, 1, 2, 3});
  }

  INFO("two opens in microseconds: "
       << std::chrono::duration_cast<std::chrono::microseconds>(planning_elapsed).count());
  INFO("reading and retirement scenario in microseconds: "
       << std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() -
                                                                reading_start)
            .count());
  CAPTURE(planning_peak, right_pool->peak_live_allocations(), right_pool->MaxMemoryUsage());
  CHECK(planning_peak > 0);
  CHECK(right_pool->invalid_free_requests() == 0);

  left->close();
  REQUIRE(ledger.charged_bytes(first) == bytes);
  left.reset();
  REQUIRE(ledger.charged_bytes(first) == 0);
  REQUIRE(ledger.charged_bytes(second) == bytes);
  REQUIRE(provider.seen->allocated_bytes == bytes);
  right->close();
  right.reset();
  REQUIRE(right_pool->CurrentUsage() == 0);
  right_pool.reset();
  REQUIRE(ledger.charged_bytes(second) == 0);
  REQUIRE(provider.seen->allocated_bytes == 0);
  ledger.retire_unit(first);
  ledger.retire_unit(second);
  REQUIRE(ledger.tracked_units() == 0);
}

TEST_CASE("Paimon admitted reallocation preserves the old buffer on physical exhaustion",
          "[paimon_cursor][paimon_admitted_pool]")
{
  namespace sm = sirius::scan_manager;
  using sirius::op::scan::detail::paimon_admitted_pool;
  constexpr uint64_t bytes = 8192;
  constexpr sm::unit_key key{1, 0};
  sm::test::test_reservation_provider provider;
  provider.grant_bytes = bytes;
  sm::preparation_ledger ledger(provider);
  std::array scans{sm::scan_envelope{{{{}, bytes}}, {}, true}};
  REQUIRE(ledger.admit(scans, sm::memory_space_id{cucascade::memory::Tier::HOST, 0}).deferred);
  auto permit = ledger.try_acquire_unit(key, bytes);
  REQUIRE(permit);
  auto pool = paimon_admitted_pool::create(ledger.allocator(permit).allocate_retained(bytes));
  permit.release();

  constexpr size_t old_size = bytes / 3, new_size = old_size * 2;
  auto* old = static_cast<char*>(pool->Malloc(old_size, 256));
  REQUIRE(reinterpret_cast<uintptr_t>(old) % 256 == 0);
  std::memset(old, 0x5a, old_size);
  REQUIRE_THROWS_AS(pool->Realloc(old, old_size, new_size, 256), std::bad_alloc);
  auto const old_extent = pool->CurrentUsage();
  CHECK(old_extent > old_size);
  CHECK(pool->MaxMemoryUsage() == old_extent);
  int unrelated = 0;
  pool->Free(old, old_size - 1);
  CHECK(pool->invalid_free_requests() == 1);
  pool->Free(&unrelated, sizeof(unrelated), 64);
  CHECK(pool->invalid_free_requests() == 2);
  CHECK(pool->CurrentUsage() == old_extent);
  REQUIRE_THROWS_AS(pool->Realloc(nullptr, 1, 8), std::bad_alloc);
  REQUIRE_THROWS_AS(pool->Realloc(&unrelated, sizeof(unrelated), 8), std::bad_alloc);
  REQUIRE_THROWS_AS(pool->Realloc(old, old_size - 1, 8), std::bad_alloc);
  CHECK(pool->CurrentUsage() == old_extent);
  CHECK(std::all_of(old, old + old_size, [](char value) { return value == 0x5a; }));
  pool->Free(old, old_size);
  // The larger request alone fits. The refused peak came from the live overlap.
  auto* larger = pool->Malloc(new_size, 256);
  REQUIRE(larger != nullptr);
  pool->Free(larger, new_size, 256);
  REQUIRE(pool->CurrentUsage() == 0);
  CHECK(provider.seen->allocations == 1);
  pool.reset();
  REQUIRE(provider.seen->allocated_bytes == 0);
  REQUIRE(ledger.charged_bytes(key) == 0);
}

TEST_CASE("Paimon admitted pool preserves aligned live allocations under mixed requests",
          "[paimon_cursor][paimon_admitted_pool]")
{
  namespace sm = sirius::scan_manager;
  using sirius::op::scan::detail::paimon_admitted_pool;
  constexpr uint64_t bytes = 128 << 10;
  constexpr sm::unit_key key{1, 0};
  sm::test::test_reservation_provider provider;
  provider.grant_bytes = bytes;
  sm::preparation_ledger ledger(provider);
  std::array scans{sm::scan_envelope{{{{}, bytes}}, {}, true}};
  REQUIRE(ledger.admit(scans, sm::memory_space_id{cucascade::memory::Tier::HOST, 0}).deferred);
  auto permit = ledger.try_acquire_unit(key, bytes);
  REQUIRE(permit);
  auto pool = paimon_admitted_pool::create(ledger.allocator(permit).allocate_retained(bytes));
  permit.release();

  REQUIRE(pool->Malloc(0) == nullptr);
  REQUIRE_THROWS_AS(pool->Malloc(1, 3), std::bad_alloc);
  auto* small = static_cast<unsigned char*>(pool->Malloc(11, 1));
  std::memset(small, 0x4b, 11);
  auto* aligned = static_cast<unsigned char*>(pool->Realloc(small, 11, 11, 4096));
  REQUIRE(reinterpret_cast<uintptr_t>(aligned) % 4096 == 0);
  REQUIRE(std::all_of(aligned, aligned + 11, [](auto c) { return c == 0x4b; }));
  REQUIRE(pool->Realloc(aligned, 11, 0) == nullptr);

  auto const seed = GENERATE(17u, 1481u, 88933u);
  std::mt19937 rng(seed);
  constexpr std::array<uint64_t, 4> alignments{1, 8, 64, 4096};
  struct allocation {
    unsigned char* data   = nullptr;
    size_t bytes          = 0;
    unsigned char pattern = 0;
  };
  std::array<allocation, 256> live{};
  auto verify = [](allocation const& item) {
    if (item.data)
      REQUIRE(
        std::all_of(item.data, item.data + item.bytes, [&](auto c) { return c == item.pattern; }));
  };
  unsigned successes = 0;
  for (unsigned i = 0; i < 100'000; ++i) {
    auto& item = live[rng() % live.size()];
    verify(item);
    if (item.data && rng() % 4 == 0) {
      pool->Free(item.data, item.bytes);
      item = {};
      continue;
    }
    auto const size      = 1 + rng() % 1024;
    auto const alignment = alignments[rng() % alignments.size()];
    auto const occupied  = pool->CurrentUsage();
    unsigned char* data;
    try {
      data = static_cast<unsigned char*>(pool->Realloc(item.data, item.bytes, size, alignment));
    } catch (std::bad_alloc const&) {
      REQUIRE(pool->CurrentUsage() == occupied);
      verify(item);
      continue;
    }
    REQUIRE(data);
    REQUIRE(reinterpret_cast<uintptr_t>(data) % alignment == 0);
    if (item.data)
      REQUIRE(std::all_of(data,
                          data + std::min(item.bytes, static_cast<size_t>(size)),
                          [&](auto c) { return c == item.pattern; }));
    auto const pattern = static_cast<unsigned char>(1 + rng() % 255);
    std::memset(data, pattern, size);
    item = {data, static_cast<size_t>(size), pattern};
    ++successes;
  }
  REQUIRE(successes > 1'000);
  REQUIRE(pool->capacity_refusals() > 0);
  for (auto const& item : live) {
    verify(item);
    if (item.data) pool->Free(item.data, item.bytes);
  }
  REQUIRE(pool->CurrentUsage() == 0);
  REQUIRE(pool->invalid_free_requests() == 0);
  // This can fit only after the occupied and free extents have coalesced.
  auto* whole = pool->Malloc(bytes - 512);
  REQUIRE(pool->CurrentUsage() > bytes - 512);
  pool->Free(whole, bytes - 512);
  REQUIRE(pool->CurrentUsage() == 0);
}
