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

#include "op/scan/paimon/paimon_native_read.hpp"

#include <paimon/executor.h>
#include <paimon/fs/file_system.h>
#include <paimon/memory/memory_pool.h>
#include <paimon/read_context.h>
#include <paimon/scan_context.h>
#include <paimon/status.h>
#include <paimon/table/source/plan.h>
#include <paimon/table/source/table_read.h>
#include <paimon/table/source/table_scan.h>

#include <functional>
#include <optional>
#include <set>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace sirius::op::scan {
namespace {
void check_stop(scan_manager::preparation_stop_token stop)
{
  if (stop.stop_requested())
    throw std::system_error(std::make_error_code(std::errc::operation_canceled));
}
template <class T>
T checked(paimon::Result<T>&& result)
{
  if (!result.ok()) detail::throw_paimon_status(result.status());
  return std::move(result).value();
}
class inline_executor final : public paimon::Executor {
 public:
  void Add(std::function<void()> task) override { task(); }
  void ShutdownNow() override {}
  uint32_t GetThreadNum() const override { return 1; }
};

// The stock Avro reader converts read failures to Invalid and preserves their diagnostic.
constexpr auto stream_stop_message = "Sirius stopped Paimon planning stream I/O";

paimon::Status planning_stopped() { return paimon::Status::Cancelled("Paimon planning stopped"); }

class planning_input_stream final : public paimon::InputStream {
 public:
  planning_input_stream(std::unique_ptr<paimon::InputStream> input,
                        scan_manager::preparation_stop_token stop,
                        bool& read_cancelled,
                        std::optional<int64_t> checked_length)
    : input_(std::move(input)),
      stop_(std::move(stop)),
      read_cancelled_(read_cancelled),
      checked_length_(checked_length)
  {
  }

  paimon::Status Close() override { return input_->Close(); }
  paimon::Result<int64_t> Length() const override
  {
    if (checked_length_) return *checked_length_;
    return input_->Length();
  }
  paimon::Result<int64_t> GetPos() const override { return input_->GetPos(); }
  paimon::Result<std::string> GetUri() const override { return input_->GetUri(); }
  paimon::Status Seek(int64_t offset, paimon::SeekOrigin origin) override
  {
    if (stop_.stop_requested()) return cancelled();
    return input_->Seek(offset, origin);
  }
  paimon::Result<int64_t> Read(char* buffer, int64_t size) override
  {
    if (stop_.stop_requested()) return cancelled();
    return input_->Read(buffer, size);
  }
  paimon::Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) override
  {
    if (stop_.stop_requested()) return cancelled();
    return input_->Read(buffer, size, offset);
  }
  void ReadAsync(char* buffer,
                 int64_t size,
                 int64_t offset,
                 std::function<void(paimon::Status)>&& callback) override
  {
    if (stop_.stop_requested()) {
      callback(cancelled());
      return;
    }
    input_->ReadAsync(buffer, size, offset, std::move(callback));
  }

 private:
  paimon::Status cancelled() const
  {
    read_cancelled_ = true;
    return paimon::Status::Cancelled(stream_stop_message);
  }

  std::unique_ptr<paimon::InputStream> input_;
  scan_manager::preparation_stop_token stop_;
  bool& read_cancelled_;
  std::optional<int64_t> checked_length_;
};

class planning_filesystem final : public paimon::FileSystem {
 public:
  planning_filesystem(std::shared_ptr<paimon::FileSystem> filesystem,
                      scan_manager::preparation_stop_token stop,
                      std::string snapshot_path,
                      std::optional<uint64_t> max_snapshot_bytes)
    : filesystem_(std::move(filesystem)),
      stop_(std::move(stop)),
      snapshot_path_(std::move(snapshot_path)),
      max_snapshot_bytes_(max_snapshot_bytes)
  {
  }

  paimon::Result<std::unique_ptr<paimon::InputStream>> Open(std::string const& path) const override
  {
    if (stop_.stop_requested()) return planning_stopped();
    return wrap(path, filesystem_->Open(path));
  }
  paimon::Result<std::unique_ptr<paimon::InputStream>> Open(
    paimon::FileStatus const& file) const override
  {
    if (stop_.stop_requested()) return planning_stopped();
    return wrap(file.GetPath(), filesystem_->Open(file));
  }
  paimon::Result<paimon::FileStatus> GetFileStatus(std::string const& path) const override
  {
    if (stop_.stop_requested()) return planning_stopped();
    return filesystem_->GetFileStatus(path);
  }
  paimon::Status ListDir(std::string const& path,
                         std::vector<paimon::BasicFileStatus>* files) const override
  {
    if (stop_.stop_requested()) return planning_stopped();
    return filesystem_->ListDir(path, files);
  }
  paimon::Status ListFileStatus(std::string const& path,
                                std::vector<paimon::FileStatus>* files) const override
  {
    if (stop_.stop_requested()) return planning_stopped();
    return filesystem_->ListFileStatus(path, files);
  }
  paimon::Result<bool> Exists(std::string const& path) const override
  {
    if (stop_.stop_requested()) return planning_stopped();
    return filesystem_->Exists(path);
  }
  paimon::Result<std::unique_ptr<paimon::OutputStream>> Create(std::string const&,
                                                               bool) const override
  {
    return read_only();
  }
  paimon::Status Mkdirs(std::string const&) const override { return read_only(); }
  paimon::Status Rename(std::string const&, std::string const&) const override
  {
    return read_only();
  }
  paimon::Status Delete(std::string const&, bool) const override { return read_only(); }

  void check_plan(paimon::Status const& status) const
  {
    // Restore only our observed stream cancellation wrapped by stock Avro. An
    // independent SDK error (including an in-flight I/O error) keeps its status.
    if (!status.ok()) {
      if (read_cancelled_ && status.IsInvalid() &&
          status.ToString().find(stream_stop_message) != std::string::npos)
        detail::throw_paimon_status(planning_stopped());
      detail::throw_paimon_status(status);
    }
    if (max_snapshot_bytes_ && !checked_snapshot_)
      throw std::runtime_error("Paimon planning did not open the bound snapshot for validation");
  }

 private:
  paimon::Result<std::unique_ptr<paimon::InputStream>> wrap(
    std::string const& path, paimon::Result<std::unique_ptr<paimon::InputStream>> opened) const
  {
    if (!opened.ok()) return opened.status();
    auto stream = std::move(opened).value();
    if (!stream) return paimon::Status::IOError("Paimon returned an empty input stream");
    std::optional<int64_t> checked_length;
    if (max_snapshot_bytes_ && path == snapshot_path_) {
      auto status = validate_snapshot(*stream, checked_length);
      if (!status.ok()) {
        (void)stream->Close();
        return status;
      }
      checked_snapshot_ = true;
    }
    return std::unique_ptr<paimon::InputStream>(std::make_unique<planning_input_stream>(
      std::move(stream), stop_, read_cancelled_, checked_length));
  }

  paimon::Status validate_snapshot(paimon::InputStream& stream,
                                   std::optional<int64_t>& checked_length) const
  {
    if (stop_.stop_requested()) return planning_stopped();
    // Stock LocalFS Length() stats the path again. Seek/Tell measure the opened
    // handle. Return that exact size to ReadFile so it cannot resize from a
    // second measurement.
    auto status = stream.Seek(0, paimon::FS_SEEK_END);
    if (!status.ok()) return status;
    auto length = stream.GetPos();
    if (!length.ok()) return length.status();
    if (length.value() < 0) return paimon::Status::IOError("Invalid Paimon snapshot length");
    if (static_cast<uint64_t>(length.value()) > *max_snapshot_bytes_)
      return paimon::Status::CapacityError("Paimon snapshot exceeds its file size limit");
    if (stop_.stop_requested()) return planning_stopped();
    checked_length = length.value();
    return stream.Seek(0, paimon::FS_SEEK_SET);
  }

  static paimon::Status read_only()
  {
    return paimon::Status::NotImplemented("planning filesystem is read-only");
  }

  std::shared_ptr<paimon::FileSystem> filesystem_;
  scan_manager::preparation_stop_token stop_;
  std::string snapshot_path_;
  std::optional<uint64_t> max_snapshot_bytes_;
  // Stock planning invokes filesystem operations and inline tasks on the native lane.
  mutable bool read_cancelled_   = false;
  mutable bool checked_snapshot_ = false;
};

struct read_owners {
  std::shared_ptr<void> modules;
  std::shared_ptr<paimon::MemoryPool> pool;
  std::shared_ptr<paimon::Executor> executor = std::make_shared<inline_executor>();
  std::unique_ptr<paimon::TableScan> scan;
  std::shared_ptr<paimon::Plan> plan;
  std::unique_ptr<paimon::TableRead> read;
};
}  // namespace

std::unique_ptr<paimon_batch_cursor> open_paimon_read(paimon_read_metadata const& metadata,
                                                      std::vector<int32_t> const& ids,
                                                      std::shared_ptr<paimon::MemoryPool> pool,
                                                      std::shared_ptr<void> modules,
                                                      int64_t max_rows,
                                                      scan_manager::preparation_stop_token stop,
                                                      paimon_native_controls const& controls)
{
  check_stop(stop);
  if (!pool || !modules || max_rows <= 0 || ids.empty() || metadata.branch != "main" ||
      metadata.path.empty() || metadata.schema_json.empty() || metadata.schema_id < 0 ||
      (metadata.snapshot_id && *metadata.snapshot_id <= 0))
    throw std::invalid_argument("Paimon read requires retained main metadata, owners and limits");
  auto const& filesystem = controls.filesystem;
  if (controls.max_snapshot_bytes && (!filesystem || !metadata.snapshot_id))
    throw std::invalid_argument(
      "Paimon snapshot length limit requires a numbered snapshot and filesystem");
  std::set<int32_t> unique;
  for (auto id : ids)
    if (id < 0 || !unique.insert(id).second)
      throw std::invalid_argument("Paimon read projection requires distinct stable field IDs");
  auto options = metadata.options;
  for (auto const& [key, value] : options) {
    if (key == "branch" && value != metadata.branch)
      throw std::invalid_argument("Paimon read options disagree with retained branch");
    if (!key.starts_with("scan.")) continue;
    if (key == "scan.mode" && value == "from-snapshot-full") continue;
    if (key == "scan.snapshot-id" && metadata.snapshot_id &&
        value == std::to_string(*metadata.snapshot_id))
      continue;
    throw std::invalid_argument("Paimon read options disagree with retained snapshot selection");
  }
  options["branch"]    = metadata.branch;
  options["scan.mode"] = "from-snapshot-full";
  if (metadata.snapshot_id) options["scan.snapshot-id"] = std::to_string(*metadata.snapshot_id);
  options["read.batch-size"]                    = std::to_string(max_rows);
  options["read.preparation-window"]            = "1";
  options["parquet.read.enable-pre-buffer"]     = "false";
  options["parquet.read.executor.thread-count"] = "0";
  auto owners                                   = std::make_shared<read_owners>();
  owners->modules                               = std::move(modules);
  owners->pool                                  = std::move(pool);
  if (metadata.snapshot_id) {
    paimon::ScanContextBuilder scan(metadata.path);
    std::shared_ptr<planning_filesystem> planning_fs;
    if (filesystem) {
      auto const snapshot_path = metadata.path + (metadata.path.ends_with('/') ? "" : "/") +
                                 "snapshot/snapshot-" + std::to_string(*metadata.snapshot_id);
      planning_fs = std::make_shared<planning_filesystem>(
        filesystem, stop, snapshot_path, controls.max_snapshot_bytes);
      scan.WithFileSystem(planning_fs);
    }
    owners->scan = checked(paimon::TableScan::Create(checked(scan.SetOptions(options)
                                                               .SetTableSchema(metadata.schema_json)
                                                               .WithMemoryPool(owners->pool)
                                                               .WithExecutor(owners->executor)
                                                               .Finish())));
    auto plan    = owners->scan->CreatePlan();
    if (planning_fs) planning_fs->check_plan(plan.status());
    owners->plan = checked(std::move(plan));
    if (owners->plan->SnapshotId() != metadata.snapshot_id)
      throw std::runtime_error("Paimon plan changed the retained snapshot");
  }
  check_stop(stop);
  paimon::ReadContextBuilder read(metadata.path);
  if (filesystem) read.WithFileSystem(filesystem);
  owners->read = checked(paimon::TableRead::Create(checked(read.WithBranch(metadata.branch)
                                                             .SetTableSchema(metadata.schema_json)
                                                             .SetOptions(options)
                                                             .SetReadFieldIds(ids)
                                                             .WithMemoryPool(owners->pool)
                                                             .WithExecutor(owners->executor)
                                                             .EnablePrefetch(false)
                                                             .EnableMultiThreadRowToBatch(false)
                                                             .EnableLateMaterializing(false)
                                                             .SetReadAheadCacheEnabled(false)
                                                             .EnablePredicateFilter(false)
                                                             .Finish())));
  check_stop(stop);
  // Initializing the retained schema and projection is required even for empty-at-bind.
  // Never plan "latest" for a null snapshot; later commits cannot make it nonempty.
  if (!metadata.snapshot_id || owners->plan->Splits().empty()) return {};
  auto reader = checked(owners->read->CreateReader(owners->plan->Splits()));
  try {
    check_stop(stop);
    return std::make_unique<paimon_batch_cursor>(std::move(reader), owners);
  } catch (...) {
    // Covers a stop or cursor-holder allocation failure before ownership transfer.
    if (reader) {
      try {
        reader->Close();
      } catch (...) {
      }
    }
    throw;
  }
}
}  // namespace sirius::op::scan
