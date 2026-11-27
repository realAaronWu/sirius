// Copyright 2026, Sirius Contributors. SPDX-License-Identifier: Apache-2.0
#pragma once

#include "scan_manager/preparation_ledger.hpp"

#include <paimon/memory/memory_pool.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <utility>

namespace sirius::op::scan::detail {
// Adapts an existing retained statement block to the stock SDK MemoryPool.
// The pool and block descriptors live in the backing. The shared control block
// and SDK allocations outside this pool require separate accounting.
class paimon_admitted_pool final : public paimon::MemoryPool {
 public:
  static std::shared_ptr<paimon_admitted_pool> create(scan_manager::charged_block backing);

  void* Malloc(uint64_t bytes, uint64_t alignment = 0) override
  {
    auto align = normalize_alignment(alignment);
    std::lock_guard lock(mutex_);
    return allocate(bytes, align);
  }
  void* Realloc(void* pointer, size_t old_size, size_t new_size, uint64_t alignment = 0) override
  {
    auto align = normalize_alignment(alignment);
    std::lock_guard lock(mutex_);
    if (!pointer) {
      if (old_size) throw std::bad_alloc();
      return allocate(new_size, align);
    }
    auto* old = find(pointer, old_size);
    if (!old) throw std::bad_alloc();
    if (!new_size) {
      release(old);
      return nullptr;
    }
    if (new_size == old_size && reinterpret_cast<uintptr_t>(pointer) % align == 0) return pointer;
    // Allocate while old is still occupied. Refusal leaves its extent/data untouched.
    auto* replacement = allocate(new_size, align);
    std::memcpy(replacement, pointer, std::min(old_size, new_size));
    release(old);
    return replacement;
  }
  void Free(void* pointer, uint64_t bytes) noexcept override
  {
    std::lock_guard lock(mutex_);
    if (!pointer && !bytes) return;
    if (auto* entry = find(pointer, bytes)) {
      release(entry);
      return;
    }
    // SDK destructors call Free. Keep the allocation charged until a valid
    // Free or arena teardown, because a bad request cannot identify a live block.
    if (invalid_frees_ != UINT64_MAX) ++invalid_frees_;
  }
  void Free(void* pointer, uint64_t bytes, uint64_t) noexcept override { Free(pointer, bytes); }
  uint64_t CurrentUsage() const override
  {
    std::lock_guard lock(mutex_);
    return occupied_;
  }
  uint64_t MaxMemoryUsage() const override
  {
    std::lock_guard lock(mutex_);
    return peak_occupied_;
  }
  uint64_t invalid_free_requests() const
  {
    std::lock_guard lock(mutex_);
    return invalid_frees_;
  }
  uint64_t peak_live_allocations() const
  {
    std::lock_guard lock(mutex_);
    return peak_live_;
  }
  uint64_t capacity_refusals() const
  {
    std::lock_guard lock(mutex_);
    return refusals_;
  }

 private:
  static void destroy(paimon_admitted_pool* pool) noexcept
  {
    // The backing resides in its own pool metadata. Move it out before any
    // member/base destruction so physical storage cannot disappear underneath it.
    auto backing = std::move(pool->backing_);
    pool->~paimon_admitted_pool();
    backing.release();
  }
  scan_manager::charged_block backing_;
  struct block {
    block* previous;
    block* next;
    size_t extent;  // Header, alignment padding and data, including any unsplittable tail.
    size_t requested;
    void* data;  // Null is free; zero-byte requests never consume a block.
  };
  explicit paimon_admitted_pool(void* storage, size_t bytes)
    : first_(::new(storage) block{nullptr, nullptr, bytes, 0, nullptr})
  {
  }
  static size_t normalize_alignment(uint64_t alignment)
  {
    // Preserve the selected SDK's default alignment, not a capacity constant.
    if (!alignment) alignment = 64;
    if ((alignment & (alignment - 1)) ||
        alignment > static_cast<uint64_t>(std::numeric_limits<ptrdiff_t>::max()))
      throw std::bad_alloc();
    return static_cast<size_t>(alignment);
  }
  [[noreturn]] void refuse()
  {
    if (refusals_ != UINT64_MAX) ++refusals_;
    throw std::bad_alloc();
  }
  void* allocate(uint64_t bytes, size_t alignment)
  {
    if (!bytes) return nullptr;
    if (bytes > static_cast<uint64_t>(std::numeric_limits<ptrdiff_t>::max())) refuse();
    for (auto* available = first_; available; available = available->next) {
      if (available->data) continue;
      auto* start        = reinterpret_cast<std::byte*>(available);
      auto* after_header = start + sizeof(block);
      auto remainder     = available->extent - sizeof(block);
      // Compute padding without pointer+alignment arithmetic that could overflow.
      auto padding = (alignment - (reinterpret_cast<uintptr_t>(after_header) & (alignment - 1))) &
                     (alignment - 1);
      if (padding > remainder || bytes > remainder - padding) continue;
      auto* data      = after_header + padding;
      auto occupied   = sizeof(block) + padding + static_cast<size_t>(bytes);
      void* tail      = start + occupied;
      auto tail_bytes = available->extent - occupied;
      // A split needs an aligned header and at least one usable byte. Otherwise keep the
      // short remainder in this allocation; it still fits entirely in the fixed backing.
      if (std::align(alignof(block), sizeof(block) + 1, tail, tail_bytes)) {
        auto* suffix = ::new (tail) block{available, available->next, tail_bytes, 0, nullptr};
        if (suffix->next) suffix->next->previous = suffix;
        available->next = suffix;
        available->extent -= tail_bytes;
      }
      available->requested = static_cast<size_t>(bytes);
      available->data      = data;
      occupied_ += available->extent;
      peak_occupied_ = std::max(peak_occupied_, occupied_);
      peak_live_     = std::max(peak_live_, ++live_);
      return data;
    }
    refuse();
  }
  block* find(void* pointer, uint64_t bytes) noexcept
  {
    // Never inspect storage before a caller pointer. The descriptors are our own bounded list.
    for (auto* entry = first_; entry; entry = entry->next)
      if (entry->data && entry->data == pointer && entry->requested == bytes) return entry;
    return nullptr;
  }
  void release(block* entry) noexcept
  {
    occupied_ -= entry->extent;
    --live_;
    entry->data      = nullptr;
    entry->requested = 0;
    if (entry->next && !entry->next->data) {
      auto* next = entry->next;
      entry->extent += next->extent;
      entry->next = next->next;
      if (entry->next) entry->next->previous = entry;
      next->~block();
    }
    if (entry->previous && !entry->previous->data) {
      auto* previous = entry->previous;
      previous->extent += entry->extent;
      previous->next = entry->next;
      if (previous->next) previous->next->previous = previous;
      entry->~block();
    }
  }
  mutable std::mutex mutex_;
  block* first_;
  // Occupied extents include headers, padding and unsplittable tails.
  // The ledger charges the entire retained backing, including free extents.
  uint64_t occupied_ = 0, peak_occupied_ = 0;
  uint64_t live_ = 0, peak_live_ = 0;
  uint64_t refusals_ = 0, invalid_frees_ = 0;
};

inline std::shared_ptr<paimon_admitted_pool> paimon_admitted_pool::create(
  scan_manager::charged_block backing)
{
  if (!backing || !backing.retained() ||
      backing.size() > static_cast<uint64_t>(std::numeric_limits<ptrdiff_t>::max()))
    throw std::bad_alloc();
  void* storage = backing.data();
  auto bytes    = static_cast<size_t>(backing.size());
  auto* object =
    std::align(alignof(paimon_admitted_pool), sizeof(paimon_admitted_pool), storage, bytes);
  if (!object) throw std::bad_alloc();
  storage = static_cast<std::byte*>(storage) + sizeof(paimon_admitted_pool);
  bytes -= sizeof(paimon_admitted_pool);
  if (!std::align(alignof(block), sizeof(block) + 1, storage, bytes)) throw std::bad_alloc();
  auto* pool = ::new (object) paimon_admitted_pool(storage, bytes);
  // Construction must finish while the caller still owns backing. Otherwise a
  // partially constructed object's unwinding could free its own base storage.
  pool->backing_ = std::move(backing);
  return {pool, destroy};
}
}  // namespace sirius::op::scan::detail
