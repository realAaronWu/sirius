// Copyright 2026, Sirius Contributors. SPDX-License-Identifier: Apache-2.0
#pragma once

#include "scan_manager/preparation_ledger.hpp"

#include <paimon/memory/memory_pool.h>

#include <algorithm>
#include <array>
#include <bit>
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
      release(old, pointer);
      return nullptr;
    }
    if (new_size == old_size && reinterpret_cast<uintptr_t>(pointer) % align == 0) return pointer;
    // Allocate while old is still occupied. Refusal leaves its extent/data untouched.
    auto* replacement = allocate(new_size, align);
    std::memcpy(replacement, pointer, std::min(old_size, new_size));
    release(old, pointer);
    return replacement;
  }
  void Free(void* pointer, uint64_t bytes) noexcept override
  {
    std::lock_guard lock(mutex_);
    if (!pointer && !bytes) return;
    if (auto* entry = find(pointer, bytes)) {
      release(entry, pointer);
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
    size_t extent;     // Header, alignment padding and data, including any unsplittable tail.
    size_t requested;  // Zero identifies free blocks, since zero-byte requests use no block.
    block* previous_free = nullptr;
    block* next_free     = nullptr;
  };
  static constexpr size_t start_alignment = alignof(block*);
  static constexpr size_t bits_per_byte   = std::numeric_limits<unsigned char>::digits;

  explicit paimon_admitted_pool(std::byte* starts, void* storage, size_t bytes)
    : live_starts_(starts), arena_start_(reinterpret_cast<uintptr_t>(storage)), arena_size_(bytes)
  {
    insert_free(::new (storage) block{nullptr, nullptr, bytes, 0});
  }

  static size_t size_class(size_t extent) noexcept { return std::bit_width(extent) - 1; }

  void insert_free(block* entry) noexcept
  {
    auto const group     = size_class(entry->extent);
    entry->previous_free = nullptr;
    entry->next_free     = free_lists_[group];
    if (entry->next_free) entry->next_free->previous_free = entry;
    free_lists_[group] = entry;
    nonempty_lists_ |= size_t{1} << group;
  }

  void remove_free(block* entry, size_t group) noexcept
  {
    if (entry->previous_free)
      entry->previous_free->next_free = entry->next_free;
    else
      free_lists_[group] = entry->next_free;
    if (entry->next_free) entry->next_free->previous_free = entry->previous_free;
    if (!free_lists_[group]) nonempty_lists_ &= ~(size_t{1} << group);
  }

  std::byte& start_byte(size_t slot) const noexcept { return live_starts_[slot / bits_per_byte]; }
  static std::byte start_mask(size_t slot) noexcept
  {
    return std::byte{1} << (slot % bits_per_byte);
  }
  static size_t normalize_alignment(uint64_t alignment)
  {
    // Preserve the selected SDK's default alignment, not a capacity constant.
    if (!alignment) alignment = 64;
    if ((alignment & (alignment - 1)) ||
        alignment > static_cast<uint64_t>(std::numeric_limits<ptrdiff_t>::max()))
      throw std::bad_alloc();
    return std::max(start_alignment, static_cast<size_t>(alignment));
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
    constexpr auto prefix = sizeof(block) + sizeof(block*);
    auto const minimum    = prefix + static_cast<size_t>(bytes);
    // Free extents are segregated by floor(log2(extent)). Skip empty lists and
    // those too small even without padding. Only free extents are inspected.
    auto groups = nonempty_lists_ & (~size_t{0} << size_class(minimum));
    while (groups) {
      auto const group = static_cast<size_t>(std::countr_zero(groups));
      groups &= groups - 1;
      for (auto* available = free_lists_[group]; available; available = available->next_free) {
        if (available->extent < minimum) continue;
        auto* start        = reinterpret_cast<std::byte*>(available);
        auto* after_header = start + prefix;
        auto const padding =
          (alignment - (reinterpret_cast<uintptr_t>(after_header) & (alignment - 1))) &
          (alignment - 1);
        if (padding > available->extent - minimum) continue;

        auto* data = after_header + padding;
        remove_free(available, group);
        auto const occupied = minimum + padding;
        void* tail          = start + occupied;
        auto tail_bytes     = available->extent - occupied;
        if (std::align(alignof(block), prefix + 1, tail, tail_bytes)) {
          auto* suffix = ::new (tail) block{available, available->next, tail_bytes, 0};
          if (suffix->next) suffix->next->previous = suffix;
          available->next = suffix;
          available->extent -= tail_bytes;
          insert_free(suffix);
        }
        available->requested = static_cast<size_t>(bytes);
        // The record is outside the payload. The bitmap certifies exact live
        // starts before lookup reads a record, even for an aligned interior pointer.
        std::memcpy(data - sizeof(block*), &available, sizeof(available));
        auto const slot = (reinterpret_cast<uintptr_t>(data) - arena_start_) / start_alignment;
        start_byte(slot) |= start_mask(slot);
        occupied_ += available->extent;
        peak_occupied_ = std::max(peak_occupied_, occupied_);
        peak_live_     = std::max(peak_live_, ++live_);
        return data;
      }
    }
    refuse();
  }

  block* find(void* pointer, uint64_t bytes) const noexcept
  {
    auto const address = reinterpret_cast<uintptr_t>(pointer);
    if (address < arena_start_) return nullptr;
    auto const offset = address - arena_start_;
    if (offset >= arena_size_ || offset % start_alignment) return nullptr;
    auto const slot = offset / start_alignment;
    if ((start_byte(slot) & start_mask(slot)) == std::byte{0}) return nullptr;
    block* entry;
    std::memcpy(&entry, static_cast<std::byte*>(pointer) - sizeof(entry), sizeof(entry));
    return entry->requested == bytes ? entry : nullptr;
  }

  void release(block* entry, void* pointer) noexcept
  {
    auto const slot = (reinterpret_cast<uintptr_t>(pointer) - arena_start_) / start_alignment;
    start_byte(slot) &= ~start_mask(slot);
    occupied_ -= entry->extent;
    --live_;
    entry->requested = 0;
    if (auto* next = entry->next; next && !next->requested) {
      remove_free(next, size_class(next->extent));
      entry->extent += next->extent;
      entry->next = next->next;
      if (entry->next) entry->next->previous = entry;
      next->~block();
    }
    if (auto* previous = entry->previous; previous && !previous->requested) {
      remove_free(previous, size_class(previous->extent));
      previous->extent += entry->extent;
      previous->next = entry->next;
      if (previous->next) previous->next->previous = previous;
      entry->~block();
      entry = previous;
    }
    insert_free(entry);
  }

  mutable std::mutex mutex_;
  std::byte* live_starts_;
  uintptr_t arena_start_;
  size_t arena_size_;
  std::array<block*, std::numeric_limits<size_t>::digits> free_lists_{};
  size_t nonempty_lists_ = 0;
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
  // Reserve one bit per possible aligned start, entirely within the charged
  // backing. Calculate from the remaining bytes before removing the bitmap
  // so it also covers every slot in the smaller, resulting arena.
  auto* starts            = static_cast<std::byte*>(storage);
  auto const coverage     = start_alignment * bits_per_byte;
  auto const bitmap_bytes = (bytes + coverage - 1) / coverage;
  storage                 = starts + bitmap_bytes;
  bytes -= bitmap_bytes;
  if (!std::align(alignof(block), sizeof(block) + sizeof(block*) + 1, storage, bytes))
    throw std::bad_alloc();
  std::memset(starts, 0, bitmap_bytes);
  auto* pool = ::new (object) paimon_admitted_pool(starts, storage, bytes);
  // Construction must finish while the caller still owns backing. Otherwise a
  // partially constructed object's unwinding could free its own base storage.
  pool->backing_ = std::move(backing);
  return {pool, destroy};
}
}  // namespace sirius::op::scan::detail
