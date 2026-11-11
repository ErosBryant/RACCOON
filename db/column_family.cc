//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/column_family.h"

#include <algorithm>
#include <cinttypes>
#include <cmath>
#include <limits>
#include <new>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "db/blob/blob_file_cache.h"
#include "db/blob/blob_file_partition_manager.h"
#include "db/blob/blob_source.h"
#include "db/compaction/compaction_picker.h"
#include "db/compaction/compaction_picker_fifo.h"
#include "db/compaction/compaction_picker_level.h"
#include "db/compaction/compaction_picker_universal.h"
#include "db/db_impl/db_impl.h"
#include "db/internal_stats.h"
#include "db/job_context.h"
#include "db/range_del_aggregator.h"
#include "db/table_properties_collector.h"
#include "db/version_set.h"
#include "db/write_controller.h"
#include "file/sst_file_manager_impl.h"
#include "logging/logging.h"
#include "monitoring/thread_status_util.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/convenience.h"
#include "rocksdb/table.h"
#include "table/merging_iterator.h"
#include "util/autovector.h"
#include "util/cast_util.h"
#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

class RACOption2Collector {
 public:
  static constexpr size_t kMaxPublicationShards = 16;
  static Status ValidateConfiguration(uint64_t window_size, int num_levels);

  static size_t ChooseShardCount() {
    size_t cpus = std::max<unsigned>(1, std::thread::hardware_concurrency());
    size_t shards = 1;
    while (shards * 2 <= cpus && shards * 2 <= kMaxPublicationShards) {
      shards *= 2;
    }
    return shards;
  }

  static uint64_t AllocateCollectorId() {
    uint64_t current = next_collector_id_.load(std::memory_order_relaxed);
    while (current != std::numeric_limits<uint64_t>::max()) {
      if (next_collector_id_.compare_exchange_weak(
              current, current + 1, std::memory_order_relaxed)) {
        return current;
      }
    }
    return 0;
  }

  RACOption2Collector(uint64_t window_size, int num_levels)
      : window_size_(window_size),
        num_levels_(num_levels),
        shard_count_(ChooseShardCount()),
        collector_id_(AllocateCollectorId()) {
    if (collector_id_ == 0) {
      initialization_status_ =
          Status::MemoryLimit("RAC option2 collector id space exhausted");
      return;
    }
    bool inject_allocation_failure = false;
    TEST_SYNC_POINT_CALLBACK("RACOption2Collector::BeforeAllocation",
                             &inject_allocation_failure);
    if (inject_allocation_failure) {
      initialization_status_ =
          Status::MemoryLimit("injected RAC option2 allocation failure");
      return;
    }
    const size_t bitmap_words =
        static_cast<size_t>((window_size_ + kBitmapBits - 1) / kBitmapBits);
    for (size_t i = 0; i < kRACEpochRingCapacity; ++i) {
      Slot& slot = slots_[i];
      slot.generation.store(i, std::memory_order_relaxed);
      slot.published.reset(
          new (std::nothrow) std::atomic<uint64_t>[bitmap_words]);
      slot.level_evidence.reset(
          new (std::nothrow)
              AtomicLevelEvidence[static_cast<size_t>(num_levels_)]);
      if (slot.published == nullptr || slot.level_evidence == nullptr) {
        initialization_status_ =
            Status::MemoryLimit("RAC option2 epoch allocation failed");
        return;
      }
      for (size_t word = 0; word < bitmap_words; ++word) {
        slot.published[word].store(0, std::memory_order_relaxed);
      }
      for (auto& view : slot.evidence_views) {
        view.store(nullptr, std::memory_order_relaxed);
      }
    }
    bitmap_words_ = bitmap_words;
    for (auto& view : registry_views_) {
      view.store(nullptr, std::memory_order_relaxed);
    }
    initialization_status_ = GrowRegistry();
    if (!initialization_status_.ok()) return;
    for (Slot& slot : slots_) ResetSlot(slot);
    std::lock_guard<std::mutex> lock(alive_collectors_mu_);
    alive_collectors_[this] = collector_id_;
  }

  ~RACOption2Collector() {
    std::lock_guard<std::mutex> lock(alive_collectors_mu_);
    alive_collectors_.erase(this);
  }

  const Status& initialization_status() const {
    return initialization_status_;
  }

  Status Reserve(RACOption2LookupRecord* record) {
    return Reserve(record, false, 0);
  }

  Status Reserve(RACOption2LookupRecord* record, uint64_t latest_sequence) {
    return Reserve(record, true, latest_sequence);
  }

  Status Reserve(RACOption2LookupRecord* record, bool track_write_sequence,
                 uint64_t latest_sequence) {
    if (record == nullptr) {
      return Status::InvalidArgument("null RAC option2 lookup record");
    }
    reserve_attempts_.fetch_add(1, std::memory_order_acq_rel);
    auto finish_reserve_attempt = [&] { FinishReserveAttempt(); };
    record->collector_ = nullptr;
    record->published_ = false;
    record->tracks_write_sequence_ = false;
    record->point_lookup_ = true;
    record->read_levels_ = 0;
    record->descent_levels_ = 0;
    for (;;) {
      if (stopping_.load(std::memory_order_acquire)) {
        finish_reserve_attempt();
        return Status::ShutdownInProgress("RAC option2 collector stopped");
      }
      if (exhausted_.load(std::memory_order_acquire)) {
        finish_reserve_attempt();
        return Status::Incomplete("RAC option2 ticket exhausted");
      }
      uint64_t ticket = next_ticket_.load(std::memory_order_relaxed);
      if (ticket == std::numeric_limits<uint64_t>::max()) {
        bool expected = false;
        if (!exhausted_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
          finish_reserve_attempt();
          return Status::Incomplete("RAC option2 ticket exhausted");
        }
        cv_.notify_all();
        std::unique_lock<std::mutex> lock(boundary_mu_);
        cv_.wait(lock, [&] {
          return outstanding_records_.load(std::memory_order_acquire) == 0 &&
                 reserve_attempts_.load(std::memory_order_acquire) == 1;
        });
        TerminalizePartialEpochs();
        cv_.notify_all();
        finish_reserve_attempt();
        return Status::Incomplete("RAC option2 ticket exhausted");
      }
      const uint64_t epoch = ticket / window_size_;
      const uint64_t ordinal = ticket % window_size_;
      Slot& slot = slots_[epoch % kRACEpochRingCapacity];
      if (slot.generation.load(std::memory_order_acquire) != epoch ||
          slot.state.load(std::memory_order_acquire) != SlotState::kCollecting) {
        reservation_stalls_.fetch_add(1, std::memory_order_relaxed);
        std::unique_lock<std::mutex> lock(boundary_mu_);
        cv_.wait(lock, [&] {
          return stopping_.load(std::memory_order_acquire) ||
                 exhausted_.load(std::memory_order_acquire) ||
                 (slot.generation.load(std::memory_order_acquire) == epoch &&
                  slot.state.load(std::memory_order_acquire) ==
                      SlotState::kCollecting);
        });
        continue;
      }
      if (!next_ticket_.compare_exchange_weak(ticket, ticket + 1,
                                              std::memory_order_acq_rel,
                                              std::memory_order_relaxed)) {
        continue;
      }
      record->collector_ = this;
      record->epoch_id_ = epoch;
      record->ordinal_ = ordinal;
      record->tracks_write_sequence_ = track_write_sequence;
      if (!track_write_sequence) {
        slot.write_sequence_tracking_complete.store(false,
                                                    std::memory_order_relaxed);
      } else if (ordinal == 0) {
        slot.write_sequence_at_start.store(latest_sequence,
                                           std::memory_order_relaxed);
      }
      slot.reservations.fetch_add(1, std::memory_order_relaxed);
      outstanding_records_.fetch_add(1, std::memory_order_release);
      finish_reserve_attempt();
      return Status::OK();
    }
  }

  Status RegisterFile(FileMetaData* file, int level) {
    if (file == nullptr || file->rac_option2_evidence_handle.collector_id ==
                               collector_id_) {
      return Status::OK();
    }
    std::lock_guard<std::mutex> lock(boundary_mu_);
    if (file->rac_option2_evidence_handle.collector_id == collector_id_) {
      return Status::OK();
    }
    ReclaimRetiredRegistryEntries();
    if (test_fail_registration_) {
      registration_exhausted_.store(true, std::memory_order_release);
      return Status::MemoryLimit(
          "injected RAC option2 evidence registry exhaustion");
    }
    uint32_t index;
    if (!free_registry_indices_.empty()) {
      index = free_registry_indices_.back();
      free_registry_indices_.pop_back();
    } else {
      index = next_registry_index_;
    }
    while (index >= registry_capacity_.load(std::memory_order_acquire)) {
      Status grow_status = GrowRegistry();
      if (!grow_status.ok()) {
        registration_exhausted_.store(true, std::memory_order_release);
        return grow_status;
      }
    }
    if (index == next_registry_index_) ++next_registry_index_;
    RegistryEntry& entry = RegistryAt(index);
    entry.file_number = file->fd.GetNumber();
    entry.file_size = file->fd.GetFileSize();
    uint32_t generation = entry.generation.load(std::memory_order_relaxed);
    if (generation == 0) generation = 1;
    entry.live = true;
    entry.generation.store(generation, std::memory_order_release);
    file->rac_option2_evidence_handle = {
        collector_id_, index, generation, this,
        &RACOption2Collector::ReleaseFileHandleThunk, true};
    (void)level;  // The observed level is recorded per epoch on the read path.
    return Status::OK();
  }

  RACOption2EvidenceHandle RegisterTestFile(uint64_t file_number,
                                            uint64_t file_size) {
    std::lock_guard<std::mutex> lock(boundary_mu_);
    auto found = test_handles_.find(file_number);
    if (found != test_handles_.end()) return found->second;
    const uint32_t index = next_registry_index_;
    while (index >= registry_capacity_.load(std::memory_order_acquire)) {
      Status grow_status = GrowRegistry();
      if (!grow_status.ok()) {
        registration_exhausted_.store(true, std::memory_order_release);
        return {};
      }
    }
    ++next_registry_index_;
    RegistryEntry& entry = RegistryAt(index);
    entry.file_number = file_number;
    entry.file_size = file_size;
    entry.generation.store(1, std::memory_order_release);
    RACOption2EvidenceHandle handle{collector_id_, index, 1, nullptr, nullptr,
                                    false};
    test_handles_.emplace(file_number, handle);
    return handle;
  }

  void RecordFile(uint64_t epoch, uint64_t ordinal,
                  const RACOption2EvidenceHandle& handle, int level,
                  int outcome,
                  uint64_t latency_nanos) {
    Slot& slot = slots_[epoch % kRACEpochRingCapacity];
    if (ordinal >= window_size_ ||
        slot.generation.load(std::memory_order_acquire) != epoch ||
        slot.state.load(std::memory_order_acquire) != SlotState::kCollecting) {
      invariant_violations_.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    if (!handle.valid() || handle.collector_id != collector_id_ ||
        handle.index >= registry_capacity_.load(std::memory_order_acquire) ||
        RegistryAt(handle.index).generation.load(std::memory_order_acquire) !=
            handle.generation) {
      slot.evidence_exhausted.store(true, std::memory_order_release);
      evidence_exhaustions_.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    AtomicFileEvidence& entry = EvidenceAt(slot, handle.index);
    int observed_level = entry.level.load(std::memory_order_relaxed);
    if (observed_level == -1) {
      entry.level.compare_exchange_strong(observed_level, level,
                                          std::memory_order_relaxed);
      observed_level = entry.level.load(std::memory_order_relaxed);
    }
    if (observed_level != level && observed_level >= 0) {
      entry.level.compare_exchange_strong(observed_level, -2,
                                          std::memory_order_relaxed);
      slot.evidence_exhausted.store(true, std::memory_order_release);
    }
    entry.touched.store(true, std::memory_order_release);
    const size_t lane_index = CurrentShard();
    AtomicLane& lane = entry.lanes[lane_index];
    if (outcome > 0) {
      SaturatingAdd(&lane.misses, 1, &slot.evidence_exhausted);
      SaturatingAdd(&lane.miss_latency_nanos, latency_nanos,
                    &slot.evidence_exhausted);
    } else if (outcome == 0) {
      SaturatingAdd(&lane.hits, 1, &slot.evidence_exhausted);
      SaturatingAdd(&lane.hit_latency_nanos, latency_nanos,
                    &slot.evidence_exhausted);
    }
  }

  void RecordLevel(uint64_t epoch, int level, bool miss) {
    if (level < 0 || level >= num_levels_) return;
    Slot& slot = slots_[epoch % kRACEpochRingCapacity];
    if (slot.generation.load(std::memory_order_acquire) != epoch ||
        slot.state.load(std::memory_order_acquire) != SlotState::kCollecting) {
      invariant_violations_.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    AtomicLevelEvidence& evidence =
        slot.level_evidence[static_cast<size_t>(level)];
    const size_t shard = CurrentShard();
    SaturatingAdd(miss ? &evidence.misses[shard] : &evidence.reads[shard], 1,
                  &slot.evidence_exhausted);
  }

  bool Publish(RACOption2LookupRecord* record, bool has_latest_sequence,
               uint64_t latest_sequence) {
    Slot& slot = slots_[record->epoch_id_ % kRACEpochRingCapacity];
    if (slot.generation.load(std::memory_order_acquire) != record->epoch_id_ ||
        slot.state.load(std::memory_order_acquire) != SlotState::kCollecting ||
        record->ordinal_ >= window_size_) {
      invariant_violations_.fetch_add(1, std::memory_order_relaxed);
      CompleteReservation();
      return false;
    }
    const size_t word = static_cast<size_t>(record->ordinal_ / kBitmapBits);
    const uint64_t bit = uint64_t{1} << (record->ordinal_ % kBitmapBits);
    const uint64_t old =
        slot.published[word].fetch_or(bit, std::memory_order_acq_rel);
    if ((old & bit) != 0) {
      invariant_violations_.fetch_add(1, std::memory_order_relaxed);
      CompleteReservation();
      return false;
    }
    if (!record->tracks_write_sequence_ || !has_latest_sequence) {
      slot.write_sequence_tracking_complete.store(false,
                                                  std::memory_order_relaxed);
    }
    if (record->point_lookup_) {
      slot.point_lookups.fetch_add(1, std::memory_order_relaxed);
    }
    const uint64_t completed =
        slot.completed.fetch_add(1, std::memory_order_acq_rel) + 1;
    if (completed != window_size_) {
      CompleteReservation();
      return false;
    }

    if (has_latest_sequence) {
      slot.write_sequence_at_end.store(latest_sequence,
                                       std::memory_order_relaxed);
    }

    slot.state.store(SlotState::kReady, std::memory_order_release);
    uint64_t observed = latest_completed_epoch_.load(std::memory_order_relaxed);
    while (observed < record->epoch_id_ &&
           !latest_completed_epoch_.compare_exchange_weak(
               observed, record->epoch_id_, std::memory_order_release,
               std::memory_order_relaxed)) {
    }
    const uint64_t backlog = ready_count_.fetch_add(1, std::memory_order_relaxed) + 1;
    uint64_t peak = peak_ready_backlog_.load(std::memory_order_relaxed);
    while (peak < backlog && !peak_ready_backlog_.compare_exchange_weak(
                                 peak, backlog, std::memory_order_relaxed)) {
    }
    cv_.notify_all();
    CompleteReservation();
    return true;
  }

  bool TakeNext(RACOption2EpochSnapshot* snapshot) {
    if (snapshot == nullptr) return false;
    std::lock_guard<std::mutex> lock(boundary_mu_);
    Slot& slot = slots_[next_evaluation_epoch_ % kRACEpochRingCapacity];
    if (slot.generation.load(std::memory_order_acquire) !=
            next_evaluation_epoch_ ||
        slot.state.load(std::memory_order_acquire) != SlotState::kReady) {
      return false;
    }
    snapshot->epoch_id = next_evaluation_epoch_;
    snapshot->total_reads = window_size_;
    snapshot->point_lookups =
        slot.point_lookups.load(std::memory_order_relaxed);
    const bool write_sequence_tracking_complete =
        slot.write_sequence_tracking_complete.load(std::memory_order_relaxed);
    const uint64_t write_sequence_at_start =
        slot.write_sequence_at_start.load(std::memory_order_relaxed);
    const uint64_t write_sequence_at_end =
        slot.write_sequence_at_end.load(std::memory_order_relaxed);
    snapshot->committed_write_count_known =
        snapshot->point_lookups == snapshot->total_reads &&
        write_sequence_tracking_complete &&
        write_sequence_at_end >= write_sequence_at_start;
    snapshot->committed_writes = snapshot->committed_write_count_known
                                     ? write_sequence_at_end -
                                           write_sequence_at_start
                                     : 0;
    snapshot->no_committed_writes =
        snapshot->committed_write_count_known &&
        snapshot->committed_writes == 0;
    snapshot->evidence_complete =
        !slot.evidence_exhausted.load(std::memory_order_acquire);
    snapshot->levels.assign(static_cast<size_t>(num_levels_), {});
    for (int level = 0; level < num_levels_; ++level) {
      for (size_t shard = 0; shard < shard_count_; ++shard) {
        snapshot->levels[static_cast<size_t>(level)].reads +=
            slot.level_evidence[static_cast<size_t>(level)].reads[shard].load(
                std::memory_order_relaxed);
        snapshot->levels[static_cast<size_t>(level)].misses +=
            slot.level_evidence[static_cast<size_t>(level)].misses[shard].load(
                std::memory_order_relaxed);
      }
    }
    snapshot->files.clear();
    if (snapshot->evidence_complete) {
      std::map<uint64_t, RACOption2FileEvidence> merged_files;
      for (size_t i = 0; i < next_registry_index_; ++i) {
        AtomicFileEvidence& source = EvidenceAt(slot, i);
        if (!source.touched.load(std::memory_order_acquire)) continue;
        const RegistryEntry& identity = RegistryAt(i);
        RACOption2FileEvidence result;
        result.file_number = identity.file_number;
        result.file_size = identity.file_size;
        result.level = source.level.load(std::memory_order_relaxed);
        if (result.level < 0) continue;  // Moved/conflicting evidence is stale.
        auto fold = [&](uint64_t* total, uint64_t value) {
          if (value > std::numeric_limits<uint64_t>::max() - *total) {
            snapshot->evidence_complete = false;
            saturated_counters_.fetch_add(1, std::memory_order_relaxed);
            return;
          }
          *total += value;
        };
        for (size_t lane = 0; lane < shard_count_; ++lane) {
          fold(&result.misses,
               source.lanes[lane].misses.load(std::memory_order_relaxed));
          fold(&result.hits,
               source.lanes[lane].hits.load(std::memory_order_relaxed));
          fold(&result.miss_latency_nanos,
               source.lanes[lane].miss_latency_nanos.load(
                   std::memory_order_relaxed));
          fold(&result.hit_latency_nanos,
               source.lanes[lane].hit_latency_nanos.load(
                   std::memory_order_relaxed));
        }
        if (!snapshot->evidence_complete) break;
        auto inserted = merged_files.emplace(result.file_number, result);
        if (!inserted.second) {
          RACOption2FileEvidence& existing = inserted.first->second;
          if (existing.level != result.level ||
              existing.file_size != result.file_size) {
            // A physical SST was observed through two Version-owned metadata
            // objects at incompatible locations. Reject the whole epoch rather
            // than allowing map insertion order to select its level.
            snapshot->evidence_complete = false;
            break;
          }
          fold(&existing.misses, result.misses);
          fold(&existing.hits, result.hits);
          fold(&existing.miss_latency_nanos, result.miss_latency_nanos);
          fold(&existing.hit_latency_nanos, result.hit_latency_nanos);
          if (!snapshot->evidence_complete) break;
        }
      }
      if (!snapshot->evidence_complete) {
        snapshot->files.clear();
      } else {
        snapshot->files.reserve(merged_files.size());
        for (const auto& entry : merged_files) {
          snapshot->files.push_back(entry.second);
        }
      }
    }
    slot.state.store(SlotState::kEvaluating, std::memory_order_release);
    ready_count_.fetch_sub(1, std::memory_order_relaxed);
    return true;
  }

  bool MarkPending(uint64_t epoch) {
    std::lock_guard<std::mutex> lock(boundary_mu_);
    if (epoch != next_evaluation_epoch_) return false;
    Slot& slot = slots_[epoch % kRACEpochRingCapacity];
    SlotState expected = SlotState::kEvaluating;
    return slot.generation.load(std::memory_order_acquire) == epoch &&
           slot.state.compare_exchange_strong(expected, SlotState::kPending,
                                              std::memory_order_release,
                                              std::memory_order_relaxed);
  }

  bool Finish(uint64_t epoch, RACOption2EpochTerminalState terminal_state) {
    if (terminal_state == RACOption2EpochTerminalState::kScheduled) {
      return MarkPending(epoch);
    }
    std::lock_guard<std::mutex> lock(boundary_mu_);
    if (epoch != next_evaluation_epoch_) return false;
    Slot& slot = slots_[epoch % kRACEpochRingCapacity];
    const SlotState state = slot.state.load(std::memory_order_acquire);
    if (slot.generation.load(std::memory_order_acquire) != epoch ||
        (state != SlotState::kEvaluating && state != SlotState::kPending)) {
      return false;
    }
    slot.terminal_state = terminal_state;
    ResetSlot(slot);
    if (epoch > std::numeric_limits<uint64_t>::max() - kRACEpochRingCapacity) {
      exhausted_.store(true, std::memory_order_release);
      slot.state.store(SlotState::kTerminal, std::memory_order_release);
    } else {
      slot.generation.store(epoch + kRACEpochRingCapacity,
                            std::memory_order_release);
      slot.state.store(SlotState::kCollecting, std::memory_order_release);
    }
    ++next_evaluation_epoch_;
    ReclaimRetiredRegistryEntries();
    cv_.notify_all();
    return true;
  }

  void TEST_SetNextTicket(uint64_t ticket) {
    next_ticket_.store(ticket, std::memory_order_release);
  }

  void TEST_SetRegistrationFailure(bool fail) {
    std::lock_guard<std::mutex> lock(boundary_mu_);
    test_fail_registration_ = fail;
  }

  void Stop() {
    stopping_.store(true, std::memory_order_release);
    cv_.notify_all();
    std::unique_lock<std::mutex> lock(boundary_mu_);
    cv_.wait(lock, [&] {
      return outstanding_records_.load(std::memory_order_acquire) == 0 &&
             reserve_attempts_.load(std::memory_order_acquire) == 0;
    });
    TerminalizeAllEpochsForStop();
    cv_.notify_all();
  }

  uint64_t latest_completed_epoch() const {
    return latest_completed_epoch_.load(std::memory_order_acquire);
  }

  RACOption2CollectorStats GetStats() const {
    RACOption2CollectorStats stats;
    stats.reservation_stalls = reservation_stalls_.load(std::memory_order_relaxed);
    stats.invariant_violations =
        invariant_violations_.load(std::memory_order_relaxed);
    stats.evidence_exhaustions =
        evidence_exhaustions_.load(std::memory_order_relaxed);
    stats.saturated_counters =
        saturated_counters_.load(std::memory_order_relaxed);
    stats.partial_epochs_terminalized =
        partial_epochs_terminalized_.load(std::memory_order_relaxed);
    stats.ready_backlog = ready_count_.load(std::memory_order_relaxed);
    stats.peak_ready_backlog =
        peak_ready_backlog_.load(std::memory_order_relaxed);
    const size_t registry_capacity =
        registry_capacity_.load(std::memory_order_acquire);
    stats.memory_bytes =
        sizeof(*this) + registry_capacity * sizeof(RegistryEntry) +
        kRACEpochRingCapacity *
            (registry_capacity * sizeof(AtomicFileEvidence) +
             bitmap_words_ * sizeof(std::atomic<uint64_t>) +
             static_cast<size_t>(num_levels_) * sizeof(AtomicLevelEvidence));
    return stats;
  }

 private:
  size_t CurrentShard() {
    struct ThreadShardToken {
      uint64_t collector_id = 0;
      size_t shard = 0;
    };
    static thread_local ThreadShardToken token;
    if (token.collector_id != collector_id_) {
      token.collector_id = collector_id_;
      token.shard = next_shard_.fetch_add(1, std::memory_order_relaxed) &
                    (shard_count_ - 1);
    }
    return token.shard;
  }

  static constexpr uint64_t kBitmapBits = 64;
  static constexpr size_t kRegistrySlabSize = 1024;
  static constexpr size_t kMaxRegistrySlabs = 48;
  enum class SlotState : uint8_t {
    kCollecting,
    kReady,
    kEvaluating,
    kPending,
    kTerminal
  };
  struct AtomicLane {
    std::atomic<uint64_t> misses{0};
    std::atomic<uint64_t> hits{0};
    std::atomic<uint64_t> miss_latency_nanos{0};
    std::atomic<uint64_t> hit_latency_nanos{0};
  };
  struct RegistryEntry {
    std::atomic<uint32_t> generation{0};
    uint64_t file_number = 0;
    uint64_t file_size = 0;
    bool live = false;
    uint64_t retire_after_epoch = 0;
  };

  static void ReleaseFileHandleThunk(void* owner, uint64_t collector_id,
                                     uint32_t index, uint32_t generation) {
    std::lock_guard<std::mutex> lock(alive_collectors_mu_);
    auto it = alive_collectors_.find(owner);
    if (it == alive_collectors_.end() || it->second != collector_id) return;
    static_cast<RACOption2Collector*>(owner)->ReleaseFileHandle(index,
                                                                generation);
  }

  void ReleaseFileHandle(uint32_t index, uint32_t generation) {
    std::lock_guard<std::mutex> lock(boundary_mu_);
    if (index >= next_registry_index_) return;
    RegistryEntry& entry = RegistryAt(index);
    if (!entry.live ||
        entry.generation.load(std::memory_order_acquire) != generation) {
      return;
    }
    entry.live = false;
    bool referenced_by_epoch = false;
    for (const Slot& slot : slots_) {
      if (EvidenceAt(slot, index).touched.load(std::memory_order_acquire)) {
        referenced_by_epoch = true;
        break;
      }
    }
    if (!referenced_by_epoch &&
        outstanding_records_.load(std::memory_order_acquire) == 0) {
      FinalizeRegistryRetirement(index);
      return;
    }
    const uint64_t next_ticket =
        next_ticket_.load(std::memory_order_acquire);
    // next_ticket is the first unreserved ticket. At an exact W boundary the
    // last epoch that can still reference this handle is therefore the prior
    // epoch, not the newly opened empty epoch.
    entry.retire_after_epoch =
        next_ticket == 0 ? 0 : (next_ticket - 1) / window_size_;
    retired_registry_indices_.push_back(index);
  }

  // REQUIRES boundary_mu_. A slot is reusable only after every epoch that
  // could have observed its old identity has retired.
  void ReclaimRetiredRegistryEntries() {
    auto out = retired_registry_indices_.begin();
    for (auto it = retired_registry_indices_.begin();
         it != retired_registry_indices_.end(); ++it) {
      RegistryEntry& entry = RegistryAt(*it);
      if (next_evaluation_epoch_ > entry.retire_after_epoch) {
        FinalizeRegistryRetirement(*it);
      } else {
        *out++ = *it;
      }
    }
    retired_registry_indices_.erase(out, retired_registry_indices_.end());
  }

  void FinalizeRegistryRetirement(uint32_t index) {
    RegistryEntry& entry = RegistryAt(index);
    const uint32_t generation =
        entry.generation.load(std::memory_order_relaxed);
    if (generation == std::numeric_limits<uint32_t>::max()) {
      registration_exhausted_.store(true, std::memory_order_release);
      return;
    }
    entry.file_number = 0;
    entry.file_size = 0;
    entry.generation.store(generation + 1, std::memory_order_release);
    free_registry_indices_.push_back(index);
  }
  struct AtomicFileEvidence {
    std::atomic<int> level{-1};
    std::atomic<bool> touched{false};
    AtomicLane lanes[kMaxPublicationShards];
  };
  struct AtomicLevelEvidence {
    std::atomic<uint64_t> reads[kMaxPublicationShards];
    std::atomic<uint64_t> misses[kMaxPublicationShards];
  };
  struct Slot {
    std::atomic<uint64_t> generation{0};
    std::atomic<SlotState> state{SlotState::kCollecting};
    RACOption2EpochTerminalState terminal_state =
        RACOption2EpochTerminalState::kFailed;
    std::atomic<uint64_t> reservations{0};
    std::atomic<uint64_t> completed{0};
    std::atomic<uint64_t> point_lookups{0};
    std::atomic<bool> evidence_exhausted{false};
    std::atomic<bool> write_sequence_tracking_complete{true};
    std::atomic<uint64_t> write_sequence_at_start{0};
    std::atomic<uint64_t> write_sequence_at_end{0};
    std::unique_ptr<std::atomic<uint64_t>[]> published;
    std::array<std::unique_ptr<AtomicFileEvidence[]>, kMaxRegistrySlabs>
        evidence_owners;
    std::array<std::atomic<AtomicFileEvidence*>, kMaxRegistrySlabs>
        evidence_views;
    std::unique_ptr<AtomicLevelEvidence[]> level_evidence;
  };

  RegistryEntry& RegistryAt(size_t index) const {
    RegistryEntry* slab = registry_views_[index / kRegistrySlabSize].load(
        std::memory_order_acquire);
    assert(slab != nullptr);
    return slab[index % kRegistrySlabSize];
  }

  AtomicFileEvidence& EvidenceAt(const Slot& slot, size_t index) const {
    AtomicFileEvidence* slab =
        slot.evidence_views[index / kRegistrySlabSize].load(
            std::memory_order_acquire);
    assert(slab != nullptr);
    return slab[index % kRegistrySlabSize];
  }

  Status GrowRegistry() {
    const size_t slab_index =
        registry_capacity_.load(std::memory_order_relaxed) /
        kRegistrySlabSize;
    if (slab_index >= kMaxRegistrySlabs) {
      return Status::MemoryLimit(
          "RAC option2 evidence registry reached memory budget");
    }
    std::unique_ptr<RegistryEntry[]> registry_slab(
        new (std::nothrow) RegistryEntry[kRegistrySlabSize]);
    std::array<std::unique_ptr<AtomicFileEvidence[]>,
               kRACEpochRingCapacity>
        evidence_slabs;
    if (registry_slab == nullptr) {
      return Status::MemoryLimit("RAC option2 registry slab allocation failed");
    }
    for (auto& evidence_slab : evidence_slabs) {
      evidence_slab.reset(
          new (std::nothrow) AtomicFileEvidence[kRegistrySlabSize]);
      if (evidence_slab == nullptr) {
        return Status::MemoryLimit(
            "RAC option2 evidence slab allocation failed");
      }
    }
    registry_owners_[slab_index] = std::move(registry_slab);
    for (size_t slot_index = 0; slot_index < kRACEpochRingCapacity;
         ++slot_index) {
      Slot& slot = slots_[slot_index];
      slot.evidence_owners[slab_index] =
          std::move(evidence_slabs[slot_index]);
      slot.evidence_views[slab_index].store(
          slot.evidence_owners[slab_index].get(), std::memory_order_release);
    }
    registry_views_[slab_index].store(registry_owners_[slab_index].get(),
                                      std::memory_order_release);
    registry_capacity_.fetch_add(kRegistrySlabSize, std::memory_order_release);
    return Status::OK();
  }

  void SaturatingAdd(std::atomic<uint64_t>* counter, uint64_t delta,
                     std::atomic<bool>* epoch_invalid) {
    uint64_t old = counter->load(std::memory_order_relaxed);
    for (;;) {
      const uint64_t next = SaturatingSum(old, delta);
      const bool saturated =
          next == std::numeric_limits<uint64_t>::max() && old != next;
      if (counter->compare_exchange_weak(old, next, std::memory_order_relaxed)) {
        if (saturated) {
          saturated_counters_.fetch_add(1, std::memory_order_relaxed);
          epoch_invalid->store(true, std::memory_order_release);
        }
        return;
      }
    }
  }

  static uint64_t SaturatingSum(uint64_t lhs, uint64_t rhs) {
    return rhs > std::numeric_limits<uint64_t>::max() - lhs
               ? std::numeric_limits<uint64_t>::max()
               : lhs + rhs;
  }

  void CompleteReservation() {
    outstanding_records_.fetch_sub(1, std::memory_order_acq_rel);
    cv_.notify_all();
  }

  void FinishReserveAttempt() {
    reserve_attempts_.fetch_sub(1, std::memory_order_acq_rel);
    cv_.notify_all();
  }

  // REQUIRES boundary_mu_. No reserved lookup may remain.
  void TerminalizePartialEpochs() {
    for (Slot& slot : slots_) {
      if (slot.state.load(std::memory_order_acquire) == SlotState::kCollecting &&
          slot.reservations.load(std::memory_order_acquire) != 0) {
        slot.terminal_state = RACOption2EpochTerminalState::kFailed;
        slot.evidence_exhausted.store(true, std::memory_order_release);
        slot.state.store(SlotState::kTerminal, std::memory_order_release);
        partial_epochs_terminalized_.fetch_add(1,
                                               std::memory_order_relaxed);
      }
    }
  }

  // REQUIRES boundary_mu_. Collector ownership is ending, so all queued,
  // evaluating, and pending generations are terminal failures as well.
  void TerminalizeAllEpochsForStop() {
    for (Slot& slot : slots_) {
      const SlotState state = slot.state.load(std::memory_order_acquire);
      if (state == SlotState::kTerminal) continue;
      if (state == SlotState::kReady) {
        ready_count_.fetch_sub(1, std::memory_order_relaxed);
      }
      if (state != SlotState::kCollecting ||
          slot.reservations.load(std::memory_order_acquire) != 0) {
        partial_epochs_terminalized_.fetch_add(1,
                                               std::memory_order_relaxed);
      }
      slot.terminal_state = RACOption2EpochTerminalState::kFailed;
      slot.evidence_exhausted.store(true, std::memory_order_release);
      slot.state.store(SlotState::kTerminal, std::memory_order_release);
    }
  }

  void ResetSlot(Slot& slot) {
    const size_t registry_capacity =
        registry_capacity_.load(std::memory_order_acquire);
    for (size_t i = 0; i < registry_capacity; ++i) {
      AtomicFileEvidence& entry = EvidenceAt(slot, i);
      entry.touched.store(false, std::memory_order_relaxed);
      for (size_t lane = 0; lane < shard_count_; ++lane) {
        entry.lanes[lane].misses.store(0, std::memory_order_relaxed);
        entry.lanes[lane].hits.store(0, std::memory_order_relaxed);
        entry.lanes[lane].miss_latency_nanos.store(0,
                                                   std::memory_order_relaxed);
        entry.lanes[lane].hit_latency_nanos.store(0,
                                                  std::memory_order_relaxed);
      }
      entry.level.store(-1, std::memory_order_relaxed);
    }
    for (size_t word = 0; word < bitmap_words_; ++word) {
      slot.published[word].store(0, std::memory_order_relaxed);
    }
    for (int level = 0; level < num_levels_; ++level) {
      for (size_t shard = 0; shard < shard_count_; ++shard) {
        slot.level_evidence[static_cast<size_t>(level)].reads[shard].store(
            0, std::memory_order_relaxed);
        slot.level_evidence[static_cast<size_t>(level)].misses[shard].store(
            0, std::memory_order_relaxed);
      }
    }
    slot.evidence_exhausted.store(false, std::memory_order_relaxed);
    slot.write_sequence_tracking_complete.store(true,
                                                std::memory_order_relaxed);
    slot.write_sequence_at_start.store(0, std::memory_order_relaxed);
    slot.write_sequence_at_end.store(0, std::memory_order_relaxed);
    slot.completed.store(0, std::memory_order_relaxed);
    slot.point_lookups.store(0, std::memory_order_relaxed);
    slot.reservations.store(0, std::memory_order_relaxed);
  }

  const uint64_t window_size_;
  const int num_levels_;
  const size_t shard_count_;
  std::atomic<size_t> next_shard_{0};
  std::atomic<size_t> registry_capacity_{0};
  size_t bitmap_words_ = 0;
  const uint64_t collector_id_;
  uint32_t next_registry_index_ = 0;
  std::vector<uint32_t> free_registry_indices_;
  std::vector<uint32_t> retired_registry_indices_;
  std::array<std::unique_ptr<RegistryEntry[]>, kMaxRegistrySlabs>
      registry_owners_;
  std::array<std::atomic<RegistryEntry*>, kMaxRegistrySlabs> registry_views_;
  std::unordered_map<uint64_t, RACOption2EvidenceHandle> test_handles_;
  static std::atomic<uint64_t> next_collector_id_;
  static std::mutex alive_collectors_mu_;
  static std::unordered_map<void*, uint64_t> alive_collectors_;
  mutable std::mutex boundary_mu_;
  std::condition_variable cv_;
  Slot slots_[kRACEpochRingCapacity];
  std::atomic<uint64_t> next_ticket_{0};
  std::atomic<uint64_t> latest_completed_epoch_{0};
  uint64_t next_evaluation_epoch_ = 0;
  std::atomic<uint64_t> reservation_stalls_{0};
  std::atomic<uint64_t> invariant_violations_{0};
  std::atomic<uint64_t> evidence_exhaustions_{0};
  std::atomic<uint64_t> saturated_counters_{0};
  std::atomic<uint64_t> ready_count_{0};
  std::atomic<uint64_t> peak_ready_backlog_{0};
  std::atomic<uint64_t> outstanding_records_{0};
  std::atomic<uint64_t> reserve_attempts_{0};
  std::atomic<uint64_t> partial_epochs_terminalized_{0};
  std::atomic<bool> stopping_{false};
  std::atomic<bool> exhausted_{false};
  std::atomic<bool> registration_exhausted_{false};
  bool test_fail_registration_ = false;
  Status initialization_status_;
};

Status RACOption2Collector::ValidateConfiguration(uint64_t window_size,
                                                  int num_levels) {
  if (window_size == 0 || num_levels <= 0) {
    return Status::InvalidArgument("invalid RAC option2 collector dimensions");
  }
  const size_t registry_capacity =
      kRegistrySlabSize * kMaxRegistrySlabs;
  const size_t bitmap_words =
      static_cast<size_t>((window_size + kBitmapBits - 1) / kBitmapBits);
  size_t bytes = sizeof(RACOption2Collector);
  auto add_product = [&](size_t count, size_t width) {
    if (count != 0 && width > std::numeric_limits<size_t>::max() / count) {
      return false;
    }
    const size_t product = count * width;
    if (product > std::numeric_limits<size_t>::max() - bytes) return false;
    bytes += product;
    return true;
  };
  if (!add_product(registry_capacity, sizeof(RegistryEntry)) ||
      !add_product(kRACEpochRingCapacity * registry_capacity,
                   sizeof(AtomicFileEvidence)) ||
      !add_product(kRACEpochRingCapacity * bitmap_words,
                   sizeof(std::atomic<uint64_t>)) ||
      !add_product(kRACEpochRingCapacity * static_cast<size_t>(num_levels),
                   sizeof(AtomicLevelEvidence))) {
    return Status::MemoryLimit("RAC option2 collector size overflow");
  }
  // Reserve 8 MiB of the 128 MiB per-CF total for bounded cooldown ranges.
  constexpr size_t kPerColumnFamilyBudget = size_t{120} * 1024 * 1024;
  if (bytes > kPerColumnFamilyBudget) {
    return Status::MemoryLimit("RAC option2 collector exceeds memory budget");
  }
  return Status::OK();
}

std::atomic<uint64_t> RACOption2Collector::next_collector_id_{1};
std::mutex RACOption2Collector::alive_collectors_mu_;
std::unordered_map<void*, uint64_t> RACOption2Collector::alive_collectors_;

RACOption2LookupRecord::~RACOption2LookupRecord() { Finish(); }

void RACOption2LookupRecord::RecordFileMiss(
    const RACOption2EvidenceHandle& handle, int level,
    uint64_t latency_nanos) {
  if (valid()) {
    collector_->RecordFile(epoch_id_, ordinal_, handle, level, 1,
                           latency_nanos);
  }
}

void RACOption2LookupRecord::RecordFileHit(
    const RACOption2EvidenceHandle& handle, int level) {
  RecordFileHit(handle, level, 0);
}

void RACOption2LookupRecord::RecordFileHit(
    const RACOption2EvidenceHandle& handle, int level,
    uint64_t latency_nanos) {
  if (valid()) {
    collector_->RecordFile(epoch_id_, ordinal_, handle, level, 0,
                           latency_nanos);
  }
}

void RACOption2LookupRecord::RecordFileObserved(
    const RACOption2EvidenceHandle& handle, int level) {
  if (valid()) collector_->RecordFile(epoch_id_, ordinal_, handle, level, -1, 0);
}

void RACOption2LookupRecord::RecordFileMiss(uint64_t file_number,
                                            uint64_t file_size, int level,
                                            uint64_t latency_nanos) {
  if (valid()) {
    const auto handle = collector_->RegisterTestFile(file_number, file_size);
    RecordFileMiss(handle, level, latency_nanos);
  }
}

void RACOption2LookupRecord::RecordFileHit(uint64_t file_number,
                                           uint64_t file_size, int level) {
  RecordFileHit(file_number, file_size, level, 0);
}

void RACOption2LookupRecord::RecordFileHit(uint64_t file_number,
                                           uint64_t file_size, int level,
                                           uint64_t latency_nanos) {
  if (valid()) {
    const auto handle = collector_->RegisterTestFile(file_number, file_size);
    RecordFileHit(handle, level, latency_nanos);
  }
}

void RACOption2LookupRecord::RecordFileObserved(uint64_t file_number,
                                                uint64_t file_size,
                                                int level) {
  if (valid()) {
    const auto handle = collector_->RegisterTestFile(file_number, file_size);
    RecordFileObserved(handle, level);
  }
}

void RACOption2LookupRecord::RecordLevelRead(int level) {
  if (!valid() || level < 0 || level >= 64) return;
  const uint64_t bit = uint64_t{1} << static_cast<unsigned>(level);
  if ((read_levels_ & bit) == 0) {
    read_levels_ |= bit;
  }
}

void RACOption2LookupRecord::RecordLevelDescent(int level) {
  if (!valid() || level < 0 || level >= 64) return;
  const uint64_t bit = uint64_t{1} << static_cast<unsigned>(level);
  if ((descent_levels_ & bit) == 0) {
    descent_levels_ |= bit;
  }
}

bool RACOption2LookupRecord::Finish() { return FinishInternal(false, 0); }

bool RACOption2LookupRecord::Finish(uint64_t latest_sequence) {
  return FinishInternal(true, latest_sequence);
}

bool RACOption2LookupRecord::FinishInternal(bool has_latest_sequence,
                                            uint64_t latest_sequence) {
  if (!valid() || published_) return false;
  published_ = true;
  for (int level = 0; level < 64; ++level) {
    const uint64_t bit = uint64_t{1} << static_cast<unsigned>(level);
    if ((read_levels_ & bit) != 0) {
      collector_->RecordLevel(epoch_id_, level, false);
    }
    if ((descent_levels_ & bit) != 0) {
      collector_->RecordLevel(epoch_id_, level, true);
    }
  }
  return collector_->Publish(this, has_latest_sequence, latest_sequence);
}

ColumnFamilyHandleImpl::ColumnFamilyHandleImpl(
    ColumnFamilyData* column_family_data, DBImpl* db, InstrumentedMutex* mutex)
    : cfd_(column_family_data), db_(db), mutex_(mutex) {
  if (cfd_ != nullptr) {
    cfd_->Ref();
  }
}

ColumnFamilyHandleImpl::~ColumnFamilyHandleImpl() {
  if (cfd_ != nullptr) {
    for (auto& listener : cfd_->ioptions().listeners) {
      listener->OnColumnFamilyHandleDeletionStarted(this);
    }
    // Job id == 0 means that this is not our background process, but rather
    // user thread
    // Need to hold some shared pointers owned by the initial_cf_options
    // before final cleaning up finishes.
    ColumnFamilyOptions initial_cf_options_copy = cfd_->initial_cf_options();
    JobContext job_context(0);
    mutex_->Lock();
    bool dropped = cfd_->IsDropped();
    if (cfd_->UnrefAndTryDelete()) {
      if (dropped) {
        db_->FindObsoleteFiles(&job_context, false, true);
      }
    }
    mutex_->Unlock();
    if (job_context.HaveSomethingToDelete()) {
      bool defer_purge =
          db_->immutable_db_options().avoid_unnecessary_blocking_io;
      db_->PurgeObsoleteFiles(job_context, defer_purge);
    }
    job_context.Clean();
  }
}

uint32_t ColumnFamilyHandleImpl::GetID() const { return cfd()->GetID(); }

const std::string& ColumnFamilyHandleImpl::GetName() const {
  return cfd()->GetName();
}

Status ColumnFamilyHandleImpl::GetDescriptor(ColumnFamilyDescriptor* desc) {
  // accessing mutable cf-options requires db mutex.
  InstrumentedMutexLock l(mutex_);
  *desc = ColumnFamilyDescriptor(cfd()->GetName(), cfd()->GetLatestCFOptions());
  return Status::OK();
}

const Comparator* ColumnFamilyHandleImpl::GetComparator() const {
  return cfd()->user_comparator();
}

void GetInternalTblPropCollFactory(
    const ImmutableCFOptions& ioptions,
    InternalTblPropCollFactories* internal_tbl_prop_coll_factories) {
  assert(internal_tbl_prop_coll_factories);

  auto& collector_factories = ioptions.table_properties_collector_factories;
  for (size_t i = 0; i < ioptions.table_properties_collector_factories.size();
       ++i) {
    assert(collector_factories[i]);
    internal_tbl_prop_coll_factories->emplace_back(
        new UserKeyTablePropertiesCollectorFactory(collector_factories[i]));
  }
}

Status CheckCompressionSupportedWithManager(
    CompressionType type, UnownedPtr<CompressionManager> mgr) {
  if (mgr) {
    if (!mgr->SupportsCompressionType(type)) {
      return Status::NotSupported("Compression type " +
                                  CompressionTypeToString(type) +
                                  " is not recognized/supported by this "
                                  "version of CompressionManager " +
                                  mgr->GetId());
    }
  } else {
    if (!CompressionTypeSupported(type)) {
      if (type <= kLastBuiltinCompression) {
        return Status::InvalidArgument("Compression type " +
                                       CompressionTypeToString(type) +
                                       " is not linked with the binary.");
      } else {
        return Status::NotSupported(
            "Compression type " + CompressionTypeToString(type) +
            " is not recognized/supported by built-in CompressionManager.");
      }
    }
  }
  return Status::OK();
}

Status CheckCompressionSupported(const ColumnFamilyOptions& cf_options) {
  if (!cf_options.compression_per_level.empty()) {
    for (size_t level = 0; level < cf_options.compression_per_level.size();
         ++level) {
      Status s = CheckCompressionSupportedWithManager(
          cf_options.compression_per_level[level],
          cf_options.compression_manager.get());
      if (!s.ok()) {
        return s;
      }
    }
  } else {
    Status s = CheckCompressionSupportedWithManager(
        cf_options.compression, cf_options.compression_manager.get());
    if (!s.ok()) {
      return s;
    }
  }
  if (cf_options.compression_opts.zstd_max_train_bytes > 0) {
    if (cf_options.compression_opts.use_zstd_dict_trainer) {
      if (!ZSTD_TrainDictionarySupported()) {
        return Status::InvalidArgument(
            "zstd dictionary trainer cannot be used because ZSTD 1.1.3+ "
            "is not linked with the binary.");
      }
    } else if (!ZSTD_FinalizeDictionarySupported()) {
      return Status::InvalidArgument(
          "zstd finalizeDictionary cannot be used because ZSTD 1.4.5+ "
          "is not linked with the binary.");
    }
    if (cf_options.compression_opts.max_dict_bytes == 0) {
      return Status::InvalidArgument(
          "The dictionary size limit (`CompressionOptions::max_dict_bytes`) "
          "should be nonzero if we're using zstd's dictionary generator.");
    }
  }

  if (!CompressionTypeSupported(cf_options.blob_compression_type)) {
    std::ostringstream oss;
    oss << "The specified blob compression type "
        << CompressionTypeToString(cf_options.blob_compression_type)
        << " is not available.";

    return Status::InvalidArgument(oss.str());
  }

  return Status::OK();
}

Status CheckConcurrentWritesSupported(const ColumnFamilyOptions& cf_options) {
  if (cf_options.inplace_update_support) {
    return Status::InvalidArgument(
        "In-place memtable updates (inplace_update_support) is not compatible "
        "with concurrent writes (allow_concurrent_memtable_write)");
  }
  if (!cf_options.memtable_factory->IsInsertConcurrentlySupported()) {
    return Status::InvalidArgument(
        "Memtable doesn't allow concurrent writes "
        "(allow_concurrent_memtable_write)");
  }
  return Status::OK();
}

Status CheckCFPathsSupported(const DBOptions& db_options,
                             const ColumnFamilyOptions& cf_options) {
  // More than one cf_paths are supported only in universal
  // and level compaction styles. This function also checks the case
  // in which cf_paths is not specified, which results in db_paths
  // being used.
  if ((cf_options.compaction_style != kCompactionStyleUniversal) &&
      (cf_options.compaction_style != kCompactionStyleLevel)) {
    if (cf_options.cf_paths.size() > 1) {
      return Status::NotSupported(
          "More than one CF paths are only supported in "
          "universal and level compaction styles. ");
    } else if (cf_options.cf_paths.empty() && db_options.db_paths.size() > 1) {
      return Status::NotSupported(
          "More than one DB paths are only supported in "
          "universal and level compaction styles. ");
    }
  }
  return Status::OK();
}

namespace {
const uint64_t kDefaultTtl = 0xfffffffffffffffe;
const uint64_t kDefaultPeriodicCompSecs = 0xfffffffffffffffe;
}  // anonymous namespace

ColumnFamilyOptions SanitizeCfOptions(const ImmutableDBOptions& db_options,
                                      bool read_only,
                                      const ColumnFamilyOptions& src) {
  ColumnFamilyOptions result = src;
  size_t clamp_max = std::conditional<
      sizeof(size_t) == 4, std::integral_constant<size_t, 0xffffffff>,
      std::integral_constant<uint64_t, 64ull << 30>>::type::value;
  ClipToRange(&result.write_buffer_size, (static_cast<size_t>(64)) << 10,
              clamp_max);
  // if user sets arena_block_size, we trust user to use this value. Otherwise,
  // calculate a proper value from writer_buffer_size;
  if (result.arena_block_size <= 0) {
    result.arena_block_size =
        std::min(size_t{1024 * 1024}, result.write_buffer_size / 8);

    // Align up to 4k
    const size_t align = 4 * 1024;
    result.arena_block_size =
        ((result.arena_block_size + align - 1) / align) * align;
  }
  result.min_write_buffer_number_to_merge =
      std::min(result.min_write_buffer_number_to_merge,
               result.max_write_buffer_number - 1);
  if (result.min_write_buffer_number_to_merge < 1) {
    result.min_write_buffer_number_to_merge = 1;
  }

  if (db_options.atomic_flush && result.min_write_buffer_number_to_merge > 1) {
    ROCKS_LOG_WARN(
        db_options.logger,
        "Currently, if atomic_flush is true, then triggering flush for any "
        "column family internally (non-manual flush) will trigger flushing "
        "all column families even if the number of memtables is smaller "
        "min_write_buffer_number_to_merge. Therefore, configuring "
        "min_write_buffer_number_to_merge > 1 is not compatible and should "
        "be satinized to 1. Not doing so will lead to data loss and "
        "inconsistent state across multiple column families when WAL is "
        "disabled, which is a common setting for atomic flush");

    result.min_write_buffer_number_to_merge = 1;
  }
  if (result.disallow_memtable_writes) {
    // A simple memtable that enforces MarkReadOnly (unlike skip list)
    result.memtable_factory = std::make_shared<VectorRepFactory>();
  }

  if (result.num_levels < 1) {
    result.num_levels = 1;
  }
  if (result.compaction_style == kCompactionStyleLevel &&
      result.num_levels < 2) {
    result.num_levels = 2;
  }

  if (result.compaction_style == kCompactionStyleUniversal &&
      (db_options.allow_ingest_behind || result.cf_allow_ingest_behind) &&
      result.num_levels < 3) {
    result.num_levels = 3;
  }

  if (result.max_write_buffer_number < 2) {
    result.max_write_buffer_number = 2;
  }
  if (result.max_write_buffer_size_to_maintain < 0) {
    result.max_write_buffer_size_to_maintain =
        result.max_write_buffer_number *
        static_cast<int64_t>(result.write_buffer_size);
  }
  // bloom filter size shouldn't exceed 1/4 of memtable size.
  if (result.memtable_prefix_bloom_size_ratio > 0.25) {
    result.memtable_prefix_bloom_size_ratio = 0.25;
  } else if (result.memtable_prefix_bloom_size_ratio < 0) {
    result.memtable_prefix_bloom_size_ratio = 0;
  }

  if (!result.prefix_extractor) {
    assert(result.memtable_factory);
    Slice name = result.memtable_factory->Name();
    if (name.compare("HashSkipListRepFactory") == 0 ||
        name.compare("HashLinkListRepFactory") == 0) {
      result.memtable_factory = std::make_shared<SkipListFactory>();
    }
  }

  if (result.compaction_style == kCompactionStyleFIFO) {
    // since we delete level0 files in FIFO compaction when there are too many
    // of them, these options don't really mean anything
    result.level0_slowdown_writes_trigger = std::numeric_limits<int>::max();
    result.level0_stop_writes_trigger = std::numeric_limits<int>::max();
  }

  if (result.max_bytes_for_level_multiplier <= 0) {
    result.max_bytes_for_level_multiplier = 1;
  }

  if (result.level0_file_num_compaction_trigger == 0) {
    ROCKS_LOG_WARN(db_options.logger,
                   "level0_file_num_compaction_trigger cannot be 0");
    result.level0_file_num_compaction_trigger = 1;
  }

  if (result.level0_stop_writes_trigger <
          result.level0_slowdown_writes_trigger ||
      result.level0_slowdown_writes_trigger <
          result.level0_file_num_compaction_trigger) {
    ROCKS_LOG_WARN(db_options.logger,
                   "This condition must be satisfied: "
                   "level0_stop_writes_trigger(%d) >= "
                   "level0_slowdown_writes_trigger(%d) >= "
                   "level0_file_num_compaction_trigger(%d)",
                   result.level0_stop_writes_trigger,
                   result.level0_slowdown_writes_trigger,
                   result.level0_file_num_compaction_trigger);
    if (result.level0_slowdown_writes_trigger <
        result.level0_file_num_compaction_trigger) {
      result.level0_slowdown_writes_trigger =
          result.level0_file_num_compaction_trigger;
    }
    if (result.level0_stop_writes_trigger <
        result.level0_slowdown_writes_trigger) {
      result.level0_stop_writes_trigger = result.level0_slowdown_writes_trigger;
    }
    ROCKS_LOG_WARN(db_options.logger,
                   "Adjust the value to "
                   "level0_stop_writes_trigger(%d) "
                   "level0_slowdown_writes_trigger(%d) "
                   "level0_file_num_compaction_trigger(%d)",
                   result.level0_stop_writes_trigger,
                   result.level0_slowdown_writes_trigger,
                   result.level0_file_num_compaction_trigger);
  }

  if (result.soft_pending_compaction_bytes_limit == 0) {
    result.soft_pending_compaction_bytes_limit =
        result.hard_pending_compaction_bytes_limit;
  } else if (result.hard_pending_compaction_bytes_limit > 0 &&
             result.soft_pending_compaction_bytes_limit >
                 result.hard_pending_compaction_bytes_limit) {
    result.soft_pending_compaction_bytes_limit =
        result.hard_pending_compaction_bytes_limit;
  }

  // When the DB is stopped, it's possible that there are some .trash files that
  // were not deleted yet, when we open the DB we will find these .trash files
  // and schedule them to be deleted (or delete immediately if SstFileManager
  // was not used)
  auto sfm =
      static_cast<SstFileManagerImpl*>(db_options.sst_file_manager.get());
  for (size_t i = 0; i < result.cf_paths.size(); i++) {
    DeleteScheduler::CleanupDirectory(db_options.env, sfm,
                                      result.cf_paths[i].path)
        .PermitUncheckedError();
  }

  if (result.cf_paths.empty()) {
    result.cf_paths = db_options.db_paths;
  }

  if (result.level_compaction_dynamic_level_bytes) {
    if (result.compaction_style != kCompactionStyleLevel) {
      ROCKS_LOG_INFO(db_options.info_log.get(),
                     "level_compaction_dynamic_level_bytes only makes sense "
                     "for level-based compaction");
      result.level_compaction_dynamic_level_bytes = false;
    } else if (result.cf_paths.size() > 1U) {
      // we don't yet know how to make both of this feature and multiple
      // DB path work.
      ROCKS_LOG_WARN(db_options.info_log.get(),
                     "multiple cf_paths/db_paths and "
                     "level_compaction_dynamic_level_bytes "
                     "can't be used together");
      result.level_compaction_dynamic_level_bytes = false;
    }
  }

  if (result.max_compaction_bytes == 0) {
    // For FIFO with use_kv_ratio_compaction, leave max_compaction_bytes as 0
    // to signal "auto-calculate target from capacity and SST/blob ratio."
    // When explicitly set by the user, it overrides the auto-calculated target.
    if (result.compaction_style != kCompactionStyleFIFO ||
        !result.compaction_options_fifo.use_kv_ratio_compaction) {
      result.max_compaction_bytes = result.target_file_size_base * 25;
    }
  }

  bool is_block_based_table = (result.table_factory->IsInstanceOf(
      TableFactory::kBlockBasedTableName()));

  const uint64_t kAdjustedTtl = 30 * 24 * 60 * 60;
  if (result.ttl == kDefaultTtl) {
    if (is_block_based_table) {
      // FIFO also requires max_open_files=-1, which is checked in
      // ValidateOptions().
      result.ttl = kAdjustedTtl;
    } else {
      result.ttl = 0;
    }
  }

  const uint64_t kAdjustedPeriodicCompSecs = 30 * 24 * 60 * 60;
  if (result.compaction_style == kCompactionStyleLevel) {
    if ((result.compaction_filter != nullptr ||
         result.compaction_filter_factory != nullptr) &&
        result.periodic_compaction_seconds == kDefaultPeriodicCompSecs &&
        is_block_based_table) {
      result.periodic_compaction_seconds = kAdjustedPeriodicCompSecs;
    }
  } else if (result.compaction_style == kCompactionStyleUniversal) {
    if (result.periodic_compaction_seconds == kDefaultPeriodicCompSecs &&
        is_block_based_table) {
      result.periodic_compaction_seconds = kAdjustedPeriodicCompSecs;
    }
  } else if (result.compaction_style == kCompactionStyleFIFO) {
    if (result.periodic_compaction_seconds != kDefaultPeriodicCompSecs) {
      ROCKS_LOG_WARN(
          db_options.info_log.get(),
          "periodic_compaction_seconds does not support FIFO compaction. You"
          "may want to set option TTL instead.");
    }
    if (result.last_level_temperature != Temperature::kUnknown) {
      ROCKS_LOG_WARN(
          db_options.info_log.get(),
          "last_level_temperature is ignored with FIFO compaction. Consider "
          "CompactionOptionsFIFO::file_temperature_age_thresholds.");
      result.last_level_temperature = Temperature::kUnknown;
    }
  }

  // For universal compaction, `ttl` and `periodic_compaction_seconds` mean the
  // same thing, take the stricter value.
  if (result.compaction_style == kCompactionStyleUniversal) {
    if (result.periodic_compaction_seconds == 0) {
      result.periodic_compaction_seconds = result.ttl;
    } else if (result.ttl != 0) {
      result.periodic_compaction_seconds =
          std::min(result.ttl, result.periodic_compaction_seconds);
    }
  }

  if (result.periodic_compaction_seconds == kDefaultPeriodicCompSecs) {
    result.periodic_compaction_seconds = 0;
  }

  if (read_only && (result.preserve_internal_time_seconds > 0 ||
                    result.preclude_last_level_data_seconds > 0)) {
    // With no writes coming in, we don't need periodic SeqnoToTime entries.
    // Existing SST files may or may not have that info associated with them.
    ROCKS_LOG_WARN(
        db_options.info_log.get(),
        "preserve_internal_time_seconds and preclude_last_level_data_seconds "
        "are ignored in read-only DB");
    result.preserve_internal_time_seconds = 0;
    result.preclude_last_level_data_seconds = 0;
  }

  if (read_only) {
    if (result.memtable_op_scan_flush_trigger) {
      ROCKS_LOG_WARN(db_options.info_log.get(),
                     "option memtable_op_scan_flush_trigger is sanitized to "
                     "0(disabled) for read only DB.");
      result.memtable_op_scan_flush_trigger = 0;
    }
    if (result.memtable_avg_op_scan_flush_trigger) {
      ROCKS_LOG_WARN(
          db_options.info_log.get(),
          "option memtable_avg_op_scan_flush_trigger is sanitized to "
          "0(disabled) for read only DB.");
      result.memtable_avg_op_scan_flush_trigger = 0;
    }
  }
  if (result.enable_blob_direct_write && !result.enable_blob_files) {
    ROCKS_LOG_WARN(db_options.info_log.get(),
                   "enable_blob_direct_write requires enable_blob_files=true. "
                   "Disabling blob direct write.");
    result.enable_blob_direct_write = false;
  }
  if (result.blob_direct_write_partitions == 0) {
    result.blob_direct_write_partitions = 1;
  }
  return result;
}

int SuperVersion::dummy = 0;
void* const SuperVersion::kSVInUse = &SuperVersion::dummy;
void* const SuperVersion::kSVObsolete = nullptr;

SuperVersion::~SuperVersion() {
  for (auto td : to_delete) {
    delete td;
  }
}

SuperVersion* SuperVersion::Ref() {
  refs.fetch_add(1, std::memory_order_relaxed);
  return this;
}

bool SuperVersion::Unref() {
  // fetch_sub returns the previous value of ref
  uint32_t previous_refs = refs.fetch_sub(1);
  assert(previous_refs > 0);
  return previous_refs == 1;
}

void SuperVersion::Cleanup() {
  assert(refs.load(std::memory_order_relaxed) == 0);
  // Since this SuperVersion object is being deleted,
  // decrement reference to the immutable MemtableList
  // this SV object was pointing to.
  imm->Unref(&to_delete);
  ReadOnlyMemTable* m = mem->Unref();
  if (m != nullptr) {
    auto* memory_usage = current->cfd()->imm()->current_memory_usage();
    assert(*memory_usage >= m->ApproximateMemoryUsage());
    *memory_usage -= m->ApproximateMemoryUsage();
    to_delete.push_back(m);
  }
  current->Unref();
  cfd->UnrefAndTryDelete();
}

void SuperVersion::Init(
    ColumnFamilyData* new_cfd, MemTable* new_mem, MemTableListVersion* new_imm,
    Version* new_current,
    std::shared_ptr<const SeqnoToTimeMapping> new_seqno_to_time_mapping) {
  cfd = new_cfd;
  mem = new_mem;
  imm = new_imm;
  current = new_current;
  full_history_ts_low = cfd->GetFullHistoryTsLow();
  seqno_to_time_mapping = std::move(new_seqno_to_time_mapping);
  cfd->Ref();
  mem->Ref();
  imm->Ref();
  current->Ref();
  refs.store(1, std::memory_order_relaxed);

  // There should be at least one mapping entry iff time tracking is enabled.
#ifndef NDEBUG
  MinAndMaxPreserveSeconds preserve_info{mutable_cf_options};
  if (preserve_info.IsEnabled()) {
    assert(seqno_to_time_mapping);
    assert(!seqno_to_time_mapping->Empty());
  } else {
    assert(seqno_to_time_mapping == nullptr);
  }
#endif  // NDEBUG
}

namespace {
void SuperVersionUnrefHandle(void* ptr) {
  // UnrefHandle is called when a thread exits or a ThreadLocalPtr gets
  // destroyed. When the former happens, the thread shouldn't see kSVInUse.
  // When the latter happens, only super_version_ holds a reference
  // to ColumnFamilyData, so no further queries are possible.
  SuperVersion* sv = static_cast<SuperVersion*>(ptr);
  bool was_last_ref __attribute__((__unused__));
  was_last_ref = sv->Unref();
  // Thread-local SuperVersions can't outlive ColumnFamilyData::super_version_.
  // This is important because we can't do SuperVersion cleanup here.
  // That would require locking DB mutex, which would deadlock because
  // SuperVersionUnrefHandle is called with locked ThreadLocalPtr mutex.
  assert(!was_last_ref);
}
}  // anonymous namespace

std::vector<std::string> ColumnFamilyData::GetDbPaths() const {
  std::vector<std::string> paths;
  paths.reserve(ioptions_.cf_paths.size());
  for (const DbPath& db_path : ioptions_.cf_paths) {
    paths.emplace_back(db_path.path);
  }
  return paths;
}

const uint32_t ColumnFamilyData::kDummyColumnFamilyDataId =
    std::numeric_limits<uint32_t>::max();

ColumnFamilyData::ColumnFamilyData(
    uint32_t id, const std::string& name, Version* _dummy_versions,
    Cache* _table_cache, WriteBufferManager* write_buffer_manager,
    const ColumnFamilyOptions& cf_options, const ImmutableDBOptions& db_options,
    const FileOptions* file_options, ColumnFamilySet* column_family_set,
    BlockCacheTracer* const block_cache_tracer,
    const std::shared_ptr<IOTracer>& io_tracer, const std::string& db_id,
    const std::string& db_session_id, bool read_only, bool fast_sst_open)
    : id_(id),
      name_(name),
      dummy_versions_(_dummy_versions),
      current_(nullptr),
      refs_(0),
      initialized_(false),
      dropped_(false),
      flush_skip_reschedule_(false),
      internal_comparator_(cf_options.comparator),
      initial_cf_options_(SanitizeCfOptions(db_options, read_only, cf_options)),
      ioptions_(db_options, initial_cf_options_),
      mutable_cf_options_(initial_cf_options_),
      is_delete_range_supported_(
          cf_options.table_factory->IsDeleteRangeSupported()),
      write_buffer_manager_(write_buffer_manager),
      mem_(nullptr),
      imm_(ioptions_.min_write_buffer_number_to_merge,
           ioptions_.max_write_buffer_size_to_maintain),
      super_version_(nullptr),
      super_version_number_(0),
      local_sv_(new ThreadLocalPtr(&SuperVersionUnrefHandle)),
      next_(nullptr),
      prev_(nullptr),
      log_number_(0),
      column_family_set_(column_family_set),
      queued_for_flush_(false),
      queued_for_compaction_(false),
      prev_compaction_needed_bytes_(0),
      allow_2pc_(db_options.allow_2pc),
      last_memtable_id_(0),
      db_paths_registered_(false),
      mempurge_used_(false),
      next_epoch_number_(1) {
  if (initial_cf_options_.enable_read_triggered_compaction &&
      (initial_cf_options_.rac_design_option == 2 ||
       initial_cf_options_.rac_design_option == 3)) {
    auto* collector = new (std::nothrow) RACOption2Collector(
        initial_cf_options_.rac_window_size, initial_cf_options_.num_levels);
    if (collector == nullptr) {
      rac_option2_initialization_status_ =
          Status::MemoryLimit("RAC option2 collector allocation failed");
    } else if (!collector->initialization_status().ok()) {
      rac_option2_initialization_status_ = collector->initialization_status();
      delete collector;
    } else {
      rac_option2_collector_.reset(collector);
    }
  }
  if (id_ != kDummyColumnFamilyDataId) {
    // TODO(cc): RegisterDbPaths can be expensive, considering moving it
    // outside of this constructor which might be called with db mutex held.
    // TODO(cc): considering using ioptions_.fs, currently some tests rely on
    // EnvWrapper, that's the main reason why we use env here.
    Status s = ioptions_.env->RegisterDbPaths(GetDbPaths());
    if (s.ok()) {
      db_paths_registered_ = true;
    } else {
      ROCKS_LOG_ERROR(
          ioptions_.logger,
          "Failed to register data paths of column family (id: %d, name: %s)",
          id_, name_.c_str());
    }
  }
  Ref();

  // Convert user defined table properties collector factories to internal ones.
  GetInternalTblPropCollFactory(ioptions_, &internal_tbl_prop_coll_factories_);

  // if _dummy_versions is nullptr, then this is a dummy column family.
  if (_dummy_versions != nullptr) {
    internal_stats_.reset(
        new InternalStats(ioptions_.num_levels, ioptions_.clock, this));
    table_cache_.reset(new TableCache(ioptions_, file_options, _table_cache,
                                      block_cache_tracer, io_tracer,
                                      db_session_id, fast_sst_open));
    blob_file_cache_.reset(
        new BlobFileCache(_table_cache, &ioptions(), soptions(), id_,
                          internal_stats_->GetBlobFileReadHist(), io_tracer));
    blob_source_.reset(new BlobSource(ioptions_, mutable_cf_options_, db_id,
                                      db_session_id, blob_file_cache_.get()));
    // Let the table cache route same-file ("embedded") blob reads through the
    // blob value cache + stats. Both objects share this CFD's lifetime.
    table_cache_->SetBlobSource(blob_source_.get());

    if (ioptions_.compaction_style == kCompactionStyleLevel) {
      compaction_picker_.reset(
          new LevelCompactionPicker(ioptions_, &internal_comparator_));
    } else if (ioptions_.compaction_style == kCompactionStyleUniversal) {
      compaction_picker_.reset(
          new UniversalCompactionPicker(ioptions_, &internal_comparator_));
    } else if (ioptions_.compaction_style == kCompactionStyleFIFO) {
      compaction_picker_.reset(
          new FIFOCompactionPicker(ioptions_, &internal_comparator_));
    } else if (ioptions_.compaction_style == kCompactionStyleNone) {
      compaction_picker_.reset(
          new NullCompactionPicker(ioptions_, &internal_comparator_));
      ROCKS_LOG_WARN(ioptions_.logger,
                     "Column family %s does not use any background compaction. "
                     "Compactions can only be done via CompactFiles\n",
                     GetName().c_str());
    } else {
      ROCKS_LOG_ERROR(ioptions_.logger,
                      "Unable to recognize the specified compaction style %d. "
                      "Column family %s will use kCompactionStyleLevel.\n",
                      ioptions_.compaction_style, GetName().c_str());
      compaction_picker_.reset(
          new LevelCompactionPicker(ioptions_, &internal_comparator_));
    }

    if (column_family_set_->NumberOfColumnFamilies() < 10) {
      ROCKS_LOG_INFO(ioptions_.logger,
                     "--------------- Options for column family [%s]:\n",
                     name.c_str());
      initial_cf_options_.Dump(ioptions_.logger);
    } else {
      ROCKS_LOG_INFO(ioptions_.logger, "\t(skipping printing options)\n");
    }
  }

  RecalculateWriteStallConditions(mutable_cf_options_);

  if (cf_options.table_factory->IsInstanceOf(
          TableFactory::kBlockBasedTableName()) &&
      cf_options.table_factory->GetOptions<BlockBasedTableOptions>()) {
    const BlockBasedTableOptions* bbto =
        cf_options.table_factory->GetOptions<BlockBasedTableOptions>();
    const auto& options_overrides = bbto->cache_usage_options.options_overrides;
    const auto file_metadata_charged =
        options_overrides.at(CacheEntryRole::kFileMetadata).charged;
    if (bbto->block_cache &&
        file_metadata_charged == CacheEntryRoleOptions::Decision::kEnabled) {
      // TODO(hx235): Add a `ConcurrentCacheReservationManager` at DB scope
      // responsible for reservation of `ObsoleteFileInfo` so that we can keep
      // this `file_metadata_cache_res_mgr_` nonconcurrent
      file_metadata_cache_res_mgr_.reset(new ConcurrentCacheReservationManager(
          std::make_shared<
              CacheReservationManagerImpl<CacheEntryRole::kFileMetadata>>(
              bbto->block_cache)));
    }
  }
}

// DB mutex held
ColumnFamilyData::~ColumnFamilyData() {
  StopRACOption2Collector();
  assert(refs_.load(std::memory_order_relaxed) == 0);
  // remove from linked list
  auto prev = prev_;
  auto next = next_;
  prev->next_ = next;
  next->prev_ = prev;

  if (!dropped_ && column_family_set_ != nullptr) {
    // If it's dropped, it's already removed from column family set
    // If column_family_set_ == nullptr, this is dummy CFD and not in
    // ColumnFamilySet
    column_family_set_->RemoveColumnFamily(this);
  }

  if (current_ != nullptr) {
    current_->Unref();
  }

  // It would be wrong if this ColumnFamilyData is in flush_queue_ or
  // compaction_queue_ and we destroyed it
  assert(!queued_for_flush_);
  assert(!queued_for_compaction_);
  assert(super_version_ == nullptr);

  if (dummy_versions_ != nullptr) {
    // List must be empty
    assert(dummy_versions_->Next() == dummy_versions_);
    bool deleted __attribute__((__unused__));
    deleted = dummy_versions_->Unref();
    assert(deleted);
  }

  if (mem_ != nullptr) {
    delete mem_->Unref();
  }
  autovector<ReadOnlyMemTable*> to_delete;
  imm_.current()->Unref(&to_delete);
  for (auto* m : to_delete) {
    delete m;
  }

  if (db_paths_registered_) {
    // TODO(cc): considering using ioptions_.fs, currently some tests rely on
    // EnvWrapper, that's the main reason why we use env here.
    Status s = ioptions_.env->UnregisterDbPaths(GetDbPaths());
    if (!s.ok()) {
      ROCKS_LOG_ERROR(
          ioptions_.logger,
          "Failed to unregister data paths of column family (id: %d, name: %s)",
          id_, name_.c_str());
    }
  }
}

void ColumnFamilyData::SetBlobPartitionManager(
    std::shared_ptr<BlobFilePartitionManager> mgr) {
  blob_partition_manager_ = std::move(mgr);
}

bool ColumnFamilyData::UnrefAndTryDelete() {
  int old_refs = refs_.fetch_sub(1);
  assert(old_refs > 0);

  if (old_refs == 1) {
    assert(super_version_ == nullptr);
    delete this;
    return true;
  }

  if (old_refs == 2 && super_version_ != nullptr) {
    // Only the super_version_ holds me
    SuperVersion* sv = super_version_;
    super_version_ = nullptr;

    // Release SuperVersion references kept in ThreadLocalPtr.
    local_sv_.reset();

    if (sv->Unref()) {
      // Note: sv will delete this ColumnFamilyData during Cleanup()
      assert(sv->cfd == this);
      sv->Cleanup();
      delete sv;
      return true;
    }
  }
  return false;
}

void ColumnFamilyData::SetDropped() {
  // can't drop default CF
  assert(id_ != 0);
  dropped_ = true;
  StopRACOption2Collector();
  write_controller_token_.reset();

  // remove from column_family_set
  column_family_set_->RemoveColumnFamily(this);
}

ColumnFamilyOptions ColumnFamilyData::GetLatestCFOptions() const {
  return BuildColumnFamilyOptions(initial_cf_options_, mutable_cf_options_);
}

uint64_t ColumnFamilyData::OldestLogToKeep() {
  auto current_log = GetLogNumber();

  if (allow_2pc_) {
    auto imm_prep_log = imm()->PrecomputeMinLogContainingPrepSection();
    auto mem_prep_log = mem()->GetMinLogContainingPrepSection();

    if (imm_prep_log > 0 && imm_prep_log < current_log) {
      current_log = imm_prep_log;
    }

    if (mem_prep_log > 0 && mem_prep_log < current_log) {
      current_log = mem_prep_log;
    }
  }

  return current_log;
}

const double kIncSlowdownRatio = 0.8;
const double kDecSlowdownRatio = 1 / kIncSlowdownRatio;
const double kNearStopSlowdownRatio = 0.6;
const double kDelayRecoverSlowdownRatio = 1.4;

namespace {
// If penalize_stop is true, we further reduce slowdown rate.
std::unique_ptr<WriteControllerToken> SetupDelay(
    WriteController* write_controller, uint64_t compaction_needed_bytes,
    uint64_t prev_compaction_need_bytes, bool penalize_stop,
    bool auto_compactions_disabled) {
  const uint64_t kMinWriteRate = 16 * 1024u;  // Minimum write rate 16KB/s.

  uint64_t max_write_rate = write_controller->max_delayed_write_rate();
  uint64_t write_rate = write_controller->delayed_write_rate();

  if (auto_compactions_disabled) {
    // When auto compaction is disabled, always use the value user gave.
    write_rate = max_write_rate;
  } else if (write_controller->NeedsDelay() && max_write_rate > kMinWriteRate) {
    // If user gives rate less than kMinWriteRate, don't adjust it.
    //
    // If already delayed, need to adjust based on previous compaction debt.
    // When there are two or more column families require delay, we always
    // increase or reduce write rate based on information for one single
    // column family. It is likely to be OK but we can improve if there is a
    // problem.
    // Ignore compaction_needed_bytes = 0 case because compaction_needed_bytes
    // is only available in level-based compaction
    //
    // If the compaction debt stays the same as previously, we also further slow
    // down. It usually means a mem table is full. It's mainly for the case
    // where both of flush and compaction are much slower than the speed we
    // insert to mem tables, so we need to actively slow down before we get
    // feedback signal from compaction and flushes to avoid the full stop
    // because of hitting the max write buffer number.
    //
    // If DB just falled into the stop condition, we need to further reduce
    // the write rate to avoid the stop condition.
    if (penalize_stop) {
      // Penalize the near stop or stop condition by more aggressive slowdown.
      // This is to provide the long term slowdown increase signal.
      // The penalty is more than the reward of recovering to the normal
      // condition.
      write_rate = static_cast<uint64_t>(static_cast<double>(write_rate) *
                                         kNearStopSlowdownRatio);
      if (write_rate < kMinWriteRate) {
        write_rate = kMinWriteRate;
      }
    } else if (prev_compaction_need_bytes > 0 &&
               prev_compaction_need_bytes <= compaction_needed_bytes) {
      write_rate = static_cast<uint64_t>(static_cast<double>(write_rate) *
                                         kIncSlowdownRatio);
      if (write_rate < kMinWriteRate) {
        write_rate = kMinWriteRate;
      }
    } else if (prev_compaction_need_bytes > compaction_needed_bytes) {
      // We are speeding up by ratio of kSlowdownRatio when we have paid
      // compaction debt. But we'll never speed up to faster than the write rate
      // given by users.
      write_rate = static_cast<uint64_t>(static_cast<double>(write_rate) *
                                         kDecSlowdownRatio);
      if (write_rate > max_write_rate) {
        write_rate = max_write_rate;
      }
    }
  }
  return write_controller->GetDelayToken(write_rate);
}

int GetL0FileCountForCompactionSpeedup(int level0_file_num_compaction_trigger,
                                       int level0_slowdown_writes_trigger) {
  // SanitizeOptions() ensures it.
  assert(level0_file_num_compaction_trigger <= level0_slowdown_writes_trigger);

  if (level0_file_num_compaction_trigger < 0) {
    return std::numeric_limits<int>::max();
  }

  const int64_t twice_level0_trigger =
      static_cast<int64_t>(level0_file_num_compaction_trigger) * 2;

  const int64_t one_fourth_trigger_slowdown =
      static_cast<int64_t>(level0_file_num_compaction_trigger) +
      ((level0_slowdown_writes_trigger - level0_file_num_compaction_trigger) /
       4);

  assert(twice_level0_trigger >= 0);
  assert(one_fourth_trigger_slowdown >= 0);

  // 1/4 of the way between L0 compaction trigger threshold and slowdown
  // condition.
  // Or twice as compaction trigger, if it is smaller.
  int64_t res = std::min(twice_level0_trigger, one_fourth_trigger_slowdown);
  if (res >= std::numeric_limits<int32_t>::max()) {
    return std::numeric_limits<int32_t>::max();
  } else {
    // res fits in int
    return static_cast<int>(res);
  }
}

uint64_t GetPendingCompactionBytesForCompactionSpeedup(
    const MutableCFOptions& mutable_cf_options,
    const VersionStorageInfo* vstorage) {
  // Compaction debt relatively large compared to the stable (bottommost) data
  // size indicates compaction fell behind.
  const uint64_t kBottommostSizeDivisor = 8;
  // Meaningful progress toward the slowdown trigger is another good indication.
  const uint64_t kSlowdownTriggerDivisor = 4;

  uint64_t bottommost_files_size = 0;
  for (const auto& level_and_file : vstorage->BottommostFiles()) {
    bottommost_files_size += level_and_file.second->fd.GetFileSize();
  }

  // Slowdown trigger might be zero but that means compaction speedup should
  // always happen (undocumented/historical), so no special treatment is needed.
  uint64_t slowdown_threshold =
      mutable_cf_options.soft_pending_compaction_bytes_limit /
      kSlowdownTriggerDivisor;

  // Size of zero, however, should not be used to decide to speedup compaction.
  if (bottommost_files_size == 0) {
    return slowdown_threshold;
  }

  // Prevent a small CF from triggering parallel compactions for other CFs.
  // Require compaction debt to be more than a full L0 to Lbase compaction.
  const uint64_t kMinDebtSize = 2 * mutable_cf_options.max_bytes_for_level_base;
  uint64_t size_threshold =
      std::max(bottommost_files_size / kBottommostSizeDivisor, kMinDebtSize);
  return std::min(size_threshold, slowdown_threshold);
}

uint64_t GetMarkedFileCountForCompactionSpeedup() {
  // When just one file is marked, it is not clear that parallel compaction will
  // help the compaction that the user nicely requested to happen sooner. When
  // multiple files are marked, however, it is pretty clearly helpful, except
  // for the rare case in which a single compaction grabs all the marked files.
  return 2;
}
}  // anonymous namespace

std::pair<WriteStallCondition, WriteStallCause>
ColumnFamilyData::GetWriteStallConditionAndCause(
    int num_unflushed_memtables, int num_l0_files,
    uint64_t num_compaction_needed_bytes,
    const MutableCFOptions& mutable_cf_options,
    const ImmutableCFOptions& immutable_cf_options) {
  if (num_unflushed_memtables >= mutable_cf_options.max_write_buffer_number) {
    return {WriteStallCondition::kStopped, WriteStallCause::kMemtableLimit};
  } else if (!mutable_cf_options.disable_auto_compactions &&
             num_l0_files >= mutable_cf_options.level0_stop_writes_trigger) {
    return {WriteStallCondition::kStopped, WriteStallCause::kL0FileCountLimit};
  } else if (!mutable_cf_options.disable_auto_compactions &&
             mutable_cf_options.hard_pending_compaction_bytes_limit > 0 &&
             num_compaction_needed_bytes >=
                 mutable_cf_options.hard_pending_compaction_bytes_limit) {
    return {WriteStallCondition::kStopped,
            WriteStallCause::kPendingCompactionBytes};
  } else if (mutable_cf_options.max_write_buffer_number > 3 &&
             num_unflushed_memtables >=
                 mutable_cf_options.max_write_buffer_number - 1 &&
             num_unflushed_memtables - 1 >=
                 immutable_cf_options.min_write_buffer_number_to_merge) {
    return {WriteStallCondition::kDelayed, WriteStallCause::kMemtableLimit};
  } else if (!mutable_cf_options.disable_auto_compactions &&
             mutable_cf_options.level0_slowdown_writes_trigger >= 0 &&
             num_l0_files >=
                 mutable_cf_options.level0_slowdown_writes_trigger) {
    return {WriteStallCondition::kDelayed, WriteStallCause::kL0FileCountLimit};
  } else if (!mutable_cf_options.disable_auto_compactions &&
             mutable_cf_options.soft_pending_compaction_bytes_limit > 0 &&
             num_compaction_needed_bytes >=
                 mutable_cf_options.soft_pending_compaction_bytes_limit) {
    return {WriteStallCondition::kDelayed,
            WriteStallCause::kPendingCompactionBytes};
  }
  return {WriteStallCondition::kNormal, WriteStallCause::kNone};
}

WriteStallCondition ColumnFamilyData::RecalculateWriteStallConditions(
    const MutableCFOptions& mutable_cf_options) {
  auto write_stall_condition = WriteStallCondition::kNormal;
  if (current_ != nullptr) {
    auto* vstorage = current_->storage_info();
    auto write_controller = column_family_set_->write_controller_;
    uint64_t compaction_needed_bytes =
        vstorage->estimated_compaction_needed_bytes();

    auto write_stall_condition_and_cause = GetWriteStallConditionAndCause(
        imm()->NumNotFlushed(), vstorage->l0_delay_trigger_count(),
        vstorage->estimated_compaction_needed_bytes(), mutable_cf_options,
        ioptions());
    write_stall_condition = write_stall_condition_and_cause.first;
    auto write_stall_cause = write_stall_condition_and_cause.second;

    bool was_stopped = write_controller->IsStopped();
    bool needed_delay = write_controller->NeedsDelay();

    if (write_stall_condition == WriteStallCondition::kStopped &&
        write_stall_cause == WriteStallCause::kMemtableLimit) {
      write_controller_token_ = write_controller->GetStopToken();
      internal_stats_->AddCFStats(InternalStats::MEMTABLE_LIMIT_STOPS, 1);
      ROCKS_LOG_WARN(
          ioptions_.logger,
          "[%s] Stopping writes because we have %d immutable memtables "
          "(waiting for flush), max_write_buffer_number is set to %d",
          name_.c_str(), imm()->NumNotFlushed(),
          mutable_cf_options.max_write_buffer_number);
    } else if (write_stall_condition == WriteStallCondition::kStopped &&
               write_stall_cause == WriteStallCause::kL0FileCountLimit) {
      write_controller_token_ = write_controller->GetStopToken();
      internal_stats_->AddCFStats(InternalStats::L0_FILE_COUNT_LIMIT_STOPS, 1);
      if (compaction_picker_->IsLevel0CompactionInProgress()) {
        internal_stats_->AddCFStats(
            InternalStats::L0_FILE_COUNT_LIMIT_STOPS_WITH_ONGOING_COMPACTION,
            1);
      }
      ROCKS_LOG_WARN(ioptions_.logger,
                     "[%s] Stopping writes because we have %d level-0 files",
                     name_.c_str(), vstorage->l0_delay_trigger_count());
    } else if (write_stall_condition == WriteStallCondition::kStopped &&
               write_stall_cause == WriteStallCause::kPendingCompactionBytes) {
      write_controller_token_ = write_controller->GetStopToken();
      internal_stats_->AddCFStats(
          InternalStats::PENDING_COMPACTION_BYTES_LIMIT_STOPS, 1);
      ROCKS_LOG_WARN(
          ioptions_.logger,
          "[%s] Stopping writes because of estimated pending compaction "
          "bytes %" PRIu64,
          name_.c_str(), compaction_needed_bytes);
    } else if (write_stall_condition == WriteStallCondition::kDelayed &&
               write_stall_cause == WriteStallCause::kMemtableLimit) {
      write_controller_token_ =
          SetupDelay(write_controller, compaction_needed_bytes,
                     prev_compaction_needed_bytes_, was_stopped,
                     mutable_cf_options.disable_auto_compactions);
      internal_stats_->AddCFStats(InternalStats::MEMTABLE_LIMIT_DELAYS, 1);
      ROCKS_LOG_WARN(
          ioptions_.logger,
          "[%s] Stalling writes because we have %d immutable memtables "
          "(waiting for flush), max_write_buffer_number is set to %d "
          "rate %" PRIu64,
          name_.c_str(), imm()->NumNotFlushed(),
          mutable_cf_options.max_write_buffer_number,
          write_controller->delayed_write_rate());
    } else if (write_stall_condition == WriteStallCondition::kDelayed &&
               write_stall_cause == WriteStallCause::kL0FileCountLimit) {
      // L0 is the last two files from stopping.
      bool near_stop = vstorage->l0_delay_trigger_count() >=
                       mutable_cf_options.level0_stop_writes_trigger - 2;
      write_controller_token_ =
          SetupDelay(write_controller, compaction_needed_bytes,
                     prev_compaction_needed_bytes_, was_stopped || near_stop,
                     mutable_cf_options.disable_auto_compactions);
      internal_stats_->AddCFStats(InternalStats::L0_FILE_COUNT_LIMIT_DELAYS, 1);
      if (compaction_picker_->IsLevel0CompactionInProgress()) {
        internal_stats_->AddCFStats(
            InternalStats::L0_FILE_COUNT_LIMIT_DELAYS_WITH_ONGOING_COMPACTION,
            1);
      }
      ROCKS_LOG_WARN(ioptions_.logger,
                     "[%s] Stalling writes because we have %d level-0 files "
                     "rate %" PRIu64,
                     name_.c_str(), vstorage->l0_delay_trigger_count(),
                     write_controller->delayed_write_rate());
    } else if (write_stall_condition == WriteStallCondition::kDelayed &&
               write_stall_cause == WriteStallCause::kPendingCompactionBytes) {
      // If the distance to hard limit is less than 1/4 of the gap between soft
      // and
      // hard bytes limit, we think it is near stop and speed up the slowdown.
      bool near_stop =
          mutable_cf_options.hard_pending_compaction_bytes_limit > 0 &&
          (compaction_needed_bytes -
           mutable_cf_options.soft_pending_compaction_bytes_limit) >
              3 *
                  (mutable_cf_options.hard_pending_compaction_bytes_limit -
                   mutable_cf_options.soft_pending_compaction_bytes_limit) /
                  4;

      write_controller_token_ =
          SetupDelay(write_controller, compaction_needed_bytes,
                     prev_compaction_needed_bytes_, was_stopped || near_stop,
                     mutable_cf_options.disable_auto_compactions);
      internal_stats_->AddCFStats(
          InternalStats::PENDING_COMPACTION_BYTES_LIMIT_DELAYS, 1);
      ROCKS_LOG_WARN(
          ioptions_.logger,
          "[%s] Stalling writes because of estimated pending compaction "
          "bytes %" PRIu64 " rate %" PRIu64,
          name_.c_str(), vstorage->estimated_compaction_needed_bytes(),
          write_controller->delayed_write_rate());
    } else {
      assert(write_stall_condition == WriteStallCondition::kNormal);
      if (vstorage->l0_delay_trigger_count() >=
          GetL0FileCountForCompactionSpeedup(
              mutable_cf_options.level0_file_num_compaction_trigger,
              mutable_cf_options.level0_slowdown_writes_trigger)) {
        write_controller_token_ =
            write_controller->GetCompactionPressureToken();
        ROCKS_LOG_INFO(
            ioptions_.logger,
            "[%s] Increasing compaction threads because we have %d level-0 "
            "files ",
            name_.c_str(), vstorage->l0_delay_trigger_count());
      } else if (mutable_cf_options.soft_pending_compaction_bytes_limit == 0) {
        // If soft pending compaction byte limit is not set, always speed up
        // compaction.
        write_controller_token_ =
            write_controller->GetCompactionPressureToken();
      } else if (vstorage->estimated_compaction_needed_bytes() >=
                 GetPendingCompactionBytesForCompactionSpeedup(
                     mutable_cf_options, vstorage)) {
        write_controller_token_ =
            write_controller->GetCompactionPressureToken();
        ROCKS_LOG_INFO(
            ioptions_.logger,
            "[%s] Increasing compaction threads because of estimated pending "
            "compaction "
            "bytes %" PRIu64,
            name_.c_str(), vstorage->estimated_compaction_needed_bytes());
      } else if (uint64_t(vstorage->FilesMarkedForCompaction().size()) >=
                 GetMarkedFileCountForCompactionSpeedup()) {
        write_controller_token_ =
            write_controller->GetCompactionPressureToken();
        ROCKS_LOG_INFO(
            ioptions_.logger,
            "[%s] Increasing compaction threads because we have %" PRIu64
            " files marked for compaction",
            name_.c_str(),
            uint64_t(vstorage->FilesMarkedForCompaction().size()));
      } else {
        write_controller_token_.reset();
      }
      // If the DB recovers from delay conditions, we reward with reducing
      // double the slowdown ratio. This is to balance the long term slowdown
      // increase signal.
      if (needed_delay) {
        uint64_t write_rate = write_controller->delayed_write_rate();
        write_controller->set_delayed_write_rate(static_cast<uint64_t>(
            static_cast<double>(write_rate) * kDelayRecoverSlowdownRatio));
        // Set the low pri limit to be 1/4 the delayed write rate.
        // Note we don't reset this value even after delay condition is relased.
        // Low-pri rate will continue to apply if there is a compaction
        // pressure.
        write_controller->low_pri_rate_limiter()->SetBytesPerSecond(write_rate /
                                                                    4);
      }
    }
    prev_compaction_needed_bytes_ = compaction_needed_bytes;
  }
  return write_stall_condition;
}

const FileOptions* ColumnFamilyData::soptions() const {
  return &(column_family_set_->file_options_);
}

void ColumnFamilyData::SetCurrent(Version* current_version) {
  current_ = current_version;
}

uint32_t ColumnFamilyData::GetRACDesignOption() const {
  return mutable_cf_options_.rac_design_option;
}

bool ColumnFamilyData::IsReadTriggeredCompactionEnabled() const {
  return mutable_cf_options_.enable_read_triggered_compaction;
}

bool ColumnFamilyData::IsRACOption2Enabled() const {
  return IsReadTriggeredCompactionEnabled() &&
         (GetRACDesignOption() == 2 || GetRACDesignOption() == 3);
}

bool ColumnFamilyData::IsRAC2TimeModelEnabled() const {
  return IsReadTriggeredCompactionEnabled() && GetRACDesignOption() == 3;
}

uint64_t ColumnFamilyData::GetRACOption2WindowSize() const {
  return mutable_cf_options_.rac_window_size;
}

uint32_t ColumnFamilyData::GetRACOption2TopK() const {
  return mutable_cf_options_.rac_top_k;
}

double ColumnFamilyData::GetRACOption2EfficiencyThreshold() const {
  return mutable_cf_options_.rac_benefit_efficiency_threshold;
}

double ColumnFamilyData::GetRACOption2HighMissRatioThreshold() const {
  return mutable_cf_options_.rac_high_miss_ratio_threshold;
}

double ColumnFamilyData::GetRACOption2EffectiveHighMissRatioThreshold(
    uint64_t total_reads, bool no_committed_writes) const {
  constexpr double kReadOnlyHighMissRatioThreshold = 0.95;
  if (total_reads == GetRACOption2WindowSize() && no_committed_writes) {
    return kReadOnlyHighMissRatioThreshold;
  }
  return GetRACOption2HighMissRatioThreshold();
}

bool ColumnFamilyData::RACOption2L0InCooldown(uint64_t current_epoch) const {
  if (!rac_option2_has_l0_trigger_epoch_) {
    return false;
  }
  if (current_epoch < rac_option2_last_l0_trigger_epoch_) {
    return true;
  }
  return current_epoch - rac_option2_last_l0_trigger_epoch_ <
         mutable_cf_options_.rac_option2_l0_cooldown_windows;
}

void ColumnFamilyData::RecordRACOption2L0Trigger(uint64_t epoch_id) {
  rac_option2_last_l0_trigger_epoch_ = epoch_id;
  rac_option2_has_l0_trigger_epoch_ = true;
}

double ColumnFamilyData::GetRACOption2BottomOverlapThreshold() const {
  return mutable_cf_options_.rac_bottom_overlap_threshold;
}

uint64_t ColumnFamilyData::GetRACOption2CooldownWindows() const {
  return mutable_cf_options_.rac_cooldown_windows;
}

Status ColumnFamilyData::BeginRACOption2Lookup(
    std::unique_ptr<RACOption2LookupRecord>* record) {
  if (record == nullptr) {
    return Status::InvalidArgument("null RAC option2 lookup output");
  }
  record->reset();
  if (!IsRACOption2Enabled()) return Status::OK();
  if (rac_option2_collector_ == nullptr) {
    return Status::Incomplete("RAC option2 collector unavailable");
  }
  std::unique_ptr<RACOption2LookupRecord> result(new RACOption2LookupRecord());
  Status status = rac_option2_collector_->Reserve(result.get());
  if (status.ok()) *record = std::move(result);
  return status;
}

Status ColumnFamilyData::BeginRACOption2Lookup(
    RACOption2LookupRecord* record) {
  if (record == nullptr) {
    return Status::InvalidArgument("null RAC option2 lookup output");
  }
  if (!IsRACOption2Enabled()) return Status::OK();
  if (rac_option2_collector_ == nullptr) {
    return Status::Incomplete("RAC option2 collector unavailable");
  }
  return rac_option2_collector_->Reserve(record);
}

Status ColumnFamilyData::BeginRACOption2Lookup(
    RACOption2LookupRecord* record, uint64_t latest_sequence) {
  if (record == nullptr) {
    return Status::InvalidArgument("null RAC option2 lookup output");
  }
  if (!IsRACOption2Enabled()) return Status::OK();
  if (rac_option2_collector_ == nullptr) {
    return Status::Incomplete("RAC option2 collector unavailable");
  }
  return rac_option2_collector_->Reserve(record, latest_sequence);
}

bool ColumnFamilyData::TakeNextRACOption2Snapshot(
    RACOption2EpochSnapshot* snapshot) {
  return rac_option2_collector_ != nullptr &&
         rac_option2_collector_->TakeNext(snapshot);
}

bool ColumnFamilyData::MarkRACOption2EpochPending(uint64_t epoch_id) {
  return rac_option2_collector_ != nullptr &&
         rac_option2_collector_->MarkPending(epoch_id);
}

bool ColumnFamilyData::FinishRACOption2Epoch(
    uint64_t epoch_id, RACOption2EpochTerminalState terminal_state) {
  return rac_option2_collector_ != nullptr &&
         rac_option2_collector_->Finish(epoch_id, terminal_state);
}

uint64_t ColumnFamilyData::GetLatestCompletedRACOption2Epoch() const {
  return rac_option2_collector_ == nullptr
             ? 0
             : rac_option2_collector_->latest_completed_epoch();
}

void ColumnFamilyData::RecordRACOption2CompactionCost(
    bool read_only_window, uint64_t rewrite_io_bytes,
    uint64_t elapsed_micros) {
  if (rewrite_io_bytes == 0 || elapsed_micros == 0) {
    return;
  }
  constexpr double kNewSampleWeight = 0.25;
  const double sample = static_cast<double>(rewrite_io_bytes) /
                        static_cast<double>(elapsed_micros);
  double* ewma = read_only_window
                     ? &rac_option2_read_only_bytes_per_micros_
                     : &rac_option2_mixed_bytes_per_micros_;
  *ewma = *ewma == 0.0
              ? sample
              : (1.0 - kNewSampleWeight) * *ewma + kNewSampleWeight * sample;
}

double ColumnFamilyData::GetRACOption2CompactionDurationFactor(
    bool read_only_window) const {
  // One GiB/s is the neutral point. Slower observed compactions are charged
  // proportionally, while faster samples do not make rewrite I/O free.
  constexpr double kReferenceBytesPerMicro = 1024.0;
  constexpr double kMaxDurationFactor = 16.0;
  const double observed = read_only_window
                              ? rac_option2_read_only_bytes_per_micros_
                              : rac_option2_mixed_bytes_per_micros_;
  if (!std::isfinite(observed) || observed <= 0.0) {
    return 1.0;
  }
  return std::clamp(kReferenceBytesPerMicro / observed, 1.0,
                    kMaxDurationFactor);
}

double ColumnFamilyData::EstimateRACOption2FutureRewriteProbability(
    int output_level, uint64_t replaced_target_bytes,
    uint64_t estimated_output_bytes) const {
  if (current_ == nullptr || output_level < 0 ||
      output_level >= NumberLevels()) {
    return 1.0;
  }
  const VersionStorageInfo* storage = current_->storage_info();
  const uint64_t current_bytes = storage->NumLevelBytes(output_level);
  const long double retained_bytes =
      current_bytes >= replaced_target_bytes
          ? static_cast<long double>(current_bytes - replaced_target_bytes)
          : 0.0L;
  const long double post_compaction_bytes =
      retained_bytes + estimated_output_bytes;
  const uint64_t level_limit = storage->MaxBytesForLevel(output_level);
  if (level_limit == 0) {
    return 1.0;
  }
  const double pressure =
      static_cast<double>(post_compaction_bytes / level_limit);
  // Pressure below 50% has no near-term rewrite charge. Between 50% and
  // 100%, risk rises linearly; at or above the level target it is certain.
  return std::clamp(2.0 * pressure - 1.0, 0.0, 1.0);
}

RACOption2CollectorStats ColumnFamilyData::GetRACOption2CollectorStats() const {
  RACOption2CollectorStats stats =
      rac_option2_collector_ == nullptr ? RACOption2CollectorStats()
                                        : rac_option2_collector_->GetStats();
  stats.cooldown_entries = rac_option2_bottom_range_cooldowns_.size();
  stats.cooldown_high_watermark = rac_option2_cooldown_high_watermark_;
  for (const auto& entry : rac_option2_bottom_range_cooldowns_) {
    stats.cooldown_bytes += sizeof(entry) + entry.smallest_key.capacity() +
                            entry.largest_key.capacity();
  }
  stats.memory_bytes += stats.cooldown_bytes;
  return stats;
}

void ColumnFamilyData::StopRACOption2Collector() {
  if (rac_option2_collector_ != nullptr) rac_option2_collector_->Stop();
  if (rac_option2_request_.active && current_ != nullptr) {
    for (int level = 0; level < NumberLevels(); ++level) {
      for (FileMetaData* file : current_->storage_info()->LevelFiles(level)) {
        if (file->fd.GetNumber() == rac_option2_request_.seed_file_number &&
            file->marked_for_compaction_source ==
                MarkedForCompactionSource::kRACOption2) {
          file->marked_for_compaction = false;
          file->marked_for_compaction_source =
              MarkedForCompactionSource::kUnknown;
        }
      }
    }
  }
  rac_option2_request_ = RACOption2CompactionRequest();
  rac_option2_bottom_range_cooldowns_.clear();
  rac_option2_last_installed_request_id_ = 0;
  rac_option2_last_l0_trigger_epoch_ = 0;
  rac_option2_has_l0_trigger_epoch_ = false;
  rac_option2_cooldown_high_watermark_ = 0;
  rac_option2_read_only_bytes_per_micros_ = 0.0;
  rac_option2_mixed_bytes_per_micros_ = 0.0;
}

void ColumnFamilyData::TEST_SetRACOption2NextTicket(uint64_t ticket) {
  if (rac_option2_collector_ != nullptr) {
    rac_option2_collector_->TEST_SetNextTicket(ticket);
  }
}

bool ColumnFamilyData::OnRACOption2CompactionInstalled(
    uint64_t install_id, const InternalKey& smallest,
    const InternalKey& largest) {
  if (!IsRACOption2Enabled() ||
      ioptions_.compaction_style != kCompactionStyleLevel || install_id == 0 ||
      install_id <= rac_option2_last_installed_request_id_) {
    return false;
  }
  if (!RACOption2CooldownHasCapacity(smallest, largest)) return false;
  rac_option2_last_installed_request_id_ = install_id;
  ReadTriggeredCompactionBottomRange entry;
  entry.smallest_key = smallest.Encode().ToString();
  entry.largest_key = largest.Encode().ToString();
  entry.window_id = GetLatestCompletedRACOption2Epoch();
  rac_option2_bottom_range_cooldowns_.emplace_back(std::move(entry));
  rac_option2_cooldown_high_watermark_ =
      std::max(rac_option2_cooldown_high_watermark_,
               rac_option2_bottom_range_cooldowns_.size());
  return true;
}

bool ColumnFamilyData::RACOption2CooldownHasCapacity(
    const InternalKey& smallest, const InternalKey& largest) const {
  constexpr size_t kMaxEntries = 65536;
  constexpr size_t kBudgetBytes = size_t{8} * 1024 * 1024;
  if (rac_option2_bottom_range_cooldowns_.size() >= kMaxEntries) return false;
  size_t used = 0;
  for (const auto& entry : rac_option2_bottom_range_cooldowns_) {
    const size_t entry_bytes = sizeof(entry) + entry.smallest_key.capacity() +
                               entry.largest_key.capacity();
    if (entry_bytes > kBudgetBytes - std::min(used, kBudgetBytes)) {
      return false;
    }
    used += entry_bytes;
  }
  const size_t key_bytes = smallest.Encode().size() + largest.Encode().size();
  if (key_bytes > (kBudgetBytes - used) / 2) return false;
  return sizeof(ReadTriggeredCompactionBottomRange) <=
         kBudgetBytes - used - 2 * key_bytes;
}

bool ColumnFamilyData::RACOption2BottomRangeInCooldown(
    const InternalKey& smallest, const InternalKey& largest,
    uint64_t current_epoch) {
  PruneRACOption2Cooldown(current_epoch);
  bool overlaps = false;
  auto& entries = rac_option2_bottom_range_cooldowns_;
  for (const auto& entry : entries) {
        if (current_epoch < entry.window_id) {
          overlaps = true;  // Fail closed for an impossible stale watermark.
          continue;
        }
        const Slice entry_smallest(entry.smallest_key);
        const Slice entry_largest(entry.largest_key);
        const Comparator* user_comparator =
            internal_comparator_.user_comparator();
        const bool disjoint =
            user_comparator->CompareWithoutTimestamp(
                ExtractUserKey(largest.Encode()),
                ExtractUserKey(entry_smallest)) < 0 ||
            user_comparator->CompareWithoutTimestamp(
                ExtractUserKey(entry_largest),
                ExtractUserKey(smallest.Encode())) < 0;
        overlaps = overlaps || !disjoint;
  }
  return overlaps;
}

void ColumnFamilyData::PruneRACOption2Cooldown(uint64_t current_epoch) {
  const uint64_t cooldown = GetRACOption2CooldownWindows();
  auto& entries = rac_option2_bottom_range_cooldowns_;
  entries.erase(std::remove_if(entries.begin(), entries.end(),
                               [&](const auto& entry) {
                                 return current_epoch >= entry.window_id &&
                                        current_epoch - entry.window_id >
                                            cooldown;
                               }),
                entries.end());
}

size_t ColumnFamilyData::GetRACOption2CooldownEntryCount() const {
  return rac_option2_bottom_range_cooldowns_.size();
}

uint64_t ColumnFamilyData::NextRACOption2RequestId() {
  if (next_rac_option2_request_id_ == 0 ||
      next_rac_option2_request_id_ ==
          std::numeric_limits<uint64_t>::max()) {
    return 0;
  }
  return next_rac_option2_request_id_++;
}

bool ColumnFamilyData::HasRACOption2Request() const {
  return rac_option2_request_.active;
}

const RACOption2CompactionRequest& ColumnFamilyData::GetRACOption2Request()
    const {
  return rac_option2_request_;
}

bool ColumnFamilyData::IsRACOption2RequestStale() const {
  if (!rac_option2_request_.active || rac_option2_request_.invalidated ||
      rac_option2_request_.source_level < 0 ||
      rac_option2_request_.source_level >= NumberLevels() ||
      rac_option2_request_.compaction_style != ioptions_.compaction_style) {
    if (rac_option2_request_.active) {
      rac_option2_request_.stale = true;
      if (rac_option2_request_.invalidation_reason.empty()) {
        rac_option2_request_.invalidation_reason =
            "invalidated_or_incompatible_style";
      }
      ROCKS_LOG_INFO(
          ioptions_.logger,
          "RAC_OPTION2 request_stale cf=%s request=%" PRIu64
          " reason=invalidated_or_incompatible_style",
          GetName().c_str(), rac_option2_request_.request_id);
    }
    return rac_option2_request_.active;
  }
  if (ioptions_.compaction_style == kCompactionStyleUniversal) {
    bool seed_found = false;
    size_t input_files_found = 0;
    for (int level = 0; level < NumberLevels(); ++level) {
      for (FileMetaData* file : current_->storage_info()->LevelFiles(level)) {
        if (file->fd.GetNumber() == rac_option2_request_.seed_file_number) {
          seed_found =
              level == rac_option2_request_.source_level &&
              file->marked_for_compaction &&
              file->marked_for_compaction_source ==
                  MarkedForCompactionSource::kRACOption2;
        }
        if (std::binary_search(rac_option2_request_.input_file_numbers.begin(),
                               rac_option2_request_.input_file_numbers.end(),
                               file->fd.GetNumber())) {
          if (file->being_compacted) {
            rac_option2_request_.stale = true;
            rac_option2_request_.invalidation_reason = "input_compacting";
            ROCKS_LOG_INFO(
                ioptions_.logger,
                "RAC_OPTION2 request_stale cf=%s request=%" PRIu64
                " reason=input_compacting file=#%" PRIu64,
                GetName().c_str(), rac_option2_request_.request_id,
                file->fd.GetNumber());
            return true;
          }
          ++input_files_found;
        }
      }
    }
    const bool stale =
        !seed_found ||
        input_files_found != rac_option2_request_.input_file_numbers.size();
    if (stale) {
      rac_option2_request_.stale = true;
      rac_option2_request_.invalidation_reason =
          seed_found ? "physical_input_changed" : "seed_changed";
      ROCKS_LOG_INFO(
          ioptions_.logger,
          "RAC_OPTION2 request_stale cf=%s request=%" PRIu64
          " reason=%s found_inputs=%" ROCKSDB_PRIszt
          " expected_inputs=%" ROCKSDB_PRIszt,
          GetName().c_str(), rac_option2_request_.request_id,
          seed_found ? "physical_input_changed" : "seed_changed",
          input_files_found, rac_option2_request_.input_file_numbers.size());
    }
    return stale;
  }
  if (ioptions_.compaction_style != kCompactionStyleLevel) {
    return true;
  }
  const int current_output_level =
      rac_option2_request_.source_level == 0
          ? current_->storage_info()->base_level()
          : rac_option2_request_.source_level + 1;
  if (current_output_level != rac_option2_request_.output_level) return true;
  for (FileMetaData* file : current_->storage_info()->LevelFiles(
           rac_option2_request_.source_level)) {
    if (file->fd.GetNumber() == rac_option2_request_.seed_file_number) {
      return !file->marked_for_compaction ||
             file->marked_for_compaction_source !=
                 MarkedForCompactionSource::kRACOption2;
    }
  }
  return true;
}

void ColumnFamilyData::SetRACOption2Request(
    const RACOption2CompactionRequest& request) {
  assert(!rac_option2_request_.active);
  rac_option2_request_ = request;
}

void ColumnFamilyData::ClearRACOption2Request(uint64_t request_id) {
  if (rac_option2_request_.active &&
      rac_option2_request_.request_id == request_id) {
    rac_option2_request_ = RACOption2CompactionRequest();
  }
}

Status ColumnFamilyData::PrepareRACOption2Version(Version* version) {
  if (rac_option2_collector_ == nullptr || version == nullptr) {
    return Status::OK();
  }
  VersionStorageInfo* storage = version->storage_info();
  for (int level = 0; level < storage->num_levels(); ++level) {
    for (FileMetaData* file : storage->LevelFiles(level)) {
      Status status = rac_option2_collector_->RegisterFile(file, level);
      if (!status.ok()) return status;
    }
  }
  return Status::OK();
}

void ColumnFamilyData::TEST_SetRACOption2RegistrationFailure(bool fail) {
  if (rac_option2_collector_ != nullptr) {
    rac_option2_collector_->TEST_SetRegistrationFailure(fail);
  }
}


uint64_t ColumnFamilyData::GetNumLiveVersions() const {
  return VersionSet::GetNumLiveVersions(dummy_versions_);
}

uint64_t ColumnFamilyData::GetTotalSstFilesSize() const {
  return VersionSet::GetTotalSstFilesSize(dummy_versions_);
}

uint64_t ColumnFamilyData::GetTotalBlobFileSize() const {
  return VersionSet::GetTotalBlobFileSize(dummy_versions_);
}

uint64_t ColumnFamilyData::GetLiveSstFilesSize() const {
  return current_->GetSstFilesSize();
}

MemTable* ColumnFamilyData::ConstructNewMemtable(
    const MutableCFOptions& mutable_cf_options, SequenceNumber earliest_seq) {
  return new MemTable(internal_comparator_, ioptions_, mutable_cf_options,
                      write_buffer_manager_, earliest_seq, id_);
}

void ColumnFamilyData::CreateNewMemtable(SequenceNumber earliest_seq) {
  if (mem_ != nullptr) {
    delete mem_->Unref();
  }
  // NOTE: db mutex must be locked for SetMemtable, so safe for
  // GetLatestMutableCFOptions
  SetMemtable(ConstructNewMemtable(GetLatestMutableCFOptions(), earliest_seq));
  mem_->Ref();
}

bool ColumnFamilyData::NeedsCompaction() const {
  bool pending_universal_rac =
      rac_option2_request_.active &&
      ioptions_.compaction_style == kCompactionStyleUniversal;
  if (pending_universal_rac) {
    for (int level = 0; level < NumberLevels() && pending_universal_rac;
         ++level) {
      for (const FileMetaData* file :
           current_->storage_info()->LevelFiles(level)) {
        if (file->being_compacted &&
            std::binary_search(rac_option2_request_.input_file_numbers.begin(),
                               rac_option2_request_.input_file_numbers.end(),
                               file->fd.GetNumber())) {
          pending_universal_rac = false;
          break;
        }
      }
    }
  }
  return !mutable_cf_options_.disable_auto_compactions &&
         (compaction_picker_->NeedsCompaction(current_->storage_info()) ||
          pending_universal_rac);
}

Compaction* ColumnFamilyData::PickCompaction(
    const MutableCFOptions& mutable_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& existing_snapshots,
    const SnapshotChecker* snapshot_checker, LogBuffer* log_buffer,
    bool require_max_output_level) {
  Compaction* result = nullptr;
  if (rac_option2_request_.active &&
      ioptions_.compaction_style == kCompactionStyleLevel &&
      !require_max_output_level) {
    FileMetaData* seed = nullptr;
    if (rac_option2_request_.source_level >= 0 &&
        rac_option2_request_.source_level < NumberLevels()) {
      for (FileMetaData* file : current_->storage_info()->LevelFiles(
               rac_option2_request_.source_level)) {
        if (file->fd.GetNumber() == rac_option2_request_.seed_file_number) {
          seed = file;
          break;
        }
      }
    }
    result = static_cast<LevelCompactionPicker*>(compaction_picker_.get())
                 ->PickRACOption2Compaction(
                     GetName(), mutable_options, mutable_db_options,
                     existing_snapshots, snapshot_checker,
                     current_->storage_info(), log_buffer,
                     GetFullHistoryTsLow(), seed,
                     rac_option2_request_.source_level,
                     rac_option2_request_.output_level);
  } else {
    result = compaction_picker_->PickCompaction(
        GetName(), mutable_options, mutable_db_options, existing_snapshots,
        snapshot_checker, current_->storage_info(), log_buffer,
        GetFullHistoryTsLow(), require_max_output_level);
    if (result == nullptr && rac_option2_request_.active &&
        ioptions_.compaction_style == kCompactionStyleUniversal &&
        (!require_max_output_level ||
         rac_option2_request_.output_level ==
             current_->storage_info()->MaxOutputLevel(
                 ioptions_.allow_ingest_behind))) {
      FileMetaData* seed = nullptr;
      if (rac_option2_request_.source_level >= 0 &&
          rac_option2_request_.source_level < NumberLevels()) {
        for (FileMetaData* file : current_->storage_info()->LevelFiles(
                 rac_option2_request_.source_level)) {
          if (file->fd.GetNumber() == rac_option2_request_.seed_file_number) {
            seed = file;
            break;
          }
        }
      }
      UniversalRACCompactionPlan plan;
      auto* universal_picker = static_cast<UniversalCompactionPicker*>(
          compaction_picker_.get());
      if (universal_picker->BuildRACOption2CompactionPlan(
              GetName(), mutable_options, mutable_db_options,
              existing_snapshots, snapshot_checker, current_->storage_info(),
              log_buffer, GetFullHistoryTsLow(), seed,
              rac_option2_request_.source_level,
              true /* require_rac_mark */, &plan)) {
        std::vector<int> physical_input_levels;
        std::vector<uint64_t> physical_input_file_numbers;
        for (const auto& input_level : plan.inputs) {
          for (const FileMetaData* file : input_level.files) {
            physical_input_levels.push_back(input_level.level);
            physical_input_file_numbers.push_back(file->fd.GetNumber());
          }
        }
        if (plan.source_level == rac_option2_request_.source_level &&
            plan.output_level == rac_option2_request_.output_level &&
            physical_input_levels ==
                rac_option2_request_.physical_input_levels &&
            physical_input_file_numbers ==
                rac_option2_request_.physical_input_file_numbers) {
          result = universal_picker->PickRACOption2Compaction(
              GetName(), mutable_options, mutable_db_options,
              existing_snapshots, snapshot_checker, current_->storage_info(),
              log_buffer, GetFullHistoryTsLow(), plan);
        } else {
          ROCKS_LOG_INFO(
              ioptions_.logger,
              "RAC_OPTION2 universal_request_rejected cf=%s request=%" PRIu64
              " reason=physical_plan_changed",
              GetName().c_str(), rac_option2_request_.request_id);
          rac_option2_request_.invalidated = true;
          rac_option2_request_.stale = true;
          rac_option2_request_.invalidation_reason = "physical_plan_changed";
        }
      } else {
        ROCKS_LOG_INFO(
            ioptions_.logger,
            "RAC_OPTION2 universal_request_rejected cf=%s request=%" PRIu64
            " reason=replan_failed",
            GetName().c_str(), rac_option2_request_.request_id);
        rac_option2_request_.invalidated = true;
        rac_option2_request_.stale = true;
        rac_option2_request_.invalidation_reason =
            "registered_conflict_or_replan_failed";
      }
      if (result == nullptr) {
        ROCKS_LOG_INFO(
            ioptions_.logger,
            "RAC_OPTION2 universal_request_rejected cf=%s request=%" PRIu64
            " reason=execution_validation_failed",
            GetName().c_str(), rac_option2_request_.request_id);
        rac_option2_request_.invalidated = true;
        rac_option2_request_.stale = true;
        if (rac_option2_request_.invalidation_reason.empty()) {
          rac_option2_request_.invalidation_reason =
              "execution_validation_failed";
        }
      }
    }
  }
  return FinalizePickedCompaction(result);
}

Compaction* ColumnFamilyData::PickRACOption2CompactionOnly(
    const MutableCFOptions& mutable_options,
    const MutableDBOptions& mutable_db_options, LogBuffer* log_buffer) {
  const std::vector<SequenceNumber> no_snapshots;
  return PickRACOption2CompactionOnly(mutable_options, mutable_db_options,
                                     no_snapshots, nullptr, log_buffer);
}

Compaction* ColumnFamilyData::PickRACOption2CompactionOnly(
    const MutableCFOptions& mutable_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& existing_snapshots,
    const SnapshotChecker* snapshot_checker, LogBuffer* log_buffer) {
  if (!rac_option2_request_.active ||
      ioptions_.compaction_style != kCompactionStyleLevel) {
    return nullptr;
  }
  FileMetaData* seed = nullptr;
  if (rac_option2_request_.source_level >= 0 &&
      rac_option2_request_.source_level < NumberLevels()) {
    for (FileMetaData* file : current_->storage_info()->LevelFiles(
             rac_option2_request_.source_level)) {
      if (file->fd.GetNumber() == rac_option2_request_.seed_file_number) {
        seed = file;
        break;
      }
    }
  }
  Compaction* result =
      static_cast<LevelCompactionPicker*>(compaction_picker_.get())
          ->PickRACOption2CompactionOnly(
              GetName(), mutable_options, mutable_db_options,
              existing_snapshots, snapshot_checker,
              current_->storage_info(), log_buffer, GetFullHistoryTsLow(),
              seed, rac_option2_request_.source_level,
              rac_option2_request_.output_level);
  return FinalizePickedCompaction(result);
}

Compaction* ColumnFamilyData::FinalizePickedCompaction(Compaction* result) {
  if (result != nullptr) {
    result->FinalizeInputInfo(current_);
    if (result->marked_for_compaction_source() ==
        MarkedForCompactionSource::kRACOption2) {
      const auto& request = GetRACOption2Request();
      RACCompactionMetadata metadata;
      metadata.option = 2;
      metadata.column_family_id = GetID();
      metadata.request_id = request.request_id;
      metadata.evidence_epoch = request.evidence_epoch;
      metadata.source_level = result->start_level();
      metadata.output_level = result->output_level();
      metadata.read_only_window = request.read_only_window;
      metadata.targets_rac_bottom = request.targets_current_bottom;
      bool first = true;
      for (const auto& input_level : *result->inputs()) {
        for (const auto* file : input_level.files) {
          if (first || internal_comparator_.Compare(file->smallest,
                                                    metadata.smallest) < 0) {
            metadata.smallest = file->smallest;
          }
          if (first || internal_comparator_.Compare(file->largest,
                                                    metadata.largest) > 0) {
            metadata.largest = file->largest;
          }
          metadata.physical_input_levels.push_back(input_level.level);
          metadata.physical_input_file_numbers.push_back(
              file->fd.GetNumber());
          metadata.input_file_numbers.push_back(file->fd.GetNumber());
          metadata.actual_input_bytes += file->fd.GetFileSize();
          first = false;
        }
      }
      std::sort(metadata.input_file_numbers.begin(),
                metadata.input_file_numbers.end());
      metadata.valid = !first && request.active && metadata.request_id != 0 &&
                       metadata.source_level == request.source_level &&
                       metadata.output_level == request.output_level &&
                       metadata.input_file_numbers == request.input_file_numbers &&
                       metadata.physical_input_levels ==
                           request.physical_input_levels &&
                       metadata.physical_input_file_numbers ==
                           request.physical_input_file_numbers;
      result->set_rac_metadata(metadata);
    }
  }
  return result;
}

bool ColumnFamilyData::RangeOverlapWithCompaction(
    const Slice& smallest_user_key, const Slice& largest_user_key,
    int level) const {
  return compaction_picker_->RangeOverlapWithCompaction(
      smallest_user_key, largest_user_key, level);
}

Status ColumnFamilyData::RangesOverlapWithMemtables(
    const autovector<UserKeyRange>& ranges, SuperVersion* super_version,
    bool allow_data_in_errors, bool* overlap) {
  assert(overlap != nullptr);
  *overlap = false;
  // Create an InternalIterator over all unflushed memtables
  Arena arena;
  // TODO: plumb Env::IOActivity, Env::IOPriority
  ReadOptions read_opts;
  read_opts.total_order_seek = true;
  MergeIteratorBuilder merge_iter_builder(&internal_comparator_, &arena);
  merge_iter_builder.AddIterator(super_version->mem->NewIterator(
      read_opts, /*seqno_to_time_mapping=*/nullptr, &arena,
      /*prefix_extractor=*/nullptr, /*for_flush=*/false));
  super_version->imm->AddIterators(read_opts, /*seqno_to_time_mapping=*/nullptr,
                                   /*prefix_extractor=*/nullptr,
                                   &merge_iter_builder,
                                   false /* add_range_tombstone_iter */);
  ScopedArenaPtr<InternalIterator> memtable_iter(merge_iter_builder.Finish());

  auto read_seq = super_version->current->version_set()->LastSequence();
  ReadRangeDelAggregator range_del_agg(&internal_comparator_, read_seq);
  auto* active_range_del_iter = super_version->mem->NewRangeTombstoneIterator(
      read_opts, read_seq, false /* immutable_memtable */);
  range_del_agg.AddTombstones(
      std::unique_ptr<FragmentedRangeTombstoneIterator>(active_range_del_iter));
  Status status;
  status = super_version->imm->AddRangeTombstoneIterators(
      read_opts, nullptr /* arena */, &range_del_agg);
  // AddRangeTombstoneIterators always return Status::OK.
  assert(status.ok());

  for (size_t i = 0; i < ranges.size() && status.ok() && !*overlap; ++i) {
    auto* vstorage = super_version->current->storage_info();
    auto* ucmp = vstorage->InternalComparator()->user_comparator();
    InternalKey range_start(ranges[i].start, kMaxSequenceNumber,
                            kValueTypeForSeek);
    memtable_iter->Seek(range_start.Encode());
    status = memtable_iter->status();
    ParsedInternalKey seek_result;

    if (status.ok() && memtable_iter->Valid()) {
      status = ParseInternalKey(memtable_iter->key(), &seek_result,
                                allow_data_in_errors);
    }

    if (status.ok()) {
      if (memtable_iter->Valid() &&
          ucmp->CompareWithoutTimestamp(seek_result.user_key,
                                        ranges[i].limit) <= 0) {
        *overlap = true;
      } else if (range_del_agg.IsRangeOverlapped(ranges[i].start,
                                                 ranges[i].limit)) {
        *overlap = true;
      }
    }
  }
  return status;
}

const int ColumnFamilyData::kCompactAllLevels = -1;
const int ColumnFamilyData::kCompactToBaseLevel = -2;

Compaction* ColumnFamilyData::CompactRange(
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, int input_level,
    int output_level, const CompactRangeOptions& compact_range_options,
    const InternalKey* begin, const InternalKey* end,
    InternalKey** compaction_end, bool* conflict,
    uint64_t max_file_num_to_ignore, const std::string& trim_ts) {
  auto* result = compaction_picker_->PickCompactionForCompactRange(
      GetName(), mutable_cf_options, mutable_db_options,
      current_->storage_info(), input_level, output_level,
      compact_range_options, begin, end, compaction_end, conflict,
      max_file_num_to_ignore, trim_ts, GetFullHistoryTsLow());
  if (result != nullptr) {
    result->FinalizeInputInfo(current_);
  }
  TEST_SYNC_POINT("ColumnFamilyData::CompactRange:Return");
  return result;
}

SuperVersion* ColumnFamilyData::GetReferencedSuperVersion(DBImpl* db) {
  SuperVersion* sv = GetThreadLocalSuperVersion(db);
  sv->Ref();
  if (!ReturnThreadLocalSuperVersion(sv)) {
    // This Unref() corresponds to the Ref() in GetThreadLocalSuperVersion()
    // when the thread-local pointer was populated. So, the Ref() earlier in
    // this function still prevents the returned SuperVersion* from being
    // deleted out from under the caller.
    sv->Unref();
  }
  return sv;
}

SuperVersion* ColumnFamilyData::GetThreadLocalSuperVersion(DBImpl* db) {
  // The SuperVersion is cached in thread local storage to avoid acquiring
  // mutex when SuperVersion does not change since the last use. When a new
  // SuperVersion is installed, the compaction or flush thread cleans up
  // cached SuperVersion in all existing thread local storage. To avoid
  // acquiring mutex for this operation, we use atomic Swap() on the thread
  // local pointer to guarantee exclusive access. If the thread local pointer
  // is being used while a new SuperVersion is installed, the cached
  // SuperVersion can become stale. In that case, the background thread would
  // have swapped in kSVObsolete. We re-check the value at when returning
  // SuperVersion back to thread local, with an atomic compare and swap.
  // The superversion will need to be released if detected to be stale.
  void* ptr = local_sv_->Swap(SuperVersion::kSVInUse);
  // Invariant:
  // (1) Scrape (always) installs kSVObsolete in ThreadLocal storage
  // (2) the Swap above (always) installs kSVInUse, ThreadLocal storage
  // should only keep kSVInUse before ReturnThreadLocalSuperVersion call
  // (if no Scrape happens).
  assert(ptr != SuperVersion::kSVInUse);
  SuperVersion* sv = static_cast<SuperVersion*>(ptr);
  if (sv == SuperVersion::kSVObsolete) {
    RecordTick(ioptions_.stats, NUMBER_SUPERVERSION_ACQUIRES);
    db->mutex()->Lock();
    sv = super_version_->Ref();
    db->mutex()->Unlock();
  }
  assert(sv != nullptr);
  return sv;
}

bool ColumnFamilyData::ReturnThreadLocalSuperVersion(SuperVersion* sv) {
  assert(sv != nullptr);
  // Put the SuperVersion back
  void* expected = SuperVersion::kSVInUse;
  if (local_sv_->CompareAndSwap(static_cast<void*>(sv), expected)) {
    // When we see kSVInUse in the ThreadLocal, we are sure ThreadLocal
    // storage has not been altered and no Scrape has happened. The
    // SuperVersion is still current.
    return true;
  } else {
    // ThreadLocal scrape happened in the process of this GetImpl call (after
    // thread local Swap() at the beginning and before CompareAndSwap()).
    // This means the SuperVersion it holds is obsolete.
    assert(expected == SuperVersion::kSVObsolete);
  }
  return false;
}

void ColumnFamilyData::InstallSuperVersion(
    SuperVersionContext* sv_context, InstrumentedMutex* db_mutex,
    std::optional<std::shared_ptr<SeqnoToTimeMapping>>
        new_seqno_to_time_mapping) {
  db_mutex->AssertHeld();

  SuperVersion* new_superversion = sv_context->new_superversion.release();
  new_superversion->mutable_cf_options = GetLatestMutableCFOptions();
  new_superversion->Init(this, mem_, imm_.current(), current_,
                         new_seqno_to_time_mapping.has_value()
                             ? std::move(new_seqno_to_time_mapping.value())
                         : super_version_
                             ? super_version_->ShareSeqnoToTimeMapping()
                             : nullptr);
  SuperVersion* old_superversion = super_version_;
  super_version_ = new_superversion;
  if (old_superversion == nullptr || old_superversion->current != current() ||
      old_superversion->mem != mem_ ||
      old_superversion->imm != imm_.current()) {
    // Should not recalculate slow down condition if nothing has changed, since
    // currently RecalculateWriteStallConditions() treats it as further slowing
    // down is needed.
    super_version_->write_stall_condition =
        RecalculateWriteStallConditions(new_superversion->mutable_cf_options);
  } else {
    super_version_->write_stall_condition =
        old_superversion->write_stall_condition;
  }
  if (old_superversion != nullptr) {
    // Reset SuperVersions cached in thread local storage.
    // This should be done before old_superversion->Unref(). That's to ensure
    // that local_sv_ never holds the last reference to SuperVersion, since
    // it has no means to safely do SuperVersion cleanup.
    ResetThreadLocalSuperVersions();

    if (old_superversion->mutable_cf_options.write_buffer_size !=
        new_superversion->mutable_cf_options.write_buffer_size) {
      mem_->UpdateWriteBufferSize(
          new_superversion->mutable_cf_options.write_buffer_size);
    }
    if (old_superversion->write_stall_condition !=
        new_superversion->write_stall_condition) {
      sv_context->PushWriteStallNotification(
          old_superversion->write_stall_condition,
          new_superversion->write_stall_condition, GetName(), &ioptions());
    }
    if (old_superversion->Unref()) {
      old_superversion->Cleanup();
      sv_context->superversions_to_free.push_back(old_superversion);
    }
  }
  ++super_version_number_;
  super_version_->version_number = super_version_number_;
}

void ColumnFamilyData::ResetThreadLocalSuperVersions() {
  autovector<void*> sv_ptrs;
  local_sv_->Scrape(&sv_ptrs, SuperVersion::kSVObsolete);
  for (auto ptr : sv_ptrs) {
    assert(ptr);
    if (ptr == SuperVersion::kSVInUse) {
      continue;
    }
    auto sv = static_cast<SuperVersion*>(ptr);
    bool was_last_ref __attribute__((__unused__));
    was_last_ref = sv->Unref();
    // sv couldn't have been the last reference because
    // ResetThreadLocalSuperVersions() is called before
    // unref'ing super_version_.
    assert(!was_last_ref);
  }
}

Status ColumnFamilyData::ValidateOptions(
    const DBOptions& db_options, const ColumnFamilyOptions& cf_options) {
  Status s;
  s = CheckCompressionSupported(cf_options);
  if (s.ok() && db_options.allow_concurrent_memtable_write) {
    s = CheckConcurrentWritesSupported(cf_options);
  }
  if (s.ok() && db_options.unordered_write &&
      cf_options.max_successive_merges != 0) {
    s = Status::InvalidArgument(
        "max_successive_merges > 0 is incompatible with unordered_write");
  }
  if (s.ok()) {
    s = CheckCFPathsSupported(db_options, cf_options);
  }
  if (!s.ok()) {
    return s;
  }

  if (cf_options.ttl > 0 && cf_options.ttl != kDefaultTtl) {
    if (!cf_options.table_factory->IsInstanceOf(
            TableFactory::kBlockBasedTableName())) {
      return Status::NotSupported(
          "TTL is only supported in Block-Based Table format. ");
    }
  }

  if (cf_options.periodic_compaction_seconds > 0 &&
      cf_options.periodic_compaction_seconds != kDefaultPeriodicCompSecs) {
    if (!cf_options.table_factory->IsInstanceOf(
            TableFactory::kBlockBasedTableName())) {
      return Status::NotSupported(
          "Periodic Compaction is only supported in "
          "Block-Based Table format. ");
    }
  }

  const auto* ucmp = cf_options.comparator;
  assert(ucmp);
  if (cf_options.enable_blob_direct_write) {
    if (db_options.enable_pipelined_write) {
      return Status::NotSupported(
          "Blob direct write v1 does not support pipelined writes.");
    }
    if (db_options.allow_concurrent_memtable_write) {
      return Status::NotSupported(
          "Blob direct write v1 does not support concurrent memtable writes.");
    }
    if (db_options.unordered_write) {
      return Status::NotSupported(
          "Blob direct write v1 does not support unordered writes.");
    }
    if (db_options.two_write_queues) {
      return Status::NotSupported(
          "Blob direct write v1 does not support two write queues.");
    }
    if (cf_options.experimental_mempurge_threshold > 0.0) {
      return Status::NotSupported(
          "Blob direct write does not support MemPurge.");
    }
    if (ucmp->timestamp_size() > 0) {
      return Status::NotSupported(
          "Blob direct write does not support user-defined timestamps.");
    }
  }
  if (ucmp->timestamp_size() > 0 &&
      !cf_options.persist_user_defined_timestamps) {
    if (db_options.atomic_flush) {
      return Status::NotSupported(
          "Not persisting user-defined timestamps feature is not supported"
          "in combination with atomic flush.");
    }
    if (db_options.allow_concurrent_memtable_write) {
      return Status::NotSupported(
          "Not persisting user-defined timestamps feature is not supported"
          " in combination with concurrent memtable write.");
    }
    const char* comparator_name = cf_options.comparator->Name();
    size_t name_size = strlen(comparator_name);
    const char* suffix = ".u64ts";
    size_t suffix_size = strlen(suffix);
    if (name_size <= suffix_size ||
        strcmp(comparator_name + name_size - suffix_size, suffix) != 0) {
      return Status::NotSupported(
          "Not persisting user-defined timestamps"
          "feature only support user-defined timestamps formatted as "
          "uint64_t.");
    }
  }

  if (cf_options.enable_blob_garbage_collection) {
    if (cf_options.blob_garbage_collection_age_cutoff < 0.0 ||
        cf_options.blob_garbage_collection_age_cutoff > 1.0) {
      return Status::InvalidArgument(
          "The age cutoff for blob garbage collection should be in the range "
          "[0.0, 1.0].");
    }
    if (cf_options.blob_garbage_collection_force_threshold < 0.0 ||
        cf_options.blob_garbage_collection_force_threshold > 1.0) {
      return Status::InvalidArgument(
          "The garbage ratio threshold for forcing blob garbage collection "
          "should be in the range [0.0, 1.0].");
    }
  }

  if (cf_options.read_triggered_compaction_threshold < 0.0 ||
      std::isnan(cf_options.read_triggered_compaction_threshold) ||
      std::isinf(cf_options.read_triggered_compaction_threshold)) {
    return Status::InvalidArgument(
        "read_triggered_compaction_threshold must be >= 0.0 and finite. "
        "Use 0.0 to disable.");
  }

  if (cf_options.compaction_style == kCompactionStyleFIFO &&
      db_options.max_open_files != -1 && cf_options.ttl > 0) {
    return Status::NotSupported(
        "FIFO compaction only supported with max_open_files = -1.");
  }

  if (db_options.open_files_async) {
    // FIFO TTL picker relies on reading the files table properties inside a
    // DB mutex. This can be slow if files are opened asynchronously, so we
    // disable it.
    //
    // TODO: consider blocking fifo compaction until async file open task
    // completes.
    if (cf_options.compaction_style == kCompactionStyleFIFO) {
      return Status::NotSupported(
          "FIFO compaction is not supported with open_files_async = true.");
    }
    // Open files async is not useful if skip_stats_update_on_db_open=true
    // because DB open will still block on IO.
    //
    // TODO: consider moving stats update inside async file open background
    // task.
    if (!db_options.skip_stats_update_on_db_open) {
      return Status::InvalidArgument(
          "open_files_async requires skip_stats_update_on_db_open = true.");
    }
  }

  std::vector<uint32_t> supported{0, 1, 2, 4, 8};
  if (std::find(supported.begin(), supported.end(),
                cf_options.memtable_protection_bytes_per_key) ==
      supported.end()) {
    return Status::NotSupported(
        "Memtable per key-value checksum protection only supports 0, 1, 2, 4 "
        "or 8 bytes per key.");
  }
  if (std::find(supported.begin(), supported.end(),
                cf_options.block_protection_bytes_per_key) == supported.end()) {
    return Status::NotSupported(
        "Block per key-value checksum protection only supports 0, 1, 2, 4 "
        "or 8 bytes per key.");
  }

  if (!cf_options.compaction_options_fifo.file_temperature_age_thresholds
           .empty()) {
    if (cf_options.compaction_style != kCompactionStyleFIFO) {
      return Status::NotSupported(
          "Option file_temperature_age_thresholds only supports FIFO "
          "compaction.");
    } else if (cf_options.num_levels > 1) {
      return Status::NotSupported(
          "Option file_temperature_age_thresholds is only supported when "
          "num_levels = 1.");
    } else {
      const auto& ages =
          cf_options.compaction_options_fifo.file_temperature_age_thresholds;
      assert(ages.size() >= 1);
      // check that age is sorted
      for (size_t i = 0; i < ages.size() - 1; ++i) {
        if (ages[i].age >= ages[i + 1].age) {
          return Status::NotSupported(
              "Option file_temperature_age_thresholds requires elements to be "
              "sorted in increasing order with respect to `age` field.");
        }
      }
    }
  }

  if (cf_options.compaction_style == kCompactionStyleUniversal) {
    int max_read_amp = cf_options.compaction_options_universal.max_read_amp;
    if (max_read_amp < -1) {
      return Status::NotSupported(
          "CompactionOptionsUniversal::max_read_amp should be at least -1.");
    } else if (0 < max_read_amp &&
               max_read_amp < cf_options.level0_file_num_compaction_trigger) {
      return Status::NotSupported(
          "CompactionOptionsUniversal::max_read_amp limits the number of sorted"
          " runs but is smaller than the compaction trigger "
          "level0_file_num_compaction_trigger.");
    }
  }
  if (cf_options.rac_design_option != 1 &&
      cf_options.rac_design_option != 2 &&
      cf_options.rac_design_option != 3) {
    return Status::InvalidArgument("rac_design_option must be 1, 2, or 3");
  }
  if (cf_options.rac2_ablation > 5) {
    return Status::InvalidArgument("rac2_ablation must be in [0, 5]");
  }
  if (cf_options.rac2_ablation != 0) {
    if (cf_options.rac_design_option != 3) {
      return Status::InvalidArgument(
          "nonzero rac2_ablation requires rac_design_option=3");
    }
    if (cf_options.rac_option2_dual_purpose_selection) {
      return Status::InvalidArgument(
          "nonzero rac2_ablation requires dual-purpose selection to be "
          "disabled");
    }
  }
  if (cf_options.rac2_profiling_only &&
      (!cf_options.enable_read_triggered_compaction ||
       cf_options.rac_design_option != 3 || cf_options.rac2_ablation != 0 ||
       cf_options.rac_option2_dual_purpose_selection)) {
    return Status::InvalidArgument(
        "rac2_profiling_only requires read-triggered compaction enabled, "
        "rac_design_option=3, rac2_ablation=0, and dual-purpose selection "
        "disabled");
  }
  if (cf_options.rac_design_option == 3) {
    if (!std::isfinite(cf_options.rac2_hotspot_persistence) ||
        cf_options.rac2_hotspot_persistence < 0.0 ||
        cf_options.rac2_hotspot_persistence > 1.0) {
      return Status::InvalidArgument(
          "rac2_hotspot_persistence must be finite and in [0, 1]");
    }
    if (cf_options.rac2_prediction_horizon == 0 ||
        cf_options.rac2_prediction_horizon > 1000) {
      return Status::InvalidArgument(
          "rac2_prediction_horizon must be in [1, 1000]");
    }
    if (!std::isfinite(cf_options.rac2_compaction_bandwidth_mib_per_sec) ||
        cf_options.rac2_compaction_bandwidth_mib_per_sec <= 0.0) {
      return Status::InvalidArgument(
          "rac2_compaction_bandwidth_mib_per_sec must be finite and "
          "positive");
    }
    if (!std::isfinite(cf_options.rac2_cost_weight) ||
        cf_options.rac2_cost_weight <= 0.0) {
      return Status::InvalidArgument(
          "rac2_cost_weight must be finite and positive");
    }
  }
  if (cf_options.rac_window_size == 0 ||
      cf_options.rac_window_size > kRACMaxWindowSize) {
    return Status::InvalidArgument(
        "rac_window_size must be in [1, 5000000]");
  }
  if (cf_options.rac_top_k == 0) {
    return Status::InvalidArgument("rac_top_k must be at least 1");
  }
  if (cf_options.rac_option2_dp_projection_top_k == 0 ||
      cf_options.rac_option2_dp_projection_top_k > 50) {
    return Status::InvalidArgument(
        "rac_option2_dp_projection_top_k must be in [1, 50]");
  }
  if (!std::isfinite(cf_options.rac_benefit_efficiency_threshold) ||
      cf_options.rac_benefit_efficiency_threshold < 0.0) {
    return Status::InvalidArgument(
        "rac_benefit_efficiency_threshold must be finite and non-negative");
  }
  if (!std::isfinite(cf_options.rac_high_miss_ratio_threshold) ||
      cf_options.rac_high_miss_ratio_threshold < 0.0 ||
      cf_options.rac_high_miss_ratio_threshold > 1.0) {
    return Status::InvalidArgument(
        "rac_high_miss_ratio_threshold must be finite and in [0, 1]");
  }
  if (!std::isfinite(cf_options.rac_option2_l0_efficiency_threshold) ||
      cf_options.rac_option2_l0_efficiency_threshold < 0.0) {
    return Status::InvalidArgument(
        "rac_option2_l0_efficiency_threshold must be finite and "
        "non-negative");
  }
  if (cf_options.rac_option2_l0_cooldown_windows == 0) {
    return Status::InvalidArgument(
        "rac_option2_l0_cooldown_windows must be at least 1");
  }
  if (!std::isfinite(cf_options.rac_bottom_overlap_threshold) ||
      cf_options.rac_bottom_overlap_threshold < 0.0) {
    return Status::InvalidArgument(
        "rac_bottom_overlap_threshold must be finite and non-negative");
  }
  if (cf_options.rac_cooldown_windows == 0) {
    return Status::InvalidArgument("rac_cooldown_windows must be at least 1");
  }
  if (!std::isfinite(cf_options.rac_option2_read_benefit_slack) ||
      cf_options.rac_option2_read_benefit_slack <= 0.0 ||
      cf_options.rac_option2_read_benefit_slack > 1.0) {
    return Status::InvalidArgument(
        "rac_option2_read_benefit_slack must be finite and in (0, 1]");
  }
  if (!std::isfinite(cf_options.rac_option2_emerging_score_floor) ||
      cf_options.rac_option2_emerging_score_floor < 0.0 ||
      cf_options.rac_option2_emerging_score_floor >= 1.0) {
    return Status::InvalidArgument(
        "rac_option2_emerging_score_floor must be finite and in [0, 1)");
  }
  if (cf_options.enable_read_triggered_compaction &&
      (cf_options.rac_design_option == 2 ||
       cf_options.rac_design_option == 3)) {
    if (cf_options.num_levels > 64) {
      return Status::InvalidArgument("RAC option2 supports at most 64 levels");
    }
    if (cf_options.disable_auto_compactions) {
      return Status::InvalidArgument(
          "RAC option2 requires automatic compactions");
    }
    if (cf_options.compaction_style != kCompactionStyleLevel &&
        cf_options.compaction_style != kCompactionStyleUniversal) {
      return Status::InvalidArgument(
          "RAC option2 requires leveled or universal compaction");
    }
    Status collector_status = RACOption2Collector::ValidateConfiguration(
        cf_options.rac_window_size, cf_options.num_levels);
    if (!collector_status.ok()) {
      return collector_status;
    }
  }
  return s;
}

Status ColumnFamilyData::SetOptions(
    const DBOptions& db_opts,
    const std::unordered_map<std::string, std::string>& options_map) {
  static const std::unordered_set<std::string> kImmutableRACOptions = {
      "rac_design_option", "rac_window_size", "rac_top_k",
      "rac2_hotspot_persistence", "rac2_prediction_horizon",
      "rac2_compaction_bandwidth_mib_per_sec", "rac2_cost_weight",
      "rac2_ablation",
      "rac2_profiling_only",
      "rac_high_miss_ratio_threshold",
      "rac_benefit_efficiency_threshold",
      "rac_option2_l0_efficiency_threshold",
      "rac_option2_l0_cooldown_windows", "rac_bottom_overlap_threshold",
      "rac_cooldown_windows", "rac_option2_dp_projection_top_k",
      "rac_option2_dp_allow_ttl_priority"};
  for (const auto& option : options_map) {
    if (kImmutableRACOptions.count(option.first) != 0) {
      return Status::InvalidArgument("RAC option is immutable for an open CF: " +
                                     option.first);
    }
    if ((GetRACDesignOption() == 2 || GetRACDesignOption() == 3) &&
        (option.first == "enable_read_triggered_compaction" ||
         option.first == "disable_auto_compactions")) {
      return Status::InvalidArgument(
          "RAC option2 activation and automatic compaction are immutable for "
          "an open CF");
    }
  }
  ColumnFamilyOptions cf_opts =
      BuildColumnFamilyOptions(initial_cf_options_, mutable_cf_options_);
  ConfigOptions config_opts;
  config_opts.mutable_options_only = true;
#ifndef NDEBUG
  if (TEST_allowSetOptionsImmutableInMutable) {
    config_opts.mutable_options_only = false;
  }
#endif
  Status s = GetColumnFamilyOptionsFromMap(config_opts, cf_opts, options_map,
                                           &cf_opts);
  if (s.ok()) {
    // FIXME: we should call SanitizeOptions() too or consolidate it with
    // ValidateOptions().
    s = ValidateOptions(db_opts, cf_opts);
  }
  if (s.ok()) {
    mutable_cf_options_ = MutableCFOptions(cf_opts);
    mutable_cf_options_.RefreshDerivedOptions(ioptions_);
  }
  return s;
}

Status ColumnFamilyData::AddDirectories(
    std::map<std::string, std::shared_ptr<FSDirectory>>* created_dirs) {
  Status s;
  assert(created_dirs != nullptr);
  assert(data_dirs_.empty());
  for (auto& p : ioptions_.cf_paths) {
    auto existing_dir = created_dirs->find(p.path);

    if (existing_dir == created_dirs->end()) {
      std::unique_ptr<FSDirectory> path_directory;
      s = DBImpl::CreateAndNewDirectory(ioptions_.fs.get(), p.path,
                                        &path_directory);
      if (!s.ok()) {
        return s;
      }
      assert(path_directory != nullptr);
      data_dirs_.emplace_back(path_directory.release());
      (*created_dirs)[p.path] = data_dirs_.back();
    } else {
      data_dirs_.emplace_back(existing_dir->second);
    }
  }
  assert(data_dirs_.size() == ioptions_.cf_paths.size());
  return s;
}

FSDirectory* ColumnFamilyData::GetDataDir(size_t path_id) const {
  if (data_dirs_.empty()) {
    return nullptr;
  }

  assert(path_id < data_dirs_.size());
  return data_dirs_[path_id].get();
}

void ColumnFamilyData::SetFlushSkipReschedule() {
  const Comparator* ucmp = user_comparator();
  const size_t ts_sz = ucmp->timestamp_size();
  if (ts_sz == 0 || ioptions_.persist_user_defined_timestamps) {
    return;
  }
  flush_skip_reschedule_.store(true);
}

bool ColumnFamilyData::GetAndClearFlushSkipReschedule() {
  return flush_skip_reschedule_.exchange(false);
}

bool ColumnFamilyData::ShouldPostponeFlushToRetainUDT(
    uint64_t max_memtable_id) {
  const Comparator* ucmp = user_comparator();
  const size_t ts_sz = ucmp->timestamp_size();
  if (ts_sz == 0 || ioptions_.persist_user_defined_timestamps) {
    return false;
  }
  // If users set the `persist_user_defined_timestamps` flag to false, they
  // should also set the `full_history_ts_low` flag to indicate the range of
  // user-defined timestamps to retain in memory. Otherwise, we do not
  // explicitly postpone flush to retain UDTs.
  const std::string& full_history_ts_low = GetFullHistoryTsLow();
  if (full_history_ts_low.empty()) {
    return false;
  }
  for (const Slice& table_newest_udt :
       imm()->GetTablesNewestUDT(max_memtable_id)) {
    if (table_newest_udt.empty()) {
      continue;
    }
    assert(table_newest_udt.size() == full_history_ts_low.size());
    // Checking the newest UDT contained in MemTable with ascending ID up to
    // `max_memtable_id`. Return immediately on finding the first MemTable that
    // needs postponing.
    if (ucmp->CompareTimestamp(table_newest_udt, full_history_ts_low) >= 0) {
      return true;
    }
  }
  return false;
}

void ColumnFamilyData::RecoverEpochNumbers() {
  assert(current_);
  auto* vstorage = current_->storage_info();
  assert(vstorage);
  vstorage->RecoverEpochNumbers(this);
}

ColumnFamilySet::ColumnFamilySet(
    const std::string& dbname, const ImmutableDBOptions* db_options,
    const FileOptions& file_options, Cache* table_cache,
    WriteBufferManager* _write_buffer_manager,
    WriteController* _write_controller,
    BlockCacheTracer* const block_cache_tracer,
    const std::shared_ptr<IOTracer>& io_tracer, const std::string& db_id,
    const std::string& db_session_id, bool fast_sst_open)
    : max_column_family_(0),
      file_options_(file_options),
      dummy_cfd_(new ColumnFamilyData(
          ColumnFamilyData::kDummyColumnFamilyDataId, "", nullptr, nullptr,
          nullptr, ColumnFamilyOptions(), *db_options, &file_options_, nullptr,
          block_cache_tracer, io_tracer, db_id, db_session_id,
          /*read_only*/ true)),
      default_cfd_cache_(nullptr),
      db_name_(dbname),
      db_options_(db_options),
      table_cache_(table_cache),
      write_buffer_manager_(_write_buffer_manager),
      write_controller_(_write_controller),
      block_cache_tracer_(block_cache_tracer),
      io_tracer_(io_tracer),
      db_id_(db_id),
      db_session_id_(db_session_id),
      fast_sst_open_(fast_sst_open) {
  // initialize linked list
  dummy_cfd_->prev_ = dummy_cfd_;
  dummy_cfd_->next_ = dummy_cfd_;
}

ColumnFamilySet::~ColumnFamilySet() {
  while (column_family_data_.size() > 0) {
    // cfd destructor will delete itself from column_family_data_
    auto cfd = column_family_data_.begin()->second;
    bool last_ref __attribute__((__unused__));
    last_ref = cfd->UnrefAndTryDelete();
    assert(last_ref);
  }
  bool dummy_last_ref __attribute__((__unused__));
  dummy_last_ref = dummy_cfd_->UnrefAndTryDelete();
  assert(dummy_last_ref);
}

ColumnFamilyData* ColumnFamilySet::GetDefault() const {
  assert(default_cfd_cache_ != nullptr);
  return default_cfd_cache_;
}

ColumnFamilyData* ColumnFamilySet::GetColumnFamily(uint32_t id) const {
  auto cfd_iter = column_family_data_.find(id);
  if (cfd_iter != column_family_data_.end()) {
    return cfd_iter->second;
  } else {
    return nullptr;
  }
}

ColumnFamilyData* ColumnFamilySet::GetColumnFamily(
    const std::string& name) const {
  auto cfd_iter = column_families_.find(name);
  if (cfd_iter != column_families_.end()) {
    auto cfd = GetColumnFamily(cfd_iter->second);
    assert(cfd != nullptr);
    return cfd;
  } else {
    return nullptr;
  }
}

uint32_t ColumnFamilySet::GetNextColumnFamilyID() {
  return ++max_column_family_;
}

uint32_t ColumnFamilySet::GetMaxColumnFamily() { return max_column_family_; }

void ColumnFamilySet::UpdateMaxColumnFamily(uint32_t new_max_column_family) {
  max_column_family_ = std::max(new_max_column_family, max_column_family_);
}

size_t ColumnFamilySet::NumberOfColumnFamilies() const {
  return column_families_.size();
}

// under a DB mutex AND write thread
ColumnFamilyData* ColumnFamilySet::CreateColumnFamily(
    const std::string& name, uint32_t id, Version* dummy_versions,
    const ColumnFamilyOptions& options, bool read_only, Status* status) {
  ColumnFamilyData* new_cfd = PrepareColumnFamily(
      name, id, dummy_versions, options, read_only, status);
  if (new_cfd != nullptr) {
    CommitPreparedColumnFamily(new_cfd);
  }
  return new_cfd;
}

ColumnFamilyData* ColumnFamilySet::PrepareColumnFamily(
    const std::string& name, uint32_t id, Version* dummy_versions,
    const ColumnFamilyOptions& options, bool read_only, Status* status) {
  assert(column_families_.find(name) == column_families_.end());
  ColumnFamilyData* new_cfd = new (std::nothrow) ColumnFamilyData(
      id, name, dummy_versions, table_cache_, write_buffer_manager_, options,
      *db_options_, &file_options_, this, block_cache_tracer_, io_tracer_,
      db_id_, db_session_id_, read_only, fast_sst_open_);
  if (new_cfd == nullptr) {
    if (status != nullptr) {
      *status = Status::MemoryLimit("column family allocation failed");
    }
    if (dummy_versions != nullptr) {
      bool deleted __attribute__((__unused__));
      deleted = dummy_versions->Unref();
      assert(deleted);
    }
    return nullptr;
  }
  if (!new_cfd->GetRACOption2InitializationStatus().ok()) {
    if (status != nullptr) {
      *status = new_cfd->GetRACOption2InitializationStatus();
    }
    DiscardPreparedColumnFamily(new_cfd);
    return nullptr;
  }
  return new_cfd;
}

void ColumnFamilySet::CommitPreparedColumnFamily(
    ColumnFamilyData* new_cfd) {
  assert(new_cfd != nullptr);
  const uint32_t id = new_cfd->GetID();
  const std::string& name = new_cfd->GetName();
  assert(column_families_.find(name) == column_families_.end());
  column_families_.insert({name, id});
  column_family_data_.insert({id, new_cfd});
  auto ucmp = new_cfd->user_comparator();
  assert(ucmp);
  size_t ts_sz = ucmp->timestamp_size();
  running_ts_sz_.insert({id, ts_sz});
  if (ts_sz > 0) {
    ts_sz_for_record_.insert({id, ts_sz});
  }
  max_column_family_ = std::max(max_column_family_, id);
  // add to linked list
  new_cfd->next_ = dummy_cfd_;
  auto prev = dummy_cfd_->prev_;
  new_cfd->prev_ = prev;
  prev->next_ = new_cfd;
  dummy_cfd_->prev_ = new_cfd;
  if (id == 0) {
    default_cfd_cache_ = new_cfd;
  }
}

void ColumnFamilySet::DiscardPreparedColumnFamily(ColumnFamilyData* new_cfd) {
  assert(new_cfd != nullptr);
  // ColumnFamilyData's destructor unlinks itself and removes an undropped CF
  // from the set. A prepared CF has not entered either structure yet.
  new_cfd->prev_ = new_cfd;
  new_cfd->next_ = new_cfd;
  new_cfd->dropped_ = true;
  new_cfd->UnrefAndTryDelete();
}

// under a DB mutex AND from a write thread
void ColumnFamilySet::RemoveColumnFamily(ColumnFamilyData* cfd) {
  uint32_t cf_id = cfd->GetID();
  auto cfd_iter = column_family_data_.find(cf_id);
  assert(cfd_iter != column_family_data_.end());
  column_family_data_.erase(cfd_iter);
  column_families_.erase(cfd->GetName());
  running_ts_sz_.erase(cf_id);
  ts_sz_for_record_.erase(cf_id);
}

// under a DB mutex OR from a write thread
bool ColumnFamilyMemTablesImpl::Seek(uint32_t column_family_id) {
  if (column_family_id == 0) {
    // optimization for common case
    current_ = column_family_set_->GetDefault();
  } else {
    current_ = column_family_set_->GetColumnFamily(column_family_id);
  }
  handle_.SetCFD(current_);
  return current_ != nullptr;
}

uint64_t ColumnFamilyMemTablesImpl::GetLogNumber() const {
  assert(current_ != nullptr);
  return current_->GetLogNumber();
}

MemTable* ColumnFamilyMemTablesImpl::GetMemTable() const {
  assert(current_ != nullptr);
  return current_->mem();
}

ColumnFamilyHandle* ColumnFamilyMemTablesImpl::GetColumnFamilyHandle() {
  assert(current_ != nullptr);
  return &handle_;
}

uint32_t GetColumnFamilyID(ColumnFamilyHandle* column_family) {
  uint32_t column_family_id = 0;
  if (column_family != nullptr) {
    auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
    column_family_id = cfh->GetID();
  }
  return column_family_id;
}

const Comparator* GetColumnFamilyUserComparator(
    ColumnFamilyHandle* column_family) {
  if (column_family != nullptr) {
    return column_family->GetComparator();
  }
  return nullptr;
}

const ImmutableOptions& GetImmutableOptions(ColumnFamilyHandle* column_family) {
  assert(column_family);

  ColumnFamilyHandleImpl* const handle =
      static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  assert(handle);

  const ColumnFamilyData* const cfd = handle->cfd();
  assert(cfd);

  return cfd->ioptions();
}

}  // namespace ROCKSDB_NAMESPACE
