//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "db/compaction/compaction_picker.h"

namespace ROCKSDB_NAMESPACE {

enum class RACOption2ProjectionStatus : unsigned char {
  kValid,
  kUnsupportedStyle,
  kUnsupportedLevel,
  kUnsupportedPriority,
  kMissingScore,
  kPriorityTailUnknown,
  kNoSelectableFile,
  kConflict,
  kTrivialMove,
  kOverflow,
};

enum class RACOption2IntentProvenance : unsigned char {
  kPureSize,
  kMixedMarkedReason,
  kMixedTtlPriority,
  kOtherNonSizeReason,
  kUnknown,
};

// Read-only counterfactual of the leveled size path for one physical source
// level. All input collections are caller-owned and no Compaction is created.
struct RACOption2ProjectedCompaction {
  RACOption2ProjectionStatus status =
      RACOption2ProjectionStatus::kNoSelectableFile;
  RACOption2IntentProvenance provenance =
      RACOption2IntentProvenance::kUnknown;
  uint64_t version_number = 0;
  int source_level = -1;
  int output_level = -1;
  double stored_score = 0.0;
  int global_score_rank = -1;
  bool eligible_now = false;
  bool is_global_next = false;
  size_t absolute_priority_position = 0;
  size_t effective_scan_position = 0;
  bool priority_tail_unknown = false;
  uint64_t projected_seed_file_number = 0;
  bool marked_priority_applied = false;
  bool ttl_priority_possible = false;
  uint64_t ttl_boost_score = 1;
  bool other_compaction_reason_present = false;
  CompactionInputFiles source_inputs;
  CompactionInputFiles target_inputs;
  uint64_t source_input_bytes = 0;
  uint64_t target_input_bytes = 0;
  uint64_t total_unique_input_bytes = 0;
};

struct RACOption2PhysicalInput {
  int level = -1;
  uint64_t file_number = 0;
  uint64_t file_size = 0;
};

struct RACOption2PhysicalAlignment {
  bool valid = false;
  bool exact_full_coverage = false;
  bool exact_physical_equality = false;
  uint64_t shared_source_bytes = 0;
  uint64_t shared_target_bytes = 0;
  uint64_t shared_unique_input_bytes = 0;
  double coverage_n = 0.0;
  double precision_r = 0.0;
};

// Computes typed physical-input identity/byte alignment without consulting or
// mutating picker state. Duplicate identities, inconsistent byte totals, and
// arithmetic overflow fail closed.
RACOption2PhysicalAlignment ComputeRACOption2PhysicalAlignment(
    const std::vector<RACOption2PhysicalInput>& rac_inputs,
    uint64_t rac_total_input_bytes,
    const RACOption2ProjectedCompaction& projection);

// Picking compactions for leveled compaction. See wiki page
// https://github.com/facebook/rocksdb/wiki/Leveled-Compaction
// for description of Leveled compaction.
class LevelCompactionPicker : public CompactionPicker {
 public:
  LevelCompactionPicker(const ImmutableOptions& ioptions,
                        const InternalKeyComparator* icmp)
      : CompactionPicker(ioptions, icmp) {}
  Compaction* PickCompaction(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      const MutableDBOptions& mutable_db_options,
      const std::vector<SequenceNumber>& /* existing_snapshots */,
      const SnapshotChecker* /* snapshot_checker */,
      VersionStorageInfo* vstorage, LogBuffer* log_buffer,
      const std::string& full_history_ts_low,
      bool /*require_max_output_level*/ = false) override;

  // Picks normal score/marked work first, then consumes this exact option-2
  // seed deterministically. The caller validates the fully expanded inputs
  // against its immutable typed request before execution.
  Compaction* PickRACOption2Compaction(
      const std::string& cf_name,
      const MutableCFOptions& mutable_cf_options,
      const MutableDBOptions& mutable_db_options,
      const std::vector<SequenceNumber>& existing_snapshots,
      const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
      LogBuffer* log_buffer, const std::string& full_history_ts_low,
      FileMetaData* rac_seed, int source_level, int output_level);

  // Picks only the active typed RAC request.
  Compaction* PickRACOption2CompactionOnly(
      const std::string& cf_name,
      const MutableCFOptions& mutable_cf_options,
      const MutableDBOptions& mutable_db_options,
      const std::vector<SequenceNumber>& existing_snapshots,
      const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
      LogBuffer* log_buffer, const std::string& full_history_ts_low,
      FileMetaData* rac_seed, int source_level, int output_level);

  void ProjectRACOption2SizeCompaction(
      const std::string& cf_name,
      const MutableCFOptions& mutable_cf_options,
      VersionStorageInfo* vstorage, uint64_t version_number,
      int source_level, RACOption2ProjectedCompaction* projection);

  // Projects up to max_priority_candidates successive write-side priority
  // seeds without mutating picker or version state. Callers must reject
  // non-valid entries such as trivial moves and conflicts.
  void ProjectRACOption2SizeCompactions(
      const std::string& cf_name,
      const MutableCFOptions& mutable_cf_options,
      VersionStorageInfo* vstorage, uint64_t version_number,
      int source_level, size_t max_priority_candidates,
      std::vector<RACOption2ProjectedCompaction>* projections);

  bool NeedsCompaction(const VersionStorageInfo* vstorage) const override;

 private:
  void ProjectRACOption2SizeCompactionFromPriorityPosition(
      const std::string& cf_name,
      const MutableCFOptions& mutable_cf_options,
      VersionStorageInfo* vstorage, uint64_t version_number,
      int source_level, size_t minimum_effective_position,
      RACOption2ProjectedCompaction* projection);
};

}  // namespace ROCKSDB_NAMESPACE
