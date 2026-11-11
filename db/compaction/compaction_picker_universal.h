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
#include "db/snapshot_checker.h"

namespace ROCKSDB_NAMESPACE {

struct UniversalRACCompactionPlan {
  std::vector<CompactionInputFiles> inputs;
  std::vector<FileMetaData*> grandparents;
  FileMetaData* rac_seed = nullptr;
  int source_level = -1;
  int output_level = -1;
  uint32_t output_path_id = 0;
};

class UniversalCompactionPicker : public CompactionPicker {
 public:
  UniversalCompactionPicker(const ImmutableOptions& ioptions,
                            const InternalKeyComparator* icmp)
      : CompactionPicker(ioptions, icmp) {}

  // If `require_max_output_level` is true, only pick compaction
  // with max output level or return nullptr if no such compaction exists.
  Compaction* PickCompaction(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      const MutableDBOptions& mutable_db_options,
      const std::vector<SequenceNumber>& existing_snapshots,
      const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
      LogBuffer* log_buffer, const std::string& full_history_ts_low,
      bool require_max_output_level = false) override;

  // Build a Universal RAC plan without registering a compaction or modifying
  // file/compaction state. When `require_rac_mark` is true, the seed must be
  // marked by RAC Option 2.
  bool BuildRACOption2CompactionPlan(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      const MutableDBOptions& mutable_db_options,
      const std::vector<SequenceNumber>& existing_snapshots,
      const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
      LogBuffer* log_buffer, const std::string& full_history_ts_low,
      FileMetaData* rac_seed, int source_level, bool require_rac_mark,
      UniversalRACCompactionPlan* plan);

  // Validate, construct, and register a compaction from a Universal RAC plan.
  Compaction* PickRACOption2Compaction(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      const MutableDBOptions& mutable_db_options,
      const std::vector<SequenceNumber>& existing_snapshots,
      const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
      LogBuffer* log_buffer, const std::string& full_history_ts_low,
      const UniversalRACCompactionPlan& plan);
  int MaxOutputLevel() const override { return NumberLevels() - 1; }

  bool NeedsCompaction(const VersionStorageInfo* vstorage) const override;
};
}  // namespace ROCKSDB_NAMESPACE
