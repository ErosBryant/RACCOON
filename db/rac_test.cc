// Copyright (c) Meta Platforms, Inc. and affiliates.

#include "db/column_family.h"

#include <limits>
#include <memory>
#include <thread>
#include <vector>

#include "db/db_test_util.h"
#include "rocksdb/merge_operator.h"
#include "test_util/testharness.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

class RACFailingMergeOperator : public MergeOperator {
 public:
  bool FullMergeV2(const MergeOperationInput&,
                   MergeOperationOutput*) const override {
    return false;
  }
  bool PartialMergeMulti(const Slice&, const std::deque<Slice>&,
                         std::string*, Logger*) const override {
    return false;
  }
  const char* Name() const override { return "RACFailingMergeOperator"; }
};

class RACOption2Test : public DBTestBase {
 public:
  RACOption2Test()
      : DBTestBase("rac_option2_test", /*env_do_fsync=*/false) {}

  ColumnFamilyData* cfd() {
    return static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())
        ->cfd();
  }

  void OpenOption2(uint64_t window_size) {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.disable_auto_compactions = false;
    options.enable_read_triggered_compaction = true;
    options.rac_design_option = 2;
    options.rac_window_size = window_size;
    Reopen(options);
  }
};

TEST_F(RACOption2Test, DefaultsAndValidation) {
  ColumnFamilyOptions options;
  ASSERT_EQ(options.rac_design_option, 1U);
  ASSERT_EQ(options.rac_window_size, 200000U);
  ASSERT_EQ(options.rac_top_k, 4U);
  ASSERT_EQ(options.rac_benefit_efficiency_threshold, 1.0);
  ASSERT_EQ(options.rac_option2_l0_efficiency_threshold, 10.0);
  ASSERT_EQ(options.rac_option2_l0_cooldown_windows, 8U);
  ASSERT_EQ(options.rac_bottom_overlap_threshold, 8.0);
  ASSERT_EQ(options.rac_cooldown_windows, 4U);

  DBOptions db_options;
  options.rac_design_option = 3;
  ASSERT_OK(ColumnFamilyData::ValidateOptions(db_options, options));
  options.rac_design_option = 2;
  options.rac_window_size = 0;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.rac_window_size = kRACMaxWindowSize + 1;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.rac_window_size = 1;
  options.enable_read_triggered_compaction = true;
  options.num_levels = 65;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
}

TEST_F(RACOption2Test, ProfilingOnlyRequiresIsolatedRAC3Policy) {
  DBOptions db_options;
  ColumnFamilyOptions options;
  options.rac2_profiling_only = true;

  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.enable_read_triggered_compaction = true;
  options.rac_design_option = 3;
  ASSERT_OK(ColumnFamilyData::ValidateOptions(db_options, options));

  options.rac_option2_dual_purpose_selection = true;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.rac_option2_dual_purpose_selection = false;
  options.rac2_ablation = 1;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
}

TEST_F(RACOption2Test, ReadOnlyConfiguredWindowUsesNinetyFivePercentThreshold) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.enable_read_triggered_compaction = true;
  options.rac_design_option = 2;
  options.rac_window_size = 50000;
  options.rac_high_miss_ratio_threshold = 1.0;
  Reopen(options);

  ASSERT_DOUBLE_EQ(
      0.95, cfd()->GetRACOption2EffectiveHighMissRatioThreshold(
                /*total_reads=*/50000, /*no_committed_writes=*/true));
  ASSERT_DOUBLE_EQ(
      1.0, cfd()->GetRACOption2EffectiveHighMissRatioThreshold(
               /*total_reads=*/50000, /*no_committed_writes=*/false));
  ASSERT_DOUBLE_EQ(
      1.0, cfd()->GetRACOption2EffectiveHighMissRatioThreshold(
               /*total_reads=*/100000, /*no_committed_writes=*/true));
}

TEST_F(RACOption2Test, WriteSequenceClassifiesReadOnlyWindow) {
  OpenOption2(2);
  RACOption2LookupRecord first_read_only;
  RACOption2LookupRecord second_read_only;
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&first_read_only,
                                         /*latest_sequence=*/100));
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&second_read_only,
                                         /*latest_sequence=*/100));
  ASSERT_FALSE(first_read_only.Finish(/*latest_sequence=*/100));
  ASSERT_TRUE(second_read_only.Finish(/*latest_sequence=*/100));

  RACOption2EpochSnapshot snapshot;
  ASSERT_TRUE(cfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_EQ(snapshot.total_reads, 2U);
  ASSERT_EQ(snapshot.point_lookups, 2U);
  ASSERT_TRUE(snapshot.committed_write_count_known);
  ASSERT_EQ(snapshot.committed_writes, 0U);
  ASSERT_TRUE(snapshot.no_committed_writes);
  ASSERT_TRUE(cfd()->FinishRACOption2Epoch(
      snapshot.epoch_id, RACOption2EpochTerminalState::kRejected));

  RACOption2LookupRecord before_write;
  RACOption2LookupRecord after_write;
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&before_write,
                                         /*latest_sequence=*/100));
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&after_write,
                                         /*latest_sequence=*/101));
  ASSERT_FALSE(before_write.Finish(/*latest_sequence=*/100));
  ASSERT_TRUE(after_write.Finish(/*latest_sequence=*/101));

  ASSERT_TRUE(cfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_EQ(snapshot.total_reads, 2U);
  ASSERT_EQ(snapshot.point_lookups, 2U);
  ASSERT_TRUE(snapshot.committed_write_count_known);
  ASSERT_EQ(snapshot.committed_writes, 1U);
  ASSERT_FALSE(snapshot.no_committed_writes);
  ASSERT_TRUE(cfd()->FinishRACOption2Epoch(
      snapshot.epoch_id, RACOption2EpochTerminalState::kRejected));
}

TEST_F(RACOption2Test, WritesDoNotConsumeReadWindowTickets) {
  OpenOption2(4);
  RACOption2LookupRecord first_read;
  RACOption2LookupRecord second_read;

  ASSERT_OK(cfd()->BeginRACOption2Lookup(&first_read,
                                         /*latest_sequence=*/100));
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&second_read,
                                         /*latest_sequence=*/100));
  ASSERT_FALSE(first_read.Finish(/*latest_sequence=*/100));
  ASSERT_FALSE(second_read.Finish(/*latest_sequence=*/101));

  RACOption2EpochSnapshot snapshot;
  ASSERT_FALSE(cfd()->TakeNextRACOption2Snapshot(&snapshot));

  RACOption2LookupRecord third_read;
  RACOption2LookupRecord fourth_read;
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&third_read,
                                         /*latest_sequence=*/101));
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&fourth_read,
                                         /*latest_sequence=*/101));
  ASSERT_FALSE(third_read.Finish(/*latest_sequence=*/101));
  ASSERT_TRUE(fourth_read.Finish(/*latest_sequence=*/101));

  ASSERT_TRUE(cfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_EQ(snapshot.total_reads, 4U);
  ASSERT_EQ(snapshot.point_lookups, 4U);
  ASSERT_TRUE(snapshot.committed_write_count_known);
  ASSERT_EQ(snapshot.committed_writes, 1U);
  ASSERT_FALSE(snapshot.no_committed_writes);
  ASSERT_TRUE(cfd()->FinishRACOption2Epoch(
      snapshot.epoch_id, RACOption2EpochTerminalState::kRejected));
}

TEST_F(RACOption2Test, ExactWindowAndEvidence) {
  OpenOption2(2);
  std::unique_ptr<RACOption2LookupRecord> first;
  std::unique_ptr<RACOption2LookupRecord> second;
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&first));
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&second));
  first->RecordLevelRead(1);
  first->RecordLevelDescent(1);
  first->RecordFileMiss(11, 4096, 1, 300);
  second->RecordLevelRead(1);
  second->RecordFileHit(12, 8192, 1, 700);
  ASSERT_FALSE(first->Finish());
  ASSERT_TRUE(second->Finish());

  RACOption2EpochSnapshot snapshot;
  ASSERT_TRUE(cfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_EQ(snapshot.epoch_id, 0U);
  ASSERT_EQ(snapshot.total_reads, 2U);
  ASSERT_EQ(snapshot.point_lookups, 2U);
  ASSERT_FALSE(snapshot.no_committed_writes);
  ASSERT_EQ(snapshot.files.size(), 2U);
  ASSERT_EQ(snapshot.files[0].file_number, 11U);
  ASSERT_EQ(snapshot.files[0].misses, 1U);
  ASSERT_EQ(snapshot.files[0].miss_latency_nanos, 300U);
  ASSERT_EQ(snapshot.files[1].file_number, 12U);
  ASSERT_EQ(snapshot.files[1].hits, 1U);
  ASSERT_EQ(snapshot.files[1].hit_latency_nanos, 700U);
  ASSERT_EQ(snapshot.levels[1].reads, 2U);
  ASSERT_EQ(snapshot.levels[1].misses, 1U);
  ASSERT_TRUE(cfd()->FinishRACOption2Epoch(
      0, RACOption2EpochTerminalState::kScheduled));
  ASSERT_EQ(cfd()->GetRACOption2CollectorStats().invariant_violations, 0U);
}

TEST_F(RACOption2Test, NeutralProbePreservesMembershipWithoutMHT) {
  OpenOption2(1);
  std::unique_ptr<RACOption2LookupRecord> lookup;
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&lookup));
  lookup->RecordLevelRead(1);
  lookup->RecordFileObserved(99, 4096, 1);
  ASSERT_TRUE(lookup->Finish());
  RACOption2EpochSnapshot snapshot;
  ASSERT_TRUE(cfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_EQ(snapshot.files.size(), 1U);
  ASSERT_EQ(snapshot.files[0].file_number, 99U);
  ASSERT_EQ(snapshot.files[0].misses, 0U);
  ASSERT_EQ(snapshot.files[0].hits, 0U);
  ASSERT_EQ(snapshot.files[0].miss_latency_nanos, 0U);
  ASSERT_EQ(snapshot.files[0].hit_latency_nanos, 0U);
  ASSERT_EQ(snapshot.levels[1].reads, 1U);
  ASSERT_EQ(snapshot.levels[1].misses, 0U);
  ASSERT_TRUE(cfd()->FinishRACOption2Epoch(
      snapshot.epoch_id, RACOption2EpochTerminalState::kRejected));
}

TEST_F(RACOption2Test, MergeFailureProbePreservesNeutralMembership) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.enable_read_triggered_compaction = true;
  options.rac_design_option = 2;
  options.rac_window_size = 1;
  options.merge_operator = std::make_shared<RACFailingMergeOperator>();
  Reopen(options);
  ASSERT_OK(Put("merge-key", "base"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  ASSERT_OK(Merge("merge-key", "operand"));
  ASSERT_OK(Flush());

  auto sleeping = std::make_shared<test::SleepingBackgroundTask>();
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, sleeping.get(),
                 Env::LOW);
  sleeping->WaitUntilSleeping();
  std::string value;
  Status status = db_->Get(ReadOptions(), "merge-key", &value);
  ASSERT_TRUE(status.IsCorruption());
  ASSERT_EQ(status.subcode(), Status::SubCode::kMergeOperatorFailed);
  RACOption2EpochSnapshot snapshot;
  ASSERT_TRUE(cfd()->TakeNextRACOption2Snapshot(&snapshot));
  sleeping->WakeUp();
  sleeping->WaitUntilDone();
  ASSERT_EQ(snapshot.total_reads, 1U);
  ASSERT_EQ(snapshot.point_lookups, 1U);
  ASSERT_TRUE(snapshot.no_committed_writes);
  ASSERT_FALSE(snapshot.files.empty());
  for (const auto& file : snapshot.files) {
    ASSERT_EQ(file.misses, 0U);
    ASSERT_EQ(file.hits, 0U);
    ASSERT_EQ(file.miss_latency_nanos, 0U);
    ASSERT_EQ(file.hit_latency_nanos, 0U);
  }
  ASSERT_TRUE(cfd()->FinishRACOption2Epoch(
      snapshot.epoch_id, RACOption2EpochTerminalState::kRejected));
}

TEST_F(RACOption2Test, EmptyLookupCountsAndCooldownExpires) {
  OpenOption2(1);
  std::unique_ptr<RACOption2LookupRecord> lookup;
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&lookup));
  ASSERT_TRUE(lookup->Finish());
  RACOption2EpochSnapshot snapshot;
  ASSERT_TRUE(cfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_TRUE(snapshot.files.empty());
  ASSERT_TRUE(cfd()->FinishRACOption2Epoch(
      0, RACOption2EpochTerminalState::kScheduled));

  InternalKey smallest("a", 1, kTypeValue);
  InternalKey largest("z", 1, kTypeValue);
  ASSERT_TRUE(cfd()->OnRACOption2CompactionInstalled(7, smallest, largest));
  ASSERT_FALSE(cfd()->OnRACOption2CompactionInstalled(7, smallest, largest));
  ASSERT_TRUE(cfd()->RACOption2BottomRangeInCooldown(smallest, largest, 0));
  ASSERT_TRUE(cfd()->RACOption2BottomRangeInCooldown(smallest, largest, 4));
  ASSERT_FALSE(cfd()->RACOption2BottomRangeInCooldown(smallest, largest, 5));
  ASSERT_EQ(cfd()->GetRACOption2CooldownEntryCount(), 0U);
}

TEST_F(RACOption2Test, PointAndRangeTombstonesAreTerminalHits) {
  OpenOption2(2);
  ASSERT_OK(Put("point-deleted", "old"));
  ASSERT_OK(Put("range-deleted", "old"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  ASSERT_OK(Delete("point-deleted"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             "range-a", "range-z"));
  ASSERT_OK(Flush());

  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "point-deleted", &value).IsNotFound());
  ASSERT_TRUE(db_->Get(ReadOptions(), "range-deleted", &value).IsNotFound());

  RACOption2EpochSnapshot snapshot;
  ASSERT_TRUE(cfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_EQ(snapshot.total_reads, 2U);
  ASSERT_EQ(snapshot.point_lookups, 2U);
  ASSERT_TRUE(snapshot.no_committed_writes);
  ASSERT_TRUE(snapshot.evidence_complete);
  uint64_t hits = 0;
  uint64_t misses = 0;
  for (const auto& file : snapshot.files) {
    hits += file.hits;
    misses += file.misses;
  }
  ASSERT_EQ(hits, 2U);
  ASSERT_EQ(misses, 0U);
  ASSERT_EQ(snapshot.levels[0].misses, 0U);
  ASSERT_TRUE(cfd()->FinishRACOption2Epoch(
      snapshot.epoch_id, RACOption2EpochTerminalState::kRejected));
}

TEST_F(RACOption2Test, ConcurrentPublicationIsExactAndAllocationFree) {
  constexpr uint64_t kWindow = 64;
  OpenOption2(kWindow);
  std::vector<std::unique_ptr<RACOption2LookupRecord>> records(kWindow);
  for (auto& record : records) {
    ASSERT_OK(cfd()->BeginRACOption2Lookup(&record));
  }
  std::vector<std::thread> workers;
  for (uint64_t i = 0; i < kWindow; ++i) {
    workers.emplace_back([&, i] {
      records[i]->RecordLevelRead(1);
      records[i]->RecordFileMiss(100 + (i % 4), 4096, 1, i + 1);
      records[i]->RecordFileHit(200 + (i % 4), 4096, 1, i + 1);
      records[i]->Finish();
    });
  }
  for (auto& worker : workers) worker.join();

  RACOption2EpochSnapshot snapshot;
  ASSERT_TRUE(cfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_EQ(snapshot.total_reads, kWindow);
  ASSERT_EQ(snapshot.point_lookups, kWindow);
  ASSERT_EQ(snapshot.files.size(), 8U);
  uint64_t misses = 0;
  uint64_t hits = 0;
  uint64_t miss_latency = 0;
  uint64_t hit_latency = 0;
  for (const auto& evidence : snapshot.files) {
    misses += evidence.misses;
    hits += evidence.hits;
    miss_latency += evidence.miss_latency_nanos;
    hit_latency += evidence.hit_latency_nanos;
  }
  ASSERT_EQ(misses, kWindow);
  ASSERT_EQ(hits, kWindow);
  ASSERT_EQ(miss_latency, kWindow * (kWindow + 1) / 2);
  ASSERT_EQ(hit_latency, kWindow * (kWindow + 1) / 2);
  ASSERT_EQ(snapshot.levels[1].reads, kWindow);
  const auto stats = cfd()->GetRACOption2CollectorStats();
  ASSERT_EQ(stats.invariant_violations, 0U);
  ASSERT_EQ(stats.evidence_exhaustions, 0U);
  ASSERT_GT(stats.memory_bytes, 0U);
  ASSERT_TRUE(cfd()->FinishRACOption2Epoch(
      0, RACOption2EpochTerminalState::kRejected));
}

TEST_F(RACOption2Test, LaterEpochSealsIndependentlyButWaitsForFIFO) {
  OpenOption2(2);
  std::unique_ptr<RACOption2LookupRecord> first0, first1, second0, second1;
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&first0));
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&first1));
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&second0));
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&second1));
  ASSERT_FALSE(first0->Finish());
  ASSERT_FALSE(second0->Finish());
  ASSERT_TRUE(second1->Finish());

  RACOption2EpochSnapshot snapshot;
  ASSERT_FALSE(cfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_TRUE(first1->Finish());
  ASSERT_EQ(cfd()->GetRACOption2CollectorStats().ready_backlog, 2U);
  ASSERT_TRUE(cfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_EQ(snapshot.epoch_id, 0U);
  ASSERT_TRUE(cfd()->MarkRACOption2EpochPending(0));
  ASSERT_FALSE(cfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_TRUE(cfd()->FinishRACOption2Epoch(
      0, RACOption2EpochTerminalState::kInstalled));
  ASSERT_TRUE(cfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_EQ(snapshot.epoch_id, 1U);
  ASSERT_TRUE(cfd()->FinishRACOption2Epoch(
      1, RACOption2EpochTerminalState::kRejected));
  ASSERT_GE(cfd()->GetRACOption2CollectorStats().peak_ready_backlog, 2U);
}

TEST_F(RACOption2Test, CounterOverflowRejectsWholeEpoch) {
  OpenOption2(2);
  std::unique_ptr<RACOption2LookupRecord> first, second;
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&first));
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&second));
  first->RecordFileMiss(77, 4096, 1,
                        std::numeric_limits<uint64_t>::max());
  second->RecordFileMiss(77, 4096, 1, 1);
  ASSERT_FALSE(first->Finish());
  ASSERT_TRUE(second->Finish());
  RACOption2EpochSnapshot snapshot;
  ASSERT_TRUE(cfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_FALSE(snapshot.evidence_complete);
  ASSERT_TRUE(snapshot.files.empty());
  ASSERT_GT(cfd()->GetRACOption2CollectorStats().saturated_counters, 0U);
  ASSERT_TRUE(cfd()->FinishRACOption2Epoch(
      0, RACOption2EpochTerminalState::kRejected));
}

TEST_F(RACOption2Test, HitLatencyOverflowRejectsWholeEpoch) {
  OpenOption2(2);
  std::unique_ptr<RACOption2LookupRecord> first, second;
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&first));
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&second));
  first->RecordFileHit(77, 4096, 1,
                       std::numeric_limits<uint64_t>::max());
  second->RecordFileHit(77, 4096, 1, 1);
  ASSERT_FALSE(first->Finish());
  ASSERT_TRUE(second->Finish());
  RACOption2EpochSnapshot snapshot;
  ASSERT_TRUE(cfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_FALSE(snapshot.evidence_complete);
  ASSERT_TRUE(snapshot.files.empty());
  ASSERT_GT(cfd()->GetRACOption2CollectorStats().saturated_counters, 0U);
  ASSERT_TRUE(cfd()->FinishRACOption2Epoch(
      0, RACOption2EpochTerminalState::kRejected));
}

TEST_F(RACOption2Test, HitLatencyIsResetWhenRingSlotIsReused) {
  OpenOption2(1);
  RACOption2EpochSnapshot snapshot;
  for (uint64_t epoch = 0; epoch <= kRACEpochRingCapacity; ++epoch) {
    std::unique_ptr<RACOption2LookupRecord> lookup;
    ASSERT_OK(cfd()->BeginRACOption2Lookup(&lookup));
    const uint64_t latency = epoch == 0 ? 11 : epoch + 1;
    lookup->RecordFileHit(88, 4096, 1, latency);
    ASSERT_TRUE(lookup->Finish());
    ASSERT_TRUE(cfd()->TakeNextRACOption2Snapshot(&snapshot));
    ASSERT_EQ(snapshot.epoch_id, epoch);
    ASSERT_TRUE(snapshot.evidence_complete);
    ASSERT_EQ(snapshot.files.size(), 1U);
    ASSERT_EQ(snapshot.files[0].hits, 1U);
    ASSERT_EQ(snapshot.files[0].hit_latency_nanos, latency);
    ASSERT_TRUE(cfd()->FinishRACOption2Epoch(
        epoch, RACOption2EpochTerminalState::kRejected));
  }
}

TEST_F(RACOption2Test, RegistryGrowsPastInitialSlabWithoutDroppingEvidence) {
  constexpr uint64_t kLookups = 8;
  constexpr uint64_t kFilesPerLookup = 160;
  OpenOption2(kLookups);
  std::vector<std::unique_ptr<RACOption2LookupRecord>> lookups(kLookups);
  for (auto& lookup : lookups) {
    ASSERT_OK(cfd()->BeginRACOption2Lookup(&lookup));
  }
  std::vector<std::thread> workers;
  for (uint64_t lookup_index = 0; lookup_index < kLookups; ++lookup_index) {
    workers.emplace_back([&, lookup_index] {
      const uint64_t first_file = lookup_index * kFilesPerLookup + 1;
      for (uint64_t offset = 0; offset < kFilesPerLookup; ++offset) {
        lookups[lookup_index]->RecordFileMiss(first_file + offset, 4096, 1,
                                               1);
      }
      lookups[lookup_index]->Finish();
    });
  }
  for (auto& worker : workers) worker.join();
  RACOption2EpochSnapshot snapshot;
  ASSERT_TRUE(cfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_TRUE(snapshot.evidence_complete);
  ASSERT_EQ(snapshot.files.size(), kLookups * kFilesPerLookup);
  ASSERT_EQ(cfd()->GetRACOption2CollectorStats().evidence_exhaustions, 0U);
  ASSERT_TRUE(cfd()->FinishRACOption2Epoch(
      0, RACOption2EpochTerminalState::kRejected));
}

TEST_F(RACOption2Test, ConflictingObservedLevelsRejectWholeEpoch) {
  OpenOption2(2);
  std::unique_ptr<RACOption2LookupRecord> first, second;
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&first));
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&second));
  first->RecordFileMiss(77, 4096, 1, 1);
  second->RecordFileMiss(77, 4096, 2, 1);
  ASSERT_FALSE(first->Finish());
  ASSERT_TRUE(second->Finish());
  RACOption2EpochSnapshot snapshot;
  ASSERT_TRUE(cfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_FALSE(snapshot.evidence_complete);
  ASSERT_TRUE(snapshot.files.empty());
  ASSERT_TRUE(cfd()->FinishRACOption2Epoch(
      0, RACOption2EpochTerminalState::kRejected));
}

TEST_F(RACOption2Test,
       VersionMoveWithDistinctHandlesRejectsConflictingPhysicalFile) {
  OpenOption2(2);
  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());
  FileMetaData* old_file = cfd()->current()->storage_info()->LevelFiles(0).front();
  const uint64_t file_number = old_file->fd.GetNumber();
  const auto old_handle = old_file->rac_option2_evidence_handle;
  ASSERT_TRUE(old_handle.valid());

  std::unique_ptr<RACOption2LookupRecord> before_move;
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&before_move));
  before_move->RecordFileMiss(old_handle, 0, 11);

  MoveFilesToLevel(1);
  const auto& moved_files = cfd()->current()->storage_info()->LevelFiles(1);
  ASSERT_EQ(moved_files.size(), 1U);
  ASSERT_EQ(moved_files.front()->fd.GetNumber(), file_number);
  const auto moved_handle = moved_files.front()->rac_option2_evidence_handle;
  ASSERT_TRUE(moved_handle.valid());
  ASSERT_NE(moved_handle.index, old_handle.index);

  std::unique_ptr<RACOption2LookupRecord> after_move;
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&after_move));
  after_move->RecordFileMiss(moved_handle, 1, 13);
  ASSERT_FALSE(before_move->Finish());
  ASSERT_TRUE(after_move->Finish());

  RACOption2EpochSnapshot snapshot;
  ASSERT_TRUE(cfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_EQ(snapshot.total_reads, 2U);
  ASSERT_EQ(snapshot.point_lookups, 2U);
  ASSERT_FALSE(snapshot.no_committed_writes);
  ASSERT_FALSE(snapshot.evidence_complete);
  ASSERT_TRUE(snapshot.files.empty());
  ASSERT_TRUE(cfd()->FinishRACOption2Epoch(
      0, RACOption2EpochTerminalState::kRejected));
}

TEST_F(RACOption2Test, VersionPublicationAssignsStableFileHandle) {
  OpenOption2(1);
  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());
  auto* storage = cfd()->current()->storage_info();
  ASSERT_FALSE(storage->LevelFiles(0).empty());
  FileMetaData* file = storage->LevelFiles(0).front();
  const auto handle = file->rac_option2_evidence_handle;
  ASSERT_TRUE(handle.valid());
  {
    FileMetaData metadata_copy = *file;
    ASSERT_TRUE(metadata_copy.rac_option2_evidence_handle.valid());
  }

  std::unique_ptr<RACOption2LookupRecord> lookup;
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&lookup));
  lookup->RecordFileMiss(handle, 0, 17);
  ASSERT_TRUE(lookup->Finish());
  RACOption2EpochSnapshot snapshot;
  ASSERT_TRUE(cfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_EQ(snapshot.total_reads, 1U);
  ASSERT_EQ(snapshot.point_lookups, 1U);
  ASSERT_FALSE(snapshot.no_committed_writes);
  ASSERT_TRUE(snapshot.evidence_complete);
  ASSERT_EQ(snapshot.files.size(), 1U);
  ASSERT_EQ(snapshot.files[0].file_number, file->fd.GetNumber());
  ASSERT_EQ(snapshot.files[0].miss_latency_nanos, 17U);
  ASSERT_TRUE(cfd()->FinishRACOption2Epoch(
      0, RACOption2EpochTerminalState::kRejected));
  ASSERT_EQ(Get("key"), "value");
  ASSERT_EQ(cfd()->GetRACOption2CollectorStats().evidence_exhaustions, 0U);
}

TEST_F(RACOption2Test,
       RegistrationFailureKeepsPriorVersionAuthoritative) {
  OpenOption2(1);
  ASSERT_OK(Put("old", "value"));
  ASSERT_OK(Flush());
  Version* prior = cfd()->current();

  cfd()->TEST_SetRACOption2RegistrationFailure(true);
  ASSERT_OK(Put("new", "value"));
  Status flush_status = Flush();
  ASSERT_TRUE(flush_status.IsMemoryLimit());
  ASSERT_EQ(cfd()->current(), prior);
  cfd()->TEST_SetRACOption2RegistrationFailure(false);
}

TEST_F(RACOption2Test, AllocationFailureFailsReopenCleanly) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = false;
  options.enable_read_triggered_compaction = true;
  options.rac_design_option = 2;
  options.rac_window_size = 1;
  Close();

  SyncPoint::GetInstance()->SetCallBack(
      "RACOption2Collector::BeforeAllocation",
      [&](void* arg) { *static_cast<bool*>(arg) = true; });
  SyncPoint::GetInstance()->EnableProcessing();
  std::unique_ptr<DB> failed_db;
  Status open_status = DB::Open(options, dbname_, &failed_db);
  ASSERT_TRUE(open_status.IsMemoryLimit());
  ASSERT_EQ(failed_db.get(), nullptr);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  Reopen(options);
  ASSERT_NE(db_, nullptr);
}

TEST_F(RACOption2Test, ColumnFamilyAllocationFailsBeforeManifestCommit) {
  OpenOption2(1);
  SyncPoint::GetInstance()->SetCallBack(
      "RACOption2Collector::BeforeAllocation", [](void* arg) {
        *static_cast<bool*>(arg) = true;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ColumnFamilyHandle* handle = nullptr;
  ColumnFamilyOptions cf_options;
  cf_options.enable_read_triggered_compaction = true;
  cf_options.rac_design_option = 2;
  cf_options.rac_window_size = 1;
  Status status = db_->CreateColumnFamily(cf_options, "allocation-failed",
                                          &handle);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(status.IsMemoryLimit());
  ASSERT_EQ(handle, nullptr);

  ASSERT_OK(db_->CreateColumnFamily(cf_options, "allocation-failed", &handle));
  ASSERT_NE(handle, nullptr);
  ASSERT_OK(db_->DropColumnFamily(handle));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(handle));
}

TEST_F(RACOption2Test, TicketExhaustionDrainsAndTerminalizesPartialEpoch) {
  OpenOption2(4);
  std::unique_ptr<RACOption2LookupRecord> lookup;
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&lookup));
  cfd()->TEST_SetRACOption2NextTicket(std::numeric_limits<uint64_t>::max());
  Status reserve_status;
  std::thread waiter([&] {
    RACOption2LookupRecord record;
    reserve_status = cfd()->BeginRACOption2Lookup(&record);
  });
  lookup->Finish();
  waiter.join();
  ASSERT_TRUE(reserve_status.IsIncomplete());
  ASSERT_EQ(cfd()->GetRACOption2CollectorStats().partial_epochs_terminalized,
            1U);
}

TEST_F(RACOption2Test, StopWaitsForReservedLookupAndDrainsPartialEpoch) {
  OpenOption2(4);
  std::unique_ptr<RACOption2LookupRecord> lookup;
  ASSERT_OK(cfd()->BeginRACOption2Lookup(&lookup));
  std::atomic<bool> started{false};
  std::atomic<bool> stopped{false};
  std::thread stopper([&] {
    started.store(true, std::memory_order_release);
    cfd()->StopRACOption2Collector();
    stopped.store(true, std::memory_order_release);
  });
  while (!started.load(std::memory_order_acquire)) {
  }
  ASSERT_FALSE(stopped.load(std::memory_order_acquire));
  lookup->Finish();
  stopper.join();
  ASSERT_TRUE(stopped.load(std::memory_order_acquire));
  ASSERT_EQ(cfd()->GetRACOption2CollectorStats().partial_epochs_terminalized,
            1U);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
