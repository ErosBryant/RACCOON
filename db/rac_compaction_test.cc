// Copyright (c) Meta Platforms, Inc. and affiliates.

#include "db/column_family.h"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "db/compaction/compaction_picker_level.h"
#include "db/db_test_util.h"
#include "file/sst_file_manager_impl.h"
#include "rocksdb/compaction_job_stats.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

namespace {
constexpr uint64_t kRACOption2MinSeedFileSize = 10ULL * 1024 * 1024;
constexpr char kRACOption2MinSeedFileSizeSyncPoint[] =
    "DBImpl::MaybeEvaluateRACOption2Locked::MinSeedFileSize";
}  // namespace

class RACCompactionEventListener : public EventListener {
 public:
  void OnCompactionCompleted(DB* /*db*/,
                             const CompactionJobInfo& info) override {
    if (info.is_rac_option2) {
      ++rac_compactions_;
    } else {
      ++ordinary_compactions_;
    }
  }

  int rac_compactions() const { return rac_compactions_.load(); }
  int ordinary_compactions() const { return ordinary_compactions_.load(); }

 private:
  std::atomic<int> rac_compactions_{0};
  std::atomic<int> ordinary_compactions_{0};
};

class RACCompactionAdversarialTest : public DBTestBase {
 public:
  RACCompactionAdversarialTest()
      : DBTestBase("rac_compaction_adversarial_test",
                   /*env_do_fsync=*/false) {
    env_->SetBackgroundThreads(1, Env::Priority::BOTTOM);
  }

  ~RACCompactionAdversarialTest() override {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }

  void IgnoreMinSeedFileSizeForExistingTests() {
    SyncPoint::GetInstance()->SetCallBack(
        kRACOption2MinSeedFileSizeSyncPoint,
        [](void* arg) { *static_cast<uint64_t*>(arg) = 0; });
    SyncPoint::GetInstance()->EnableProcessing();
  }

  void EnforceMinSeedFileSize() {
    SyncPoint::GetInstance()->SetCallBack(
        kRACOption2MinSeedFileSizeSyncPoint, [](void* arg) {
          ASSERT_EQ(kRACOption2MinSeedFileSize,
                    *static_cast<uint64_t*>(arg));
        });
    SyncPoint::GetInstance()->EnableProcessing();
  }

  static ColumnFamilyData* GetCfd(ColumnFamilyHandle* handle) {
    return static_cast<ColumnFamilyHandleImpl*>(handle)->cfd();
  }

  ColumnFamilyData* DefaultCfd() {
    return GetCfd(db_->DefaultColumnFamily());
  }

  Options Option2Options(uint64_t window_size = 1,
                         uint64_t cooldown_windows = 4) {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.disable_auto_compactions = false;
    options.enable_read_triggered_compaction = true;
    options.rac_design_option = 2;
    options.rac_window_size = window_size;
    options.rac_benefit_efficiency_threshold = 0.0;
    options.rac_cooldown_windows = cooldown_windows;
    options.num_levels = 4;
    options.level0_file_num_compaction_trigger = 8;
    options.max_background_compactions = 1;
    return options;
  }

  Options RAC2Options(uint64_t window_size = 1,
                      uint64_t cooldown_windows = 4) {
    Options options = Option2Options(window_size, cooldown_windows);
    options.rac_design_option = 3;
    options.rac2_hotspot_persistence = 0.8;
    options.rac2_prediction_horizon = 4;
    options.rac2_compaction_bandwidth_mib_per_sec = 200.0;
    return options;
  }

  void ReopenOption2(uint64_t window_size = 1,
                     uint64_t cooldown_windows = 4) {
    Reopen(Option2Options(window_size, cooldown_windows));
  }

  Options UniversalOption2Options(bool dual_purpose = false,
                                  double efficiency_threshold = 0.0) {
    Options options = Option2Options();
    options.compaction_style = kCompactionStyleUniversal;
    options.rac_option2_dual_purpose_selection = dual_purpose;
    options.rac_benefit_efficiency_threshold = efficiency_threshold;
    options.level0_file_num_compaction_trigger = 100;
    options.compaction_options_universal.max_size_amplification_percent =
        10000;
    return options;
  }

  void ReopenUniversalOption2(bool dual_purpose = false,
                              double efficiency_threshold = 0.0) {
    Reopen(UniversalOption2Options(dual_purpose, efficiency_threshold));
  }

  bool InstallCooldown(ColumnFamilyData* cfd, uint64_t install_id,
                       const InternalKey& smallest,
                       const InternalKey& largest) {
    dbfull()->TEST_LockMutex();
    const bool installed = cfd->OnRACOption2CompactionInstalled(
        install_id, smallest, largest);
    dbfull()->TEST_UnlockMutex();
    return installed;
  }

  bool InCooldown(ColumnFamilyData* cfd, const InternalKey& smallest,
                  const InternalKey& largest, uint64_t epoch) {
    dbfull()->TEST_LockMutex();
    const bool result =
        cfd->RACOption2BottomRangeInCooldown(smallest, largest, epoch);
    dbfull()->TEST_UnlockMutex();
    return result;
  }

  size_t CooldownCount(ColumnFamilyData* cfd) {
    dbfull()->TEST_LockMutex();
    const size_t result = cfd->GetRACOption2CooldownEntryCount();
    dbfull()->TEST_UnlockMutex();
    return result;
  }

  void FinishSingleLookupEpoch(ColumnFamilyData* cfd, uint64_t epoch) {
    std::unique_ptr<RACOption2LookupRecord> lookup;
    ASSERT_OK(cfd->BeginRACOption2Lookup(&lookup));
    ASSERT_NE(lookup, nullptr);
    ASSERT_TRUE(lookup->Finish());

    RACOption2EpochSnapshot snapshot;
    ASSERT_TRUE(cfd->TakeNextRACOption2Snapshot(&snapshot));
    ASSERT_EQ(snapshot.epoch_id, epoch);
    ASSERT_TRUE(cfd->FinishRACOption2Epoch(
        epoch, RACOption2EpochTerminalState::kRejected));
  }

  VersionStorageInfo* CurrentStorageInfo() {
    return DefaultCfd()->current()->storage_info();
  }

  void PutRange(int begin, int end, const std::string& value_tag) {
    for (int key = begin; key <= end; ++key) {
      ASSERT_OK(Put(Key(key), value_tag + std::to_string(key)));
    }
  }

  void MakeFileAtLevel(int level, int begin, int end,
                       const std::string& value_tag) {
    PutRange(begin, end, value_tag);
    ASSERT_OK(Flush());
    MoveFilesToLevel(level);
  }

  FileMetaData* OnlyFileAtLevel(int level) {
    const auto& files = CurrentStorageInfo()->LevelFiles(level);
    EXPECT_EQ(1U, files.size());
    return files.empty() ? nullptr : files.front();
  }

  std::string LiveFileName(uint64_t file_number) {
    std::vector<LiveFileMetaData> files;
    db_->GetLiveFilesMetaData(&files);
    for (const LiveFileMetaData& file : files) {
      if (file.file_number == file_number) {
        return file.name;
      }
    }
    return "";
  }

  void RecordCompactionRewrite(int source_level, int output_level,
                               uint64_t input_bytes, uint64_t output_bytes,
                               uint64_t elapsed_us) {
    CompactionJobStats stats;
    stats.total_input_bytes = input_bytes;
    stats.total_output_bytes = output_bytes;
    stats.elapsed_micros = elapsed_us;
    dbfull()->TEST_LockMutex();
    DefaultCfd()->internal_stats()->RecordCompactionRewrite(
        source_level, output_level, stats);
    dbfull()->TEST_UnlockMutex();
  }

  double GetCompactionRewriteRate(int source_level, int output_level) {
    dbfull()->TEST_LockMutex();
    const double rate = DefaultCfd()->internal_stats()->GetCompactionRewriteRate(
        source_level, output_level);
    dbfull()->TEST_UnlockMutex();
    return rate;
  }

  size_t GetCompactionRewriteSampleCount(int source_level, int output_level) {
    dbfull()->TEST_LockMutex();
    const size_t count =
        DefaultCfd()->internal_stats()->GetCompactionRewriteSampleCount(
            source_level, output_level);
    dbfull()->TEST_UnlockMutex();
    return count;
  }

  void EvaluateRAC2WithSampleElapsed(
      uint64_t elapsed_us, double cost_weight,
      RACOption2CompactionRequest* request) {
    Options options = RAC2Options(/*window_size=*/1);
    options.rac2_hotspot_persistence = 1.0;
    options.rac2_prediction_horizon = 1;
    options.rac2_cost_weight = cost_weight;
    DestroyAndReopen(options);
    MakeFileAtLevel(2, 0, 9, "target-");
    MakeFileAtLevel(1, 5, 14, "source-");
    FileMetaData* source = OnlyFileAtLevel(1);
    ASSERT_NE(source, nullptr);
    FileMetaData* target = OnlyFileAtLevel(2);
    ASSERT_NE(target, nullptr);
    const uint64_t total_input_bytes =
        source->fd.GetFileSize() + target->fd.GetFileSize();
    RecordCompactionRewrite(/*source_level=*/1, /*output_level=*/2,
                            total_input_bytes, total_input_bytes, elapsed_us);

    PauseRACCompactionThread();
    SealMissEpoch(DefaultCfd(), source, /*level=*/1,
                  /*latency_nanos=*/2000);
    dbfull()->TEST_ForceRACOption2Evaluation();
    dbfull()->TEST_LockMutex();
    *request = DefaultCfd()->GetRACOption2Request();
    dbfull()->TEST_UnlockMutex();
    ResumeRACCompactionThread();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }

  void BeginTrackedLookup(
      ColumnFamilyData* cfd,
      std::unique_ptr<RACOption2LookupRecord>* lookup) {
    IgnoreMinSeedFileSizeForExistingTests();
    lookup->reset(new RACOption2LookupRecord());
    ASSERT_OK(cfd->BeginRACOption2Lookup(
        lookup->get(), db_->GetLatestSequenceNumber()));
  }

  bool FinishTrackedLookup(RACOption2LookupRecord* lookup) {
    return lookup->Finish(db_->GetLatestSequenceNumber());
  }

  void SealMissEpoch(ColumnFamilyData* cfd, FileMetaData* file, int level,
                     uint64_t latency_nanos = 1000) {
    IgnoreMinSeedFileSizeForExistingTests();
    const uint64_t sequence = db_->GetLatestSequenceNumber();
    std::unique_ptr<RACOption2LookupRecord> lookup(
        new RACOption2LookupRecord());
    ASSERT_OK(cfd->BeginRACOption2Lookup(lookup.get(), sequence));
    ASSERT_NE(lookup, nullptr);
    lookup->RecordFileMiss(file->fd.GetNumber(), file->fd.GetFileSize(), level,
                           latency_nanos);
    ASSERT_TRUE(lookup->Finish(sequence));
  }

  void SealMixedWriteEpoch(ColumnFamilyData* cfd, FileMetaData* file,
                           int level, uint64_t latency_nanos = 1000) {
    IgnoreMinSeedFileSizeForExistingTests();
    const uint64_t window_size = cfd->GetRACOption2WindowSize();
    const uint64_t sequence_before_write = db_->GetLatestSequenceNumber();
    for (uint64_t i = 0; i < window_size; ++i) {
      std::unique_ptr<RACOption2LookupRecord> lookup(
          new RACOption2LookupRecord());
      ASSERT_OK(cfd->BeginRACOption2Lookup(
          lookup.get(), db_->GetLatestSequenceNumber()));
      ASSERT_NE(lookup, nullptr);
      lookup->RecordFileMiss(file->fd.GetNumber(), file->fd.GetFileSize(),
                             level, latency_nanos);
      if (i == 0) {
        ASSERT_OK(Put("write-inside-rac-window-" +
                          std::to_string(sequence_before_write),
                      "value"));
        ASSERT_GT(db_->GetLatestSequenceNumber(), sequence_before_write);
      }
      ASSERT_EQ(i + 1 == window_size,
                lookup->Finish(db_->GetLatestSequenceNumber()));
    }
  }

  void SealMissRatioEpoch(ColumnFamilyData* cfd, FileMetaData* file, int level,
                          uint64_t misses, uint64_t hits,
                          uint64_t miss_latency_nanos = 1000) {
    IgnoreMinSeedFileSizeForExistingTests();
    const uint64_t observations = misses + hits;
    ASSERT_EQ(observations, cfd->GetRACOption2WindowSize());
    const uint64_t sequence = db_->GetLatestSequenceNumber();
    for (uint64_t i = 0; i < observations; ++i) {
      std::unique_ptr<RACOption2LookupRecord> lookup(
          new RACOption2LookupRecord());
      ASSERT_OK(cfd->BeginRACOption2Lookup(lookup.get(), sequence));
      ASSERT_NE(lookup, nullptr);
      if (i < misses) {
        lookup->RecordFileMiss(file->fd.GetNumber(), file->fd.GetFileSize(),
                               level, miss_latency_nanos);
      } else {
        lookup->RecordFileHit(file->fd.GetNumber(), file->fd.GetFileSize(),
                              level);
      }
      ASSERT_EQ(i + 1 == observations, lookup->Finish(sequence));
    }
  }

  void SealSourceAndTargetEpoch(FileMetaData* source, FileMetaData* target,
                                uint64_t source_misses, uint64_t source_hits,
                                uint64_t source_latency_nanos,
                                uint64_t target_latency_nanos) {
    const uint64_t reads = source_misses + source_hits;
    ASSERT_EQ(reads, DefaultCfd()->GetRACOption2WindowSize());
    for (uint64_t i = 0; i < reads; ++i) {
      std::unique_ptr<RACOption2LookupRecord> lookup;
      BeginTrackedLookup(DefaultCfd(), &lookup);
      if (i < source_misses) {
        lookup->RecordFileMiss(source->fd.GetNumber(),
                               source->fd.GetFileSize(), 1,
                               source_latency_nanos);
        lookup->RecordFileMiss(target->fd.GetNumber(),
                               target->fd.GetFileSize(), 2,
                               target_latency_nanos);
      } else {
        lookup->RecordFileHit(source->fd.GetNumber(),
                              source->fd.GetFileSize(), 1);
      }
      ASSERT_EQ(i + 1 == reads, FinishTrackedLookup(lookup.get()));
    }
  }

  std::pair<FileMetaData*, FileMetaData*>
  MakeDualPurposeSameLevelCandidates() {
    MakeFileAtLevel(2, 0, 9, "target-a-");
    MakeFileAtLevel(2, 100, 109, std::string(512, 't'));
    MakeFileAtLevel(1, 0, 9, std::string(512, 's'));
    MakeFileAtLevel(1, 100, 109, "source-b-");
    const auto& sources = CurrentStorageInfo()->LevelFiles(1);
    const auto& targets = CurrentStorageInfo()->LevelFiles(2);
    EXPECT_EQ(2U, sources.size());
    EXPECT_EQ(2U, targets.size());
    if (sources.size() != 2) {
      return {nullptr, nullptr};
    }
    const auto& priority = CurrentStorageInfo()->FilesByCompactionPri(1);
    EXPECT_EQ(2U, priority.size());
    if (priority.size() != 2) {
      return {nullptr, nullptr};
    }
    const int cursor = CurrentStorageInfo()->NextCompactionIndex(1);
    EXPECT_GE(cursor, 0);
    EXPECT_LT(static_cast<size_t>(cursor), priority.size());
    if (cursor < 0 || static_cast<size_t>(cursor) >= priority.size()) {
      return {nullptr, nullptr};
    }
    const int aligned_index = priority[static_cast<size_t>(cursor)];
    EXPECT_TRUE(aligned_index == 0 || aligned_index == 1);
    if (aligned_index != 0 && aligned_index != 1) {
      return {nullptr, nullptr};
    }
    return {sources[static_cast<size_t>(aligned_index)],
            sources[static_cast<size_t>(aligned_index == 0 ? 1 : 0)]};
  }

  uint64_t RACInputBytes(FileMetaData* source, int output_level) {
    std::vector<FileMetaData*> targets;
    CurrentStorageInfo()->GetOverlappingInputs(
        output_level, &source->smallest, &source->largest, &targets);
    uint64_t bytes = source->fd.GetFileSize();
    for (const FileMetaData* target : targets) {
      bytes += target->fd.GetFileSize();
    }
    return bytes;
  }

  std::pair<FileMetaData*, FileMetaData*> MakeUniversalRACCandidate(
      bool dual_purpose = false, double efficiency_threshold = 0.0) {
    ReopenOption2();
    MakeFileAtLevel(2, 0, 19, std::string(512, 't'));
    MakeFileAtLevel(1, 5, 14, "universal-source-");
    ReopenUniversalOption2(dual_purpose, efficiency_threshold);
    return {OnlyFileAtLevel(1), OnlyFileAtLevel(2)};
  }

  void SealTwoCandidateEpoch(FileMetaData* first_file,
                             uint64_t first_latency,
                             FileMetaData* second_file,
                             uint64_t second_latency) {
    IgnoreMinSeedFileSizeForExistingTests();
    std::unique_ptr<RACOption2LookupRecord> first;
    std::unique_ptr<RACOption2LookupRecord> second;
    ASSERT_OK(DefaultCfd()->BeginRACOption2Lookup(&first));
    ASSERT_OK(DefaultCfd()->BeginRACOption2Lookup(&second));
    first->RecordFileMiss(first_file->fd.GetNumber(),
                          first_file->fd.GetFileSize(), 1, first_latency);
    second->RecordFileMiss(second_file->fd.GetNumber(),
                           second_file->fd.GetFileSize(), 1, second_latency);
    ASSERT_FALSE(first->Finish());
    ASSERT_TRUE(second->Finish());
  }

  void PauseRACCompactionThread() {
    sleeping_task_.reset(new test::SleepingBackgroundTask());
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                   sleeping_task_.get(), Env::BOTTOM);
    sleeping_task_->WaitUntilSleeping();
  }

  void ResumeRACCompactionThread() {
    if (sleeping_task_) {
      sleeping_task_->WakeUp();
      sleeping_task_->WaitUntilDone();
      sleeping_task_.reset();
    }
  }

  void PauseLowCompactionThread() {
    low_sleeping_task_.reset(new test::SleepingBackgroundTask());
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                   low_sleeping_task_.get(), Env::LOW);
    low_sleeping_task_->WaitUntilSleeping();
  }

  void ResumeLowCompactionThread() {
    if (low_sleeping_task_) {
      low_sleeping_task_->WakeUp();
      low_sleeping_task_->WaitUntilDone();
      low_sleeping_task_.reset();
    }
  }

  std::shared_ptr<test::SleepingBackgroundTask> sleeping_task_;
  std::shared_ptr<test::SleepingBackgroundTask> low_sleeping_task_;
};

TEST_F(RACCompactionAdversarialTest,
       CompactionRewriteRateUsesWeightedRecentSamples) {
  ReopenOption2();
  RecordCompactionRewrite(/*source_level=*/1, /*output_level=*/2,
                          /*input_bytes=*/100, /*output_bytes=*/300,
                          /*elapsed_us=*/10);
  RecordCompactionRewrite(/*source_level=*/1, /*output_level=*/2,
                          /*input_bytes=*/200, /*output_bytes=*/100,
                          /*elapsed_us=*/20);

  ASSERT_DOUBLE_EQ(700.0 / 30.0, GetCompactionRewriteRate(1, 2));
  ASSERT_EQ(2U, GetCompactionRewriteSampleCount(1, 2));

  // Samples with a zero input, output, or elapsed time are not rewrites that
  // can contribute a meaningful throughput observation.
  RecordCompactionRewrite(1, 2, 0, 1000, 1);
  RecordCompactionRewrite(1, 2, 1000, 0, 1);
  RecordCompactionRewrite(1, 2, 1000, 1000, 0);
  ASSERT_DOUBLE_EQ(700.0 / 30.0, GetCompactionRewriteRate(1, 2));
  ASSERT_EQ(2U, GetCompactionRewriteSampleCount(1, 2));
}

TEST_F(RACCompactionAdversarialTest,
       CompactionRewriteRateKeepsNewestSixteenPerLevelPair) {
  ReopenOption2();
  RecordCompactionRewrite(/*source_level=*/1, /*output_level=*/2,
                          /*input_bytes=*/5000, /*output_bytes=*/5000,
                          /*elapsed_us=*/1);
  for (int sample = 0; sample < 16; ++sample) {
    RecordCompactionRewrite(/*source_level=*/1, /*output_level=*/2,
                            /*input_bytes=*/40, /*output_bytes=*/60,
                            /*elapsed_us=*/10);
  }

  ASSERT_DOUBLE_EQ(10.0, GetCompactionRewriteRate(1, 2));
  ASSERT_EQ(16U, GetCompactionRewriteSampleCount(1, 2));
}

TEST_F(RACCompactionAdversarialTest,
       CompactionRewriteRateIsIsolatedByLevelPairAndColumnFamily) {
  ReopenOption2();
  RecordCompactionRewrite(/*source_level=*/1, /*output_level=*/2,
                          /*input_bytes=*/40, /*output_bytes=*/60,
                          /*elapsed_us=*/10);
  RecordCompactionRewrite(/*source_level=*/0, /*output_level=*/1,
                          /*input_bytes=*/100, /*output_bytes=*/100,
                          /*elapsed_us=*/5);
  ASSERT_DOUBLE_EQ(10.0, GetCompactionRewriteRate(1, 2));
  ASSERT_DOUBLE_EQ(40.0, GetCompactionRewriteRate(0, 1));
  ASSERT_DOUBLE_EQ(0.0, GetCompactionRewriteRate(2, 3));
  ASSERT_EQ(1U, GetCompactionRewriteSampleCount(1, 2));
  ASSERT_EQ(1U, GetCompactionRewriteSampleCount(0, 1));
  ASSERT_EQ(0U, GetCompactionRewriteSampleCount(2, 3));

  ColumnFamilyHandle* second = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(Option2Options(), "second", &second));
  ColumnFamilyData* second_cfd = GetCfd(second);
  dbfull()->TEST_LockMutex();
  ASSERT_DOUBLE_EQ(
      0.0, second_cfd->internal_stats()->GetCompactionRewriteRate(1, 2));
  dbfull()->TEST_UnlockMutex();
  ASSERT_OK(db_->DestroyColumnFamilyHandle(second));
}

TEST_F(RACCompactionAdversarialTest,
       SuccessfulRewriteRecordsWholeCompactionRunElapsedTime) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.num_levels = 4;
  options.disable_auto_compactions = true;
  Reopen(options);
  MakeFileAtLevel(2, 0, 19, "target-");
  MakeFileAtLevel(1, 5, 14, "source-");
  FileMetaData* source = OnlyFileAtLevel(1);
  ASSERT_NE(source, nullptr);
  const std::string source_name = LiveFileName(source->fd.GetNumber());
  ASSERT_FALSE(source_name.empty());
  ASSERT_DOUBLE_EQ(0.0, GetCompactionRewriteRate(1, 2));

  env_->SetMockSleep();
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run:BeforeVerify",
      [&](void*) { env_->MockSleepForMicroseconds(250); });
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():End",
      [&](void*) { env_->MockSleepForMicroseconds(150); });
  SyncPoint::GetInstance()->EnableProcessing();
  CompactionJobInfo info;
  ASSERT_OK(db_->CompactFiles(CompactionOptions(), {source_name},
                              /*output_level=*/2, /*output_path_id=*/-1,
                              /*output_file_names=*/nullptr, &info));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  env_->SetMockSleep(false);

  ASSERT_GT(info.stats.total_input_bytes, 0U);
  ASSERT_GT(info.stats.total_output_bytes, 0U);
  ASSERT_GE(info.stats.elapsed_micros, 400U);
  ASSERT_DOUBLE_EQ(
      static_cast<double>(info.stats.total_input_bytes +
                          info.stats.total_output_bytes) /
          static_cast<double>(info.stats.elapsed_micros),
      GetCompactionRewriteRate(1, 2));
}

TEST_F(RACCompactionAdversarialTest,
       TrivialMoveAndFailedCompactionDoNotRecordRewriteRate) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.num_levels = 4;
  options.disable_auto_compactions = true;
  Reopen(options);
  MakeFileAtLevel(1, 0, 9, "trivial-");
  FileMetaData* source = OnlyFileAtLevel(1);
  ASSERT_NE(source, nullptr);
  const std::string source_name = LiveFileName(source->fd.GetNumber());
  ASSERT_FALSE(source_name.empty());
  const uint64_t moved_file_number = source->fd.GetNumber();
  CompactionOptions move_options;
  move_options.allow_trivial_move = true;
  ASSERT_OK(db_->CompactFiles(move_options, {source_name},
                             /*output_level=*/2));
  ASSERT_EQ(moved_file_number, OnlyFileAtLevel(2)->fd.GetNumber());
  ASSERT_DOUBLE_EQ(0.0, GetCompactionRewriteRate(1, 2));

  DestroyAndReopen(options);
  MakeFileAtLevel(2, 0, 19, "target-");
  MakeFileAtLevel(1, 5, 14, "source-");
  source = OnlyFileAtLevel(1);
  ASSERT_NE(source, nullptr);
  const std::string failing_source_name =
      LiveFileName(source->fd.GetNumber());
  ASSERT_FALSE(failing_source_name.empty());
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():AfterVerifyOutputFiles", [](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("injected failure");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_TRUE(db_->CompactFiles(CompactionOptions(), {failing_source_name},
                                /*output_level=*/2)
                  .IsIOError());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_DOUBLE_EQ(0.0, GetCompactionRewriteRate(1, 2));
}

TEST_F(RACCompactionAdversarialTest, RejectsInvalidOption2Configuration) {
  DBOptions db_options;
  ColumnFamilyOptions options;

  options.rac_design_option = 0;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.rac_design_option = 4;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());

  options.rac_design_option = 3;
  options.rac2_hotspot_persistence = -0.1;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.rac2_hotspot_persistence = 1.1;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.rac2_hotspot_persistence = 0.8;
  options.rac2_prediction_horizon = 0;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.rac2_prediction_horizon = 4;
  options.rac2_compaction_bandwidth_mib_per_sec = 0.0;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.rac2_compaction_bandwidth_mib_per_sec = 200.0;
  options.rac2_cost_weight = 0.0;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.rac2_cost_weight = -1.0;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.rac2_cost_weight = std::numeric_limits<double>::quiet_NaN();
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.rac2_cost_weight = std::numeric_limits<double>::infinity();
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.rac2_cost_weight = 2.0;
  ASSERT_OK(ColumnFamilyData::ValidateOptions(db_options, options));
  options.rac_option2_dp_projection_top_k = 0;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.rac_option2_dp_projection_top_k = 51;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.rac_option2_dp_projection_top_k = 50;
  ASSERT_OK(ColumnFamilyData::ValidateOptions(db_options, options));

  options.rac_design_option = 2;
  options.rac_window_size = 0;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.rac_window_size = kRACMaxWindowSize + 1;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.rac_window_size = 1;
  options.rac_top_k = 0;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.rac_top_k = 4;
  options.rac_benefit_efficiency_threshold =
      std::numeric_limits<double>::quiet_NaN();
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.rac_benefit_efficiency_threshold = 10.0;
  options.rac_bottom_overlap_threshold = -1.0;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.rac_bottom_overlap_threshold = 8.0;
  options.rac_option2_l0_efficiency_threshold = -1.0;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.rac_option2_l0_efficiency_threshold = 10.0;
  options.rac_option2_l0_cooldown_windows = 0;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.rac_option2_l0_cooldown_windows = 8;
  options.rac_cooldown_windows = 0;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.rac_cooldown_windows = 4;
  options.enable_read_triggered_compaction = true;
  options.disable_auto_compactions = true;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
  options.disable_auto_compactions = false;
  options.compaction_style = kCompactionStyleUniversal;
  ASSERT_OK(ColumnFamilyData::ValidateOptions(db_options, options));
  options.compaction_style = kCompactionStyleFIFO;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, options)
                  .IsInvalidArgument());
}

TEST_F(RACCompactionAdversarialTest,
       RuntimeOption2ActivationSelectorAndTunablesAreImmutable) {
  Options options = Option2Options();
  options.enable_read_triggered_compaction = false;
  Reopen(options);

  ASSERT_TRUE(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"enable_read_triggered_compaction", "true"}})
                  .IsInvalidArgument());
  ASSERT_TRUE(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"rac_design_option", "1"}})
                  .IsInvalidArgument());
  ASSERT_TRUE(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"rac_window_size", "2"}})
                  .IsInvalidArgument());
  ASSERT_TRUE(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"rac_top_k", "2"}})
                  .IsInvalidArgument());
  ASSERT_TRUE(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"rac2_hotspot_persistence", "0.5"}})
                  .IsInvalidArgument());
  ASSERT_TRUE(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"rac2_prediction_horizon", "2"}})
                  .IsInvalidArgument());
  ASSERT_TRUE(
      db_->SetOptions(
             db_->DefaultColumnFamily(),
             {{"rac2_compaction_bandwidth_mib_per_sec", "100.0"}})
          .IsInvalidArgument());
  ASSERT_TRUE(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"rac2_cost_weight", "0.5"}})
                  .IsInvalidArgument());
  ASSERT_TRUE(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"rac2_profiling_only", "true"}})
                  .IsInvalidArgument());
  ASSERT_TRUE(db_->SetOptions(
                     db_->DefaultColumnFamily(),
                     {{"rac_option2_dp_projection_top_k", "4"}})
                  .IsInvalidArgument());
  ASSERT_TRUE(db_->SetOptions(
                     db_->DefaultColumnFamily(),
                     {{"rac_option2_dp_allow_ttl_priority", "true"}})
                  .IsInvalidArgument());
  ASSERT_TRUE(
      db_->SetOptions(db_->DefaultColumnFamily(),
                      {{"rac_benefit_efficiency_threshold", "1.0"}})
          .IsInvalidArgument());
  ASSERT_TRUE(
      db_->SetOptions(db_->DefaultColumnFamily(),
                      {{"rac_option2_l0_efficiency_threshold", "5.0"}})
          .IsInvalidArgument());
  ASSERT_TRUE(
      db_->SetOptions(db_->DefaultColumnFamily(),
                      {{"rac_option2_l0_cooldown_windows", "4"}})
          .IsInvalidArgument());
  ASSERT_TRUE(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"rac_bottom_overlap_threshold", "1.0"}})
                  .IsInvalidArgument());
  ASSERT_TRUE(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"rac_cooldown_windows", "2"}})
                  .IsInvalidArgument());
  ASSERT_TRUE(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"disable_auto_compactions", "true"}})
                  .IsInvalidArgument());

  options.enable_read_triggered_compaction = true;
  Reopen(options);
  ASSERT_TRUE(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"enable_read_triggered_compaction", "false"}})
                  .IsInvalidArgument());
}

TEST_F(RACCompactionAdversarialTest,
       RuntimeOption1EnablementRemainsMutableForCompatibility) {
  Options options = CurrentOptions();
  options.enable_read_triggered_compaction = false;
  options.rac_design_option = 1;
  Reopen(options);

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_read_triggered_compaction", "true"}}));
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_read_triggered_compaction", "false"}}));
}

TEST_F(RACCompactionAdversarialTest,
       DualPurposeRuntimeOptionsUpdateAtomically) {
  ReopenOption2();
  ASSERT_OK(db_->SetOptions(
      db_->DefaultColumnFamily(),
      {{"rac_option2_dual_purpose_selection", "true"},
       {"rac_option2_read_benefit_slack", "0.95"},
       {"rac_option2_emerging_score_floor", "0.75"}}));
  const MutableCFOptions* configured =
      &DefaultCfd()->GetLatestMutableCFOptions();
  ASSERT_TRUE(configured->rac_option2_dual_purpose_selection);
  ASSERT_EQ(0.95, configured->rac_option2_read_benefit_slack);
  ASSERT_EQ(0.75, configured->rac_option2_emerging_score_floor);

  ASSERT_TRUE(db_->SetOptions(
                     db_->DefaultColumnFamily(),
                     {{"rac_option2_read_benefit_slack", "0"},
                      {"rac_option2_emerging_score_floor", "0.25"}})
                  .IsInvalidArgument());
  configured = &DefaultCfd()->GetLatestMutableCFOptions();
  ASSERT_TRUE(configured->rac_option2_dual_purpose_selection);
  ASSERT_EQ(0.95, configured->rac_option2_read_benefit_slack);
  ASSERT_EQ(0.75, configured->rac_option2_emerging_score_floor);
}

TEST_F(RACCompactionAdversarialTest,
       ExactWindowPublishesEveryOrdinalWithoutDroppingEvidence) {
  ReopenOption2(/*window_size=*/4);
  std::vector<std::unique_ptr<RACOption2LookupRecord>> lookups(4);
  for (auto& lookup : lookups) {
    ASSERT_OK(DefaultCfd()->BeginRACOption2Lookup(&lookup));
    ASSERT_NE(lookup, nullptr);
  }

  lookups[0]->RecordFileMiss(30, 3000, 2, 100);
  lookups[1]->RecordFileMiss(10, 1000, 1, 300);
  lookups[2]->RecordFileHit(20, 2000, 1);
  lookups[3]->RecordFileMiss(10, 1000, 1, 200);
  ASSERT_FALSE(lookups[2]->Finish());
  ASSERT_FALSE(lookups[0]->Finish());
  ASSERT_FALSE(lookups[3]->Finish());
  ASSERT_TRUE(lookups[1]->Finish());

  RACOption2EpochSnapshot snapshot;
  ASSERT_TRUE(DefaultCfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_EQ(snapshot.total_reads, 4U);
  ASSERT_EQ(snapshot.point_lookups, 4U);
  ASSERT_EQ(snapshot.files.size(), 3U);
  ASSERT_EQ(snapshot.files[0].file_number, 10U);
  ASSERT_EQ(snapshot.files[0].misses, 2U);
  ASSERT_EQ(snapshot.files[0].miss_latency_nanos, 500U);
  ASSERT_EQ(snapshot.files[1].file_number, 20U);
  ASSERT_EQ(snapshot.files[1].hits, 1U);
  ASSERT_EQ(snapshot.files[2].file_number, 30U);
  ASSERT_EQ(snapshot.files[2].misses, 1U);
  ASSERT_EQ(DefaultCfd()->GetRACOption2CollectorStats().invariant_violations,
            0U);
}

TEST_F(RACCompactionAdversarialTest,
       EmptyLookupsStillCountTowardExactWindowDenominator) {
  ReopenOption2(/*window_size=*/1);
  std::unique_ptr<RACOption2LookupRecord> empty;
  ASSERT_OK(DefaultCfd()->BeginRACOption2Lookup(&empty));
  ASSERT_TRUE(empty->Finish());

  RACOption2EpochSnapshot snapshot;
  ASSERT_TRUE(DefaultCfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_EQ(snapshot.total_reads, 1U);
  ASSERT_EQ(snapshot.point_lookups, 1U);
  ASSERT_TRUE(snapshot.files.empty());
}

TEST_F(RACCompactionAdversarialTest,
       ProfilingOnlyConsumesMoreThanRingCapacityWithoutPolicyEvaluation) {
  Options options = RAC2Options(/*window_size=*/1);
  options.rac2_profiling_only = true;
  options.statistics = CreateDBStatistics();
  Reopen(options);
  MakeFileAtLevel(1, 0, 9, "profiling-only-");
  FileMetaData* source = OnlyFileAtLevel(1);
  ASSERT_NE(source, nullptr);

  const uint64_t evaluated_before =
      options.statistics->getTickerCount(RAC_OPTION2_EVALUATED_WINDOWS);
  const uint64_t triggered_before =
      options.statistics->getTickerCount(RAC_OPTION2_TRIGGERED_WINDOWS);
  const uint64_t non_triggered_before =
      options.statistics->getTickerCount(RAC_OPTION2_NON_TRIGGERED_WINDOWS);
  constexpr uint64_t kWindows = kRACEpochRingCapacity + 3;
  constexpr uint64_t kMissLatencyNanos = 1234;
  for (uint64_t window = 0; window < kWindows; ++window) {
    SealMissEpoch(DefaultCfd(), source, /*level=*/1, kMissLatencyNanos);
    dbfull()->TEST_ForceRACOption2Evaluation();
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  dbfull()->TEST_LockMutex();
  ASSERT_FALSE(DefaultCfd()->GetRACOption2Request().active);
  const RACOption2CollectorStats collector_stats =
      DefaultCfd()->GetRACOption2CollectorStats();
  dbfull()->TEST_UnlockMutex();
  ASSERT_EQ(0U, collector_stats.ready_backlog);
  ASSERT_EQ(0U, collector_stats.reservation_stalls);
  ASSERT_EQ(0U, collector_stats.saturated_counters);
  ASSERT_EQ(evaluated_before,
            options.statistics->getTickerCount(RAC_OPTION2_EVALUATED_WINDOWS));
  ASSERT_EQ(triggered_before,
            options.statistics->getTickerCount(RAC_OPTION2_TRIGGERED_WINDOWS));
  ASSERT_EQ(
      non_triggered_before,
      options.statistics->getTickerCount(RAC_OPTION2_NON_TRIGGERED_WINDOWS));
  ASSERT_EQ(kWindows,
            options.statistics->getTickerCount(
                RAC_OPTION2_PROFILING_ONLY_WINDOWS));
  ASSERT_EQ(kWindows,
            options.statistics->getTickerCount(
                RAC_OPTION2_PROFILING_ONLY_POINT_LOOKUPS));
  ASSERT_EQ(0U, options.statistics->getTickerCount(
                    RAC_OPTION2_PROFILING_ONLY_FILE_HITS));
  ASSERT_EQ(kWindows,
            options.statistics->getTickerCount(
                RAC_OPTION2_PROFILING_ONLY_FILE_MISSES));
  ASSERT_EQ(kWindows * kMissLatencyNanos,
            options.statistics->getTickerCount(
                RAC_OPTION2_PROFILING_ONLY_MISS_LATENCY_NANOS));
}

TEST_F(RACCompactionAdversarialTest,
       AllMissEvidenceRetainsEveryMissAndLatencySample) {
  ReopenOption2(/*window_size=*/2);
  std::unique_ptr<RACOption2LookupRecord> first;
  std::unique_ptr<RACOption2LookupRecord> second;
  ASSERT_OK(DefaultCfd()->BeginRACOption2Lookup(&first));
  ASSERT_OK(DefaultCfd()->BeginRACOption2Lookup(&second));
  first->RecordFileMiss(7, 4096, 1, 20);
  second->RecordFileMiss(7, 4096, 1, 30);
  ASSERT_FALSE(first->Finish());
  ASSERT_TRUE(second->Finish());

  RACOption2EpochSnapshot snapshot;
  ASSERT_TRUE(DefaultCfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_EQ(snapshot.total_reads, 2U);
  ASSERT_EQ(snapshot.point_lookups, 2U);
  ASSERT_EQ(snapshot.files.size(), 1U);
  ASSERT_EQ(snapshot.files[0].misses, 2U);
  ASSERT_EQ(snapshot.files[0].hits, 0U);
  ASSERT_EQ(snapshot.files[0].miss_latency_nanos, 50U);
}

TEST_F(RACCompactionAdversarialTest,
       ZeroBenefitIsRejectedWhenEfficiencyThresholdIsZero) {
  ReopenOption2(/*window_size=*/1);
  MakeFileAtLevel(1, 0, 9, "zero-benefit-");
  FileMetaData* source = OnlyFileAtLevel(1);
  ASSERT_NE(source, nullptr);
  SealMissEpoch(DefaultCfd(), source, /*level=*/1, /*latency_nanos=*/0);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  ASSERT_FALSE(DefaultCfd()->GetRACOption2Request().active);
  dbfull()->TEST_UnlockMutex();
}

TEST_F(RACCompactionAdversarialTest,
       SeedAtOrBelowTenMiBIsExcludedBeforeRACPlanning) {
  ReopenOption2(/*window_size=*/1);
  MakeFileAtLevel(2, 100, 109, "deepest-anchor-");
  MakeFileAtLevel(1, 0, 9, "small-seed-");
  FileMetaData* source = OnlyFileAtLevel(1);
  ASSERT_NE(source, nullptr);
  ASSERT_LE(source->fd.GetFileSize(), kRACOption2MinSeedFileSize);

  PauseRACCompactionThread();
  SealMissEpoch(DefaultCfd(), source, /*level=*/1,
                /*latency_nanos=*/1000000000);
  EnforceMinSeedFileSize();
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  ASSERT_FALSE(DefaultCfd()->GetRACOption2Request().active);
  dbfull()->TEST_UnlockMutex();
  ResumeRACCompactionThread();
}

TEST_F(RACCompactionAdversarialTest,
       CostModelLearnsDurationAndChargesFutureRewritePressure) {
  ReopenOption2();
  dbfull()->TEST_LockMutex();
  ASSERT_DOUBLE_EQ(
      1.0, DefaultCfd()->GetRACOption2CompactionDurationFactor(false));
  DefaultCfd()->RecordRACOption2CompactionCost(
      /*read_only_window=*/false, /*rewrite_io_bytes=*/1024 * 1024,
      /*elapsed_micros=*/8192);
  const double duration_factor =
      DefaultCfd()->GetRACOption2CompactionDurationFactor(false);
  const uint64_t level_limit = CurrentStorageInfo()->MaxBytesForLevel(1);
  const uint64_t current_bytes = CurrentStorageInfo()->NumLevelBytes(1);
  const double rewrite_probability =
      DefaultCfd()->EstimateRACOption2FutureRewriteProbability(
          /*output_level=*/1, /*replaced_target_bytes=*/current_bytes,
          /*estimated_output_bytes=*/level_limit);
  dbfull()->TEST_UnlockMutex();

  ASSERT_DOUBLE_EQ(8.0, duration_factor);
  ASSERT_DOUBLE_EQ(1.0, rewrite_probability);
}

TEST_F(RACCompactionAdversarialTest,
       LeveledRequestUsesGrossBenefitAndCompleteInputCost) {
  ReopenOption2(/*window_size=*/1);
  MakeFileAtLevel(1, 0, 9, "target-");
  PutRange(0, 9, "source-");
  ASSERT_OK(Flush());
  FileMetaData* source = OnlyFileAtLevel(0);
  ASSERT_NE(source, nullptr);

  PauseRACCompactionThread();
  SealMissEpoch(DefaultCfd(), source, /*level=*/0,
                /*latency_nanos=*/1000);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();

  ASSERT_TRUE(request.active);
  ASSERT_DOUBLE_EQ(1000.0, request.gross_benefit);
  ASSERT_DOUBLE_EQ(request.gross_benefit, request.persistent_benefit);
  ASSERT_DOUBLE_EQ(1.0, request.persistence_factor);
  ASSERT_DOUBLE_EQ(0.0, request.output_residual_benefit);
  ASSERT_DOUBLE_EQ(request.gross_benefit, request.benefit);
  ASSERT_EQ(request.total_input_bytes, request.estimated_output_bytes);
  ASSERT_EQ(2 * request.total_input_bytes, request.rewrite_io_bytes);
  ASSERT_DOUBLE_EQ(static_cast<double>(request.total_input_bytes) /
                       (1024.0 * 1024.0),
                   request.modeled_cost_mib);
  ASSERT_GE(request.future_rewrite_probability, 0.0);
  ASSERT_LE(request.future_rewrite_probability, 1.0);

  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       RAC2UsesObservedRewriteRateAndIgnoresLegacyThresholds) {
  Options options = RAC2Options(/*window_size=*/1);
  options.rac2_hotspot_persistence = 0.5;
  options.rac2_prediction_horizon = 2;
  options.rac2_compaction_bandwidth_mib_per_sec = 1e-9;
  options.rac_benefit_efficiency_threshold = 1e300;
  options.rac_option2_l0_efficiency_threshold = 1e300;
  Reopen(options);
  MakeFileAtLevel(2, 0, 9, "target-");
  MakeFileAtLevel(1, 0, 9, "source-");
  FileMetaData* source = OnlyFileAtLevel(1);
  ASSERT_NE(source, nullptr);
  const uint64_t total_input_bytes =
      source->fd.GetFileSize() + OnlyFileAtLevel(2)->fd.GetFileSize();

  // rate = (input + output) / elapsed = total_input_bytes bytes/us.
  RecordCompactionRewrite(/*source_level=*/1, /*output_level=*/2,
                          total_input_bytes, total_input_bytes,
                          /*elapsed_us=*/2);

  PauseRACCompactionThread();
  SealMissEpoch(DefaultCfd(), source, /*level=*/1,
                /*latency_nanos=*/4000);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();

  ASSERT_TRUE(request.active);
  ASSERT_TRUE(request.uses_time_model);
  ASSERT_DOUBLE_EQ(4000.0, request.gross_benefit);
  ASSERT_DOUBLE_EQ(0.75, request.future_benefit_multiplier);
  ASSERT_DOUBLE_EQ(3.0, request.predicted_benefit_us);
  ASSERT_DOUBLE_EQ(request.predicted_benefit_us, request.benefit);
  ASSERT_EQ(total_input_bytes, request.total_input_bytes);
  ASSERT_DOUBLE_EQ(static_cast<double>(total_input_bytes),
                   request.rate_bytes_per_us);
  ASSERT_DOUBLE_EQ(1.0, request.estimated_cost_us);
  // Physical I/O accounting includes output even though scoring uses input only.
  ASSERT_EQ(total_input_bytes, request.estimated_output_bytes);
  ASSERT_EQ(2 * total_input_bytes, request.rewrite_io_bytes);
  ASSERT_DOUBLE_EQ(2.0, request.predicted_execution_time_us);
  ASSERT_DOUBLE_EQ(static_cast<double>(total_input_bytes),
                   request.prediction_rate_bytes_per_us);
  ASSERT_EQ(1U, request.prediction_history_samples);
  ASSERT_FALSE(request.prediction_used_fallback);
  ASSERT_DOUBLE_EQ(3.0, request.efficiency);
  ASSERT_DOUBLE_EQ(0.0, request.future_rewrite_probability);
  ASSERT_DOUBLE_EQ(0.0, request.future_rewrite_bytes);

  // A later completed job may change history, but must not re-predict this
  // already selected request before it reaches the compaction worker.
  RecordCompactionRewrite(/*source_level=*/1, /*output_level=*/2,
                          total_input_bytes, total_input_bytes,
                          /*elapsed_us=*/20);
  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest frozen_request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_DOUBLE_EQ(request.predicted_execution_time_us,
                   frozen_request.predicted_execution_time_us);
  ASSERT_DOUBLE_EQ(request.prediction_rate_bytes_per_us,
                   frozen_request.prediction_rate_bytes_per_us);
  ASSERT_EQ(request.prediction_history_samples,
            frozen_request.prediction_history_samples);

  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       RAC2BenefitIncludesTargetMissLatency) {
  Options options = RAC2Options(/*window_size=*/1);
  options.rac2_hotspot_persistence = 1.0;
  options.rac2_prediction_horizon = 1;
  Reopen(options);
  MakeFileAtLevel(2, 0, 9, "target-");
  MakeFileAtLevel(1, 0, 9, "source-");
  FileMetaData* source = OnlyFileAtLevel(1);
  FileMetaData* target = OnlyFileAtLevel(2);
  ASSERT_NE(source, nullptr);
  ASSERT_NE(target, nullptr);
  const uint64_t bytes = source->fd.GetFileSize() + target->fd.GetFileSize();
  RecordCompactionRewrite(1, 2, bytes, bytes, 2);

  PauseRACCompactionThread();
  SealSourceAndTargetEpoch(source, target, 1, 0, 1000, 3000);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(request.active);
  ASSERT_DOUBLE_EQ(4.0, request.predicted_benefit_us);
  ASSERT_DOUBLE_EQ(1.0, request.estimated_cost_us);
  ASSERT_DOUBLE_EQ(4.0, request.efficiency);
  ASSERT_DOUBLE_EQ(1.0, request.miss_ratio);
  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       RAC2UniformMissCostUsesAllFilesOnceAndCountSeeds) {
  Options options = RAC2Options(/*window_size=*/3);
  options.rac2_ablation = 1;
  options.rac2_hotspot_persistence = 1.0;
  options.rac2_prediction_horizon = 1;
  options.rac_top_k = 1;
  options.rac_bottom_overlap_threshold = 1e12;
  options.compression = kNoCompression;
  Reopen(options);
  const std::pair<FileMetaData*, FileMetaData*> candidates =
      MakeDualPurposeSameLevelCandidates();
  ASSERT_NE(candidates.first, nullptr);
  ASSERT_NE(candidates.second, nullptr);
  const auto& sources = CurrentStorageInfo()->LevelFiles(1);
  const auto& targets = CurrentStorageInfo()->LevelFiles(2);
  FileMetaData* frequent = sources[0];
  FileMetaData* slow = sources[1];
  RecordCompactionRewrite(1, 2, 500000, 500000, 1);

  PauseRACCompactionThread();
  for (uint64_t i = 0; i < 3; ++i) {
    std::unique_ptr<RACOption2LookupRecord> lookup;
    BeginTrackedLookup(DefaultCfd(), &lookup);
    lookup->RecordFileMiss(frequent->fd.GetNumber(),
                           frequent->fd.GetFileSize(), 1, 100);
    if (i == 0) {
      lookup->RecordFileMiss(slow->fd.GetNumber(), slow->fd.GetFileSize(), 1,
                             10000);
      lookup->RecordFileMiss(targets[0]->fd.GetNumber(),
                             targets[0]->fd.GetFileSize(), 2, 50000);
      lookup->RecordFileMiss(targets[1]->fd.GetNumber(),
                             targets[1]->fd.GetFileSize(), 2, 20000);
    }
    ASSERT_EQ(i == 2, FinishTrackedLookup(lookup.get()));
  }
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(request.active);
  ASSERT_EQ(frequent->fd.GetNumber(), request.seed_file_number);
  // Four valid files, six misses: the unselected source/target also contribute
  // to the shared average, without being included in this candidate's count.
  ASSERT_NEAR(80300.0 / 6.0 * 4.0 / 1000.0,
              request.predicted_benefit_us, 1e-9);
  ASSERT_EQ(2U, request.input_file_numbers.size());
  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       RAC2SeedCostKeepsCompleteCompactionInputs) {
  Options options = RAC2Options(/*window_size=*/1);
  options.rac2_ablation = 2;
  options.rac2_hotspot_persistence = 1.0;
  options.rac2_prediction_horizon = 1;
  Reopen(options);
  MakeFileAtLevel(2, 0, 9, "target-");
  MakeFileAtLevel(1, 0, 9, "source-");
  FileMetaData* source = OnlyFileAtLevel(1);
  FileMetaData* target = OnlyFileAtLevel(2);
  ASSERT_NE(source, nullptr);
  ASSERT_NE(target, nullptr);
  const uint64_t bytes = source->fd.GetFileSize() + target->fd.GetFileSize();
  const uint64_t seed_bytes = source->fd.GetFileSize();
  const uint64_t target_number = target->fd.GetNumber();
  RecordCompactionRewrite(1, 2, bytes, bytes, 2);

  PauseRACCompactionThread();
  SealMissEpoch(DefaultCfd(), source, 1, 1000000);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(request.active);
  ASSERT_EQ(bytes, request.total_input_bytes);
  ASSERT_EQ(bytes, request.estimated_output_bytes);
  ASSERT_EQ(2U, request.input_file_numbers.size());
  ASSERT_NE(std::find(request.input_file_numbers.begin(),
                      request.input_file_numbers.end(), target_number),
             request.input_file_numbers.end());
  ASSERT_DOUBLE_EQ(static_cast<double>(seed_bytes) /
                       static_cast<double>(bytes),
                   request.estimated_cost_us);
  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(0U, CurrentStorageInfo()->LevelFiles(1).size());
}

TEST_F(RACCompactionAdversarialTest,
       RAC2CoreAdmitsPositiveRatioBelowOneAtColdStart) {
  Options options = RAC2Options(/*window_size=*/1);
  options.rac2_hotspot_persistence = 1.0;
  options.rac2_prediction_horizon = 1;
  options.rac2_compaction_bandwidth_mib_per_sec = 1.0;
  options.rac2_cost_weight = 2.0;
  Reopen(options);
  MakeFileAtLevel(2, 0, 9, "target-");
  MakeFileAtLevel(1, 0, 9, "source-");
  FileMetaData* source = OnlyFileAtLevel(1);
  ASSERT_NE(source, nullptr);
  ASSERT_DOUBLE_EQ(0.0, GetCompactionRewriteRate(1, 2));

  PauseRACCompactionThread();
  SealMissEpoch(DefaultCfd(), source, 1, 2000);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(request.active);
  ASSERT_DOUBLE_EQ(1024.0 * 1024.0 / 1000000.0,
                   request.rate_bytes_per_us);
  ASSERT_TRUE(std::isfinite(request.efficiency));
  ASSERT_GT(request.efficiency, 0.0);
  ASSERT_LT(request.efficiency, 1.0);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  const uint64_t repeated_request_id =
      DefaultCfd()->GetRACOption2Request().request_id;
  dbfull()->TEST_UnlockMutex();
  ASSERT_EQ(request.request_id, repeated_request_id);
  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       RAC2NoAdmissionOnlyBypassesZeroRatioGate) {
  struct AdmissionCase {
    uint32_t ablation;
    bool expect_request;
  };
  const AdmissionCase cases[] = {
      {0, false},
      {3, true},
  };
  for (const AdmissionCase& admission : cases) {
    SCOPED_TRACE(testing::Message() << "ablation=" << admission.ablation);
    Options options = RAC2Options(/*window_size=*/1);
    options.rac2_ablation = admission.ablation;
    options.rac2_hotspot_persistence = 0.0;
    options.rac2_prediction_horizon = 1;
    DestroyAndReopen(options);
    MakeFileAtLevel(2, 0, 9, "target-");
    MakeFileAtLevel(1, 0, 9, "source-");
    FileMetaData* source = OnlyFileAtLevel(1);
    ASSERT_NE(source, nullptr);
    const uint64_t bytes =
        source->fd.GetFileSize() + OnlyFileAtLevel(2)->fd.GetFileSize();
    RecordCompactionRewrite(1, 2, bytes, bytes, 2);

    PauseRACCompactionThread();
    SealMissEpoch(DefaultCfd(), source, 1, 1000);
    dbfull()->TEST_ForceRACOption2Evaluation();
    dbfull()->TEST_LockMutex();
    const RACOption2CompactionRequest request =
        DefaultCfd()->GetRACOption2Request();
    dbfull()->TEST_UnlockMutex();
    ASSERT_EQ(admission.expect_request, request.active);
    if (request.active) {
      ASSERT_DOUBLE_EQ(0.0, request.predicted_benefit_us);
      ASSERT_DOUBLE_EQ(0.0, request.efficiency);
    }
    ResumeRACCompactionThread();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
}

TEST_F(RACCompactionAdversarialTest,
       RAC2DefaultSkipsSourceGuardWhileDesign2RetainsIt) {
  struct PolicyCase {
    uint32_t design;
    uint32_t ablation;
    bool dp;
    bool expect_request;
  };
  const PolicyCase cases[] = {
      {2, 0, false, false},
      {3, 0, false, true},
      {3, 0, true, true},
      {3, 4, false, true},
  };
  for (const PolicyCase& policy : cases) {
    SCOPED_TRACE(testing::Message() << "design=" << policy.design
                                   << " ablation=" << policy.ablation
                                   << " dp=" << policy.dp);
    Options options = RAC2Options(/*window_size=*/3);
    options.rac_design_option = policy.design;
    options.rac2_ablation = policy.ablation;
    options.rac_option2_dual_purpose_selection = policy.dp;
    options.rac2_hotspot_persistence = 1.0;
    options.rac2_prediction_horizon = 1;
    DestroyAndReopen(options);
    MakeFileAtLevel(2, 0, 9, "target-");
    MakeFileAtLevel(1, 0, 9, "source-");
    FileMetaData* source = OnlyFileAtLevel(1);
    FileMetaData* target = OnlyFileAtLevel(2);
    ASSERT_NE(source, nullptr);
    ASSERT_NE(target, nullptr);
    const uint64_t bytes = source->fd.GetFileSize() + target->fd.GetFileSize();
    RecordCompactionRewrite(1, 2, bytes, bytes, 2);

    PauseRACCompactionThread();
    SealSourceAndTargetEpoch(source, target, 1, 2, 1000, 3000);
    dbfull()->TEST_ForceRACOption2Evaluation();
    dbfull()->TEST_LockMutex();
    const RACOption2CompactionRequest request =
        DefaultCfd()->GetRACOption2Request();
    dbfull()->TEST_UnlockMutex();
    ASSERT_EQ(policy.expect_request, request.active);
    if (request.active) {
      ASSERT_NEAR(1.0 / 3.0, request.miss_ratio, 1e-12);
      ASSERT_DOUBLE_EQ(4.0, request.efficiency);
    }
    ResumeRACCompactionThread();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
}

TEST_F(RACCompactionAdversarialTest,
       RAC2TimeGuardUsesSourceLatencyAndRetainsEfficiencyGate) {
  struct GuardCase {
    uint32_t ablation;
    uint64_t misses;
    uint64_t hits;
    uint64_t miss_nanos;
    uint64_t hit_nanos;
    uint64_t target_miss_nanos;
    bool expect_request;
  };
  const GuardCase cases[] = {
      // Design 3 admits hit-dominated sources without a separate guard.
      {0, 1, 2, 3000, 100, 1000, true},
      {1, 1, 2, 3000, 100, 1000, true},
      {2, 1, 2, 3000, 100, 1000, true},
      {3, 1, 2, 3000, 100, 1000, true},
      {5, 1, 2, 3000, 100, 1000, true},
      // Target benefit cannot rescue a failed source-only time guard.
      {0, 2, 1, 100, 1000, 3000, true},
      {5, 2, 1, 100, 1000, 3000, false},
      {4, 2, 1, 100, 1000, 3000, true},
      // Equality is rejected, even with a positive complete-input benefit.
      {5, 1, 2, 1000, 500, 3000, false},
      {0, 1, 2, 1000, 500, 3000, true},
      // Any positive benefit/cost ratio passes the admission gate.
      {5, 1, 2, 300, 100, 100, true},
      {0, 1, 2, 300, 100, 100, true},
      {3, 1, 2, 300, 100, 100, true},
  };
  for (const GuardCase& guard_case : cases) {
    SCOPED_TRACE(testing::Message()
                 << "ablation=" << guard_case.ablation
                 << " miss_nanos=" << guard_case.miss_nanos
                 << " hit_nanos=" << guard_case.hit_nanos);
    Options options = RAC2Options(/*window_size=*/3);
    options.rac2_ablation = guard_case.ablation;
    options.rac2_hotspot_persistence = 1.0;
    options.rac2_prediction_horizon = 1;
    DestroyAndReopen(options);
    MakeFileAtLevel(2, 0, 9, "target-");
    MakeFileAtLevel(1, 0, 9, "source-");
    FileMetaData* source = OnlyFileAtLevel(1);
    FileMetaData* target = OnlyFileAtLevel(2);
    ASSERT_NE(source, nullptr);
    ASSERT_NE(target, nullptr);
    const uint64_t bytes = source->fd.GetFileSize() + target->fd.GetFileSize();
    // The input-only cost is exactly one microsecond.
    RecordCompactionRewrite(1, 2, bytes, bytes, 2);
    IgnoreMinSeedFileSizeForExistingTests();
    PauseRACCompactionThread();
    const uint64_t sequence = db_->GetLatestSequenceNumber();
    const uint64_t reads = guard_case.misses + guard_case.hits;
    for (uint64_t i = 0; i < reads; ++i) {
      RACOption2LookupRecord lookup;
      ASSERT_OK(DefaultCfd()->BeginRACOption2Lookup(&lookup, sequence));
      if (i < guard_case.misses) {
        lookup.RecordFileMiss(source->fd.GetNumber(), source->fd.GetFileSize(),
                              1, guard_case.miss_nanos);
        lookup.RecordFileMiss(target->fd.GetNumber(), target->fd.GetFileSize(),
                              2, guard_case.target_miss_nanos);
      } else {
        lookup.RecordFileHit(source->fd.GetNumber(), source->fd.GetFileSize(),
                             1, guard_case.hit_nanos);
      }
      ASSERT_EQ(i + 1 == reads, lookup.Finish(sequence));
    }
    dbfull()->TEST_ForceRACOption2Evaluation();
    dbfull()->TEST_LockMutex();
    const RACOption2CompactionRequest request =
        DefaultCfd()->GetRACOption2Request();
    dbfull()->TEST_UnlockMutex();
    ASSERT_EQ(guard_case.expect_request, request.active);
    if (request.active) {
      const double expected_benefit_us =
          static_cast<double>(guard_case.misses) *
          (guard_case.miss_nanos + guard_case.target_miss_nanos) / 1000.0;
      ASSERT_DOUBLE_EQ(expected_benefit_us, request.predicted_benefit_us);
      const double expected_cost_us =
          guard_case.ablation == 2
              ? static_cast<double>(source->fd.GetFileSize()) / bytes
              : 1.0;
      ASSERT_DOUBLE_EQ(expected_cost_us, request.estimated_cost_us);
      ASSERT_DOUBLE_EQ(expected_benefit_us / expected_cost_us,
                       request.efficiency);
      ASSERT_FALSE(request.dual_purpose_selected);
    }
    ResumeRACCompactionThread();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
}

TEST_F(RACCompactionAdversarialTest,
       RAC2PauseEvaluationLeavesSealedWindowUnevaluated) {
  Options options = RAC2Options(/*window_size=*/1);
  options.rac2_pause_evaluation = true;
  Reopen(options);
  MakeFileAtLevel(2, 0, 9, "target-");
  MakeFileAtLevel(1, 0, 9, "source-");
  FileMetaData* source = OnlyFileAtLevel(1);
  ASSERT_NE(source, nullptr);
  const uint64_t bytes =
      source->fd.GetFileSize() + OnlyFileAtLevel(2)->fd.GetFileSize();
  RecordCompactionRewrite(1, 2, bytes, bytes, 2);
  PauseRACCompactionThread();
  SealMissEpoch(DefaultCfd(), source, 1, 1000000);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  const bool initially_active = DefaultCfd()->HasRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_FALSE(initially_active);
  ASSERT_OK(db_->SetOptions({{"rac2_pause_evaluation", "false"}}));
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  const bool resumed_active = DefaultCfd()->HasRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(resumed_active);
  ASSERT_OK(db_->SetOptions({{"rac2_pause_evaluation", "true"}}));
  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       RAC2ColdStartUsesConfiguredFallbackRateAndCost) {
  Options options = RAC2Options(/*window_size=*/1);
  options.rac2_hotspot_persistence = 1.0;
  options.rac2_prediction_horizon = 1;
  options.rac2_compaction_bandwidth_mib_per_sec = 123.0;
  Reopen(options);
  MakeFileAtLevel(2, 0, 9, "target-");
  MakeFileAtLevel(1, 0, 9, "high-miss-");
  Reopen(options);
  FileMetaData* source = OnlyFileAtLevel(1);
  FileMetaData* target = OnlyFileAtLevel(2);
  ASSERT_NE(source, nullptr);
  ASSERT_NE(target, nullptr);
  ASSERT_DOUBLE_EQ(0.0, GetCompactionRewriteRate(1, 2));
  const uint64_t total_input_bytes =
      source->fd.GetFileSize() + target->fd.GetFileSize();
  const double fallback_rate = 123.0 * 1024.0 * 1024.0 / 1000000.0;

  PauseRACCompactionThread();
  SealMissEpoch(DefaultCfd(), source, /*level=*/1,
                /*latency_nanos=*/1000000000);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(request.active);
  ASSERT_DOUBLE_EQ(fallback_rate, request.rate_bytes_per_us);
  ASSERT_EQ(total_input_bytes, request.total_input_bytes);
  ASSERT_DOUBLE_EQ(static_cast<double>(total_input_bytes) /
                       fallback_rate,
                   request.estimated_cost_us);
  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       RAC2MissingExactPairHistoryUsesConfiguredFallback) {
  Options options = RAC2Options(/*window_size=*/1);
  options.rac2_hotspot_persistence = 1.0;
  options.rac2_prediction_horizon = 1;
  options.rac2_compaction_bandwidth_mib_per_sec = 37.0;
  Reopen(options);
  MakeFileAtLevel(2, 0, 9, "target-");
  MakeFileAtLevel(1, 0, 9, "source-");
  FileMetaData* source = OnlyFileAtLevel(1);
  ASSERT_NE(source, nullptr);
  RecordCompactionRewrite(/*source_level=*/0, /*output_level=*/1,
                          /*input_bytes=*/1000, /*output_bytes=*/1000,
                          /*elapsed_us=*/1);
  ASSERT_DOUBLE_EQ(2000.0, GetCompactionRewriteRate(0, 1));
  ASSERT_DOUBLE_EQ(0.0, GetCompactionRewriteRate(1, 2));
  const double fallback_rate = 37.0 * 1024.0 * 1024.0 / 1000000.0;

  PauseRACCompactionThread();
  SealMissEpoch(DefaultCfd(), source, /*level=*/1,
                /*latency_nanos=*/1000000000);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(request.active);
  ASSERT_DOUBLE_EQ(fallback_rate, request.rate_bytes_per_us);
  ASSERT_DOUBLE_EQ(fallback_rate, request.prediction_rate_bytes_per_us);
  ASSERT_EQ(0U, request.prediction_history_samples);
  ASSERT_TRUE(request.prediction_used_fallback);
  ASSERT_DOUBLE_EQ(
      static_cast<double>(request.rewrite_io_bytes) / fallback_rate,
      request.predicted_execution_time_us);
  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest, RAC2InputOnlyCostProducesExpectedRatio) {
  RACOption2CompactionRequest request;
  // A history sample taking 3 us gives input-only cost 1.5 us for I == O.
  // The previous full-rewrite, net-score admission would reject the same
  // 2 us benefit at q == 1.
  EvaluateRAC2WithSampleElapsed(/*elapsed_us=*/3, /*cost_weight=*/1.0,
                                &request);
  ASSERT_TRUE(request.active);
  ASSERT_DOUBLE_EQ(2.0, request.predicted_benefit_us);
  ASSERT_DOUBLE_EQ(1.5, request.estimated_cost_us);
  ASSERT_DOUBLE_EQ(3.0, request.predicted_execution_time_us);
  ASSERT_EQ(1U, request.prediction_history_samples);
  ASSERT_FALSE(request.prediction_used_fallback);
  ASSERT_NEAR(4.0 / 3.0, request.efficiency, 1e-12);
}

TEST_F(RACCompactionAdversarialTest,
       RAC2PositiveRatioAdmitsAtAndBelowOne) {
  RACOption2CompactionRequest unit_ratio;
  EvaluateRAC2WithSampleElapsed(/*elapsed_us=*/4, /*cost_weight=*/1.0,
                                &unit_ratio);
  ASSERT_TRUE(unit_ratio.active);
  ASSERT_DOUBLE_EQ(1.0, unit_ratio.efficiency);

  RACOption2CompactionRequest below_one;
  EvaluateRAC2WithSampleElapsed(/*elapsed_us=*/8, /*cost_weight=*/1.0,
                                &below_one);
  ASSERT_TRUE(below_one.active);
  ASSERT_DOUBLE_EQ(0.5, below_one.efficiency);
}

TEST_F(RACCompactionAdversarialTest,
       RAC2CostWeightScalesScoreWithoutChangingAdmission) {
  RACOption2CompactionRequest default_weight;
  EvaluateRAC2WithSampleElapsed(/*elapsed_us=*/6, /*cost_weight=*/1.0,
                                &default_weight);
  ASSERT_TRUE(default_weight.active);
  ASSERT_NEAR(2.0 / 3.0, default_weight.efficiency, 1e-12);

  RACOption2CompactionRequest reduced_weight;
  EvaluateRAC2WithSampleElapsed(/*elapsed_us=*/6, /*cost_weight=*/0.5,
                                &reduced_weight);
  ASSERT_TRUE(reduced_weight.active);
  ASSERT_DOUBLE_EQ(2.0, reduced_weight.predicted_benefit_us);
  ASSERT_DOUBLE_EQ(3.0, reduced_weight.estimated_cost_us);
  ASSERT_NEAR(4.0 / 3.0, reduced_weight.efficiency, 1e-12);
}

TEST_F(RACCompactionAdversarialTest,
       RAC2CostWeightKeepsUnitRatioAdmitted) {
  RACOption2CompactionRequest boundary;
  EvaluateRAC2WithSampleElapsed(/*elapsed_us=*/8, /*cost_weight=*/0.5,
                                &boundary);
  ASSERT_TRUE(boundary.active);
  ASSERT_DOUBLE_EQ(1.0, boundary.efficiency);
}

TEST_F(RACCompactionAdversarialTest,
       RAC2CostWeightAboveOneKeepsPositiveRatioAdmitted) {
  RACOption2CompactionRequest default_weight;
  EvaluateRAC2WithSampleElapsed(/*elapsed_us=*/2, /*cost_weight=*/1.0,
                                &default_weight);
  ASSERT_TRUE(default_weight.active);
  ASSERT_DOUBLE_EQ(2.0, default_weight.predicted_benefit_us);
  ASSERT_DOUBLE_EQ(1.0, default_weight.estimated_cost_us);
  ASSERT_DOUBLE_EQ(2.0, default_weight.efficiency);

  RACOption2CompactionRequest increased_weight;
  EvaluateRAC2WithSampleElapsed(/*elapsed_us=*/2, /*cost_weight=*/3.0,
                                &increased_weight);
  ASSERT_TRUE(increased_weight.active);
  ASSERT_NEAR(2.0 / 3.0, increased_weight.efficiency, 1e-12);
}

TEST_F(RACCompactionAdversarialTest,
       DeepestPopulatedLevelIsExcludedFromLeveledRACSeeds) {
  ReopenOption2(/*window_size=*/1);
  MakeFileAtLevel(2, 100, 109, "deepest-");
  MakeFileAtLevel(1, 0, 9, "source-");
  FileMetaData* deepest = OnlyFileAtLevel(2);
  FileMetaData* source = OnlyFileAtLevel(1);
  ASSERT_NE(deepest, nullptr);
  ASSERT_NE(source, nullptr);

  PauseRACCompactionThread();
  SealMissEpoch(DefaultCfd(), deepest, /*level=*/2);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  ASSERT_FALSE(DefaultCfd()->GetRACOption2Request().active);
  dbfull()->TEST_UnlockMutex();

  SealMissEpoch(DefaultCfd(), source, /*level=*/1);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(request.active);
  ASSERT_EQ(request.seed_file_number, source->fd.GetNumber());
  ASSERT_EQ(request.source_level, 1);
  ASSERT_EQ(request.output_level, 2);

  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       ConfiguredHighMissRatioBypassesEfficiencyThreshold) {
  Options options = Option2Options(/*window_size=*/100);
  options.rac_benefit_efficiency_threshold = 1e300;
  options.rac_high_miss_ratio_threshold = 0.90;
  Reopen(options);
  MakeFileAtLevel(2, 100, 109, "deepest-anchor-");
  MakeFileAtLevel(1, 0, 9, "high-miss-");
  // Test setup writes are foreground operations too. Reopen to start the
  // measured epoch at an exact operation-window boundary.
  Reopen(options);
  FileMetaData* source = OnlyFileAtLevel(1);
  ASSERT_NE(source, nullptr);

  PauseRACCompactionThread();
  SealMissRatioEpoch(DefaultCfd(), source, /*level=*/1, /*misses=*/96,
                     /*hits=*/4);
  dbfull()->TEST_ForceRACOption2Evaluation();

  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(request.active);
  ASSERT_TRUE(request.high_miss_triggered);
  ASSERT_EQ(request.seed_file_number, source->fd.GetNumber());
  ASSERT_NEAR(request.miss_ratio, 0.96, 1e-12);
  ASSERT_LT(request.efficiency,
            options.rac_benefit_efficiency_threshold);

  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       MissRatioAtNinetyFivePercentDoesNotBypassEfficiencyThreshold) {
  Options options = Option2Options(/*window_size=*/100);
  options.rac_benefit_efficiency_threshold = 1e300;
  Reopen(options);
  MakeFileAtLevel(2, 100, 109, "deepest-anchor-");
  MakeFileAtLevel(1, 0, 9, "boundary-miss-");
  Reopen(options);
  FileMetaData* source = OnlyFileAtLevel(1);
  ASSERT_NE(source, nullptr);

  SealMissRatioEpoch(DefaultCfd(), source, /*level=*/1, /*misses=*/95,
                     /*hits=*/5);
  dbfull()->TEST_ForceRACOption2Evaluation();

  dbfull()->TEST_LockMutex();
  const bool request_active = DefaultCfd()->GetRACOption2Request().active;
  dbfull()->TEST_UnlockMutex();
  ASSERT_FALSE(request_active);
}

TEST_F(RACCompactionAdversarialTest,
       HitDominatedExpandedSourceEvidenceIsRepresentedExactly) {
  ReopenOption2(/*window_size=*/3);
  std::unique_ptr<RACOption2LookupRecord> miss;
  std::unique_ptr<RACOption2LookupRecord> first_hit;
  std::unique_ptr<RACOption2LookupRecord> second_hit;
  ASSERT_OK(DefaultCfd()->BeginRACOption2Lookup(&miss));
  ASSERT_OK(DefaultCfd()->BeginRACOption2Lookup(&first_hit));
  ASSERT_OK(DefaultCfd()->BeginRACOption2Lookup(&second_hit));
  miss->RecordFileMiss(7, 4096, 1, 50);
  first_hit->RecordFileHit(7, 4096, 1);
  second_hit->RecordFileHit(7, 4096, 1);
  ASSERT_FALSE(miss->Finish());
  ASSERT_FALSE(first_hit->Finish());
  ASSERT_TRUE(second_hit->Finish());

  RACOption2EpochSnapshot snapshot;
  ASSERT_TRUE(DefaultCfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_EQ(snapshot.point_lookups, 3U);
  ASSERT_EQ(snapshot.files.size(), 1U);
  ASSERT_EQ(snapshot.files[0].misses, 1U);
  ASSERT_EQ(snapshot.files[0].hits, 2U);
  ASSERT_EQ(snapshot.files[0].miss_latency_nanos, 50U);
}

TEST_F(RACCompactionAdversarialTest,
       LaterSealedEpochWaitsForEarlierEpochAndEvidenceOrderIsStable) {
  ReopenOption2(/*window_size=*/1);
  std::unique_ptr<RACOption2LookupRecord> first;
  std::unique_ptr<RACOption2LookupRecord> second;
  ASSERT_OK(DefaultCfd()->BeginRACOption2Lookup(&first));
  ASSERT_OK(DefaultCfd()->BeginRACOption2Lookup(&second));
  first->RecordFileMiss(9, 100, 1, 50);
  second->RecordFileMiss(3, 100, 1, 50);
  ASSERT_TRUE(second->Finish());

  RACOption2EpochSnapshot snapshot;
  ASSERT_FALSE(DefaultCfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_TRUE(first->Finish());
  ASSERT_TRUE(DefaultCfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_EQ(snapshot.epoch_id, 0U);
  ASSERT_EQ(snapshot.files[0].file_number, 9U);
  ASSERT_TRUE(DefaultCfd()->FinishRACOption2Epoch(
      0, RACOption2EpochTerminalState::kRejected));
  ASSERT_TRUE(DefaultCfd()->TakeNextRACOption2Snapshot(&snapshot));
  ASSERT_EQ(snapshot.epoch_id, 1U);
  ASSERT_EQ(snapshot.files[0].file_number, 3U);
}

TEST_F(RACCompactionAdversarialTest,
       CooldownUsesInclusiveBoundaryAndPhysicallyPrunesOnExpiry) {
  ReopenOption2(/*window_size=*/1, /*cooldown_windows=*/4);
  const InternalKey installed_smallest("b", 1, kTypeValue);
  const InternalKey installed_largest("m", 1, kTypeValue);
  const InternalKey overlap_smallest("a", 1, kTypeValue);
  const InternalKey overlap_largest("c", 1, kTypeValue);
  const InternalKey disjoint_smallest("x", 1, kTypeValue);
  const InternalKey disjoint_largest("z", 1, kTypeValue);

  ASSERT_TRUE(InstallCooldown(DefaultCfd(), 11, installed_smallest,
                              installed_largest));
  ASSERT_FALSE(InstallCooldown(DefaultCfd(), 11, installed_smallest,
                               installed_largest));
  ASSERT_FALSE(InCooldown(DefaultCfd(), disjoint_smallest, disjoint_largest,
                          /*epoch=*/0));
  ASSERT_TRUE(InCooldown(DefaultCfd(), overlap_smallest, overlap_largest,
                         /*epoch=*/0));
  ASSERT_TRUE(InCooldown(DefaultCfd(), overlap_smallest, overlap_largest,
                         /*epoch=*/4));
  ASSERT_EQ(CooldownCount(DefaultCfd()), 1U);
  ASSERT_FALSE(InCooldown(DefaultCfd(), overlap_smallest, overlap_largest,
                          /*epoch=*/5));
  ASSERT_EQ(CooldownCount(DefaultCfd()), 0U);

  const std::string oversized_key(5 * 1024 * 1024, 'q');
  const InternalKey oversized(oversized_key, 1, kTypeValue);
  ASSERT_FALSE(InstallCooldown(DefaultCfd(), 12, oversized, oversized));
  ASSERT_EQ(CooldownCount(DefaultCfd()), 0U);
}

TEST_F(RACCompactionAdversarialTest,
       CooldownAnchorsAtCallbackTimeLatestCompletedEpoch) {
  ReopenOption2(/*window_size=*/1, /*cooldown_windows=*/1);
  FinishSingleLookupEpoch(DefaultCfd(), 0);
  FinishSingleLookupEpoch(DefaultCfd(), 1);
  FinishSingleLookupEpoch(DefaultCfd(), 2);
  const InternalKey smallest("a", 1, kTypeValue);
  const InternalKey largest("z", 1, kTypeValue);

  ASSERT_EQ(DefaultCfd()->GetLatestCompletedRACOption2Epoch(), 2U);
  ASSERT_TRUE(InstallCooldown(DefaultCfd(), 21, smallest, largest));
  ASSERT_TRUE(InCooldown(DefaultCfd(), smallest, largest, 2));
  ASSERT_TRUE(InCooldown(DefaultCfd(), smallest, largest, 3));
  ASSERT_FALSE(InCooldown(DefaultCfd(), smallest, largest, 4));
}

TEST_F(RACCompactionAdversarialTest,
       MissingSuccessfulInstallCallbackCreatesNoCooldown) {
  ReopenOption2();
  const InternalKey smallest("a", 1, kTypeValue);
  const InternalKey largest("z", 1, kTypeValue);

  // Rejected, cancelled, and failed compactions never call the successful
  // installation callback. Their shared observable result is no cooldown.
  ASSERT_EQ(CooldownCount(DefaultCfd()), 0U);
  ASSERT_FALSE(InCooldown(DefaultCfd(), smallest, largest, 0));
  ASSERT_FALSE(InstallCooldown(DefaultCfd(), 0, smallest, largest));
  ASSERT_EQ(CooldownCount(DefaultCfd()), 0U);
}

TEST_F(RACCompactionAdversarialTest,
       ReopenStartsWithFreshCollectorAndNoCooldown) {
  Options options = Option2Options(/*window_size=*/1);
  Reopen(options);
  const InternalKey smallest("a", 1, kTypeValue);
  const InternalKey largest("z", 1, kTypeValue);
  FinishSingleLookupEpoch(DefaultCfd(), 0);
  ASSERT_TRUE(InstallCooldown(DefaultCfd(), 31, smallest, largest));
  ASSERT_EQ(CooldownCount(DefaultCfd()), 1U);

  Reopen(options);
  ASSERT_EQ(DefaultCfd()->GetLatestCompletedRACOption2Epoch(), 0U);
  ASSERT_EQ(CooldownCount(DefaultCfd()), 0U);
}

TEST_F(RACCompactionAdversarialTest,
       MultipleColumnFamiliesKeepCollectorsAndCooldownsIndependent) {
  ReopenOption2(/*window_size=*/1);
  ColumnFamilyHandle* second = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(Option2Options(/*window_size=*/1),
                                    "second", &second));
  ColumnFamilyData* second_cfd = GetCfd(second);
  const InternalKey smallest("a", 1, kTypeValue);
  const InternalKey largest("z", 1, kTypeValue);

  FinishSingleLookupEpoch(DefaultCfd(), 0);
  ASSERT_EQ(second_cfd->GetLatestCompletedRACOption2Epoch(), 0U);
  ASSERT_TRUE(InstallCooldown(DefaultCfd(), 41, smallest, largest));
  ASSERT_EQ(CooldownCount(DefaultCfd()), 1U);
  ASSERT_EQ(CooldownCount(second_cfd), 0U);

  FinishSingleLookupEpoch(second_cfd, 0);
  ASSERT_TRUE(InstallCooldown(second_cfd, 42, smallest, largest));
  ASSERT_EQ(CooldownCount(DefaultCfd()), 1U);
  ASSERT_EQ(CooldownCount(second_cfd), 1U);
  ASSERT_OK(db_->DestroyColumnFamilyHandle(second));
}

TEST_F(RACCompactionAdversarialTest,
       DroppingColumnFamilyStopsCollectorAndClearsCooldown) {
  ReopenOption2();
  ColumnFamilyHandle* dropped = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(Option2Options(), "dropped", &dropped));
  ColumnFamilyData* dropped_cfd = GetCfd(dropped);
  const InternalKey smallest("a", 1, kTypeValue);
  const InternalKey largest("z", 1, kTypeValue);
  ASSERT_TRUE(InstallCooldown(dropped_cfd, 51, smallest, largest));
  ASSERT_EQ(CooldownCount(dropped_cfd), 1U);

  ASSERT_OK(db_->DropColumnFamily(dropped));
  ASSERT_TRUE(dropped_cfd->IsDropped());
  ASSERT_EQ(CooldownCount(dropped_cfd), 0U);
  RACOption2LookupRecord lookup;
  ASSERT_TRUE(dropped_cfd->BeginRACOption2Lookup(&lookup)
                  .IsShutdownInProgress());
  ASSERT_OK(db_->DestroyColumnFamilyHandle(dropped));
}

TEST_F(RACCompactionAdversarialTest,
       L0CandidateUsesCompleteExpandedSourceAndTargetInputs) {
  ReopenOption2();
  MakeFileAtLevel(1, 0, 19, "target-");
  PutRange(0, 9, "source-a-");
  ASSERT_OK(Flush());
  PutRange(5, 14, "source-b-");
  ASSERT_OK(Flush());

  const auto& l0_files = CurrentStorageInfo()->LevelFiles(0);
  const auto& l1_files = CurrentStorageInfo()->LevelFiles(1);
  ASSERT_EQ(l0_files.size(), 2U);
  ASSERT_EQ(l1_files.size(), 1U);
  // Pick the second L0 file so clean-cut expansion puts a different file at
  // inputs.front(); typed request ownership must still survive expansion.
  const uint64_t seed_number = l0_files.back()->fd.GetNumber();
  uint64_t expected_input_bytes = l1_files.front()->fd.GetFileSize();
  std::vector<uint64_t> expected_inputs = {l1_files.front()->fd.GetNumber()};
  for (const auto* file : l0_files) {
    expected_input_bytes += file->fd.GetFileSize();
    expected_inputs.push_back(file->fd.GetNumber());
  }
  std::sort(expected_inputs.begin(), expected_inputs.end());

  PauseRACCompactionThread();
  SealMissEpoch(DefaultCfd(), l0_files.back(), /*level=*/0);
  dbfull()->TEST_ForceRACOption2Evaluation();

  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest pending =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  auto actual_inputs = pending.input_file_numbers;
  std::sort(actual_inputs.begin(), actual_inputs.end());
  ASSERT_TRUE(pending.active);
  ASSERT_EQ(pending.seed_file_number, seed_number);
  ASSERT_EQ(actual_inputs, expected_inputs);
  ASSERT_EQ(pending.total_input_bytes, expected_input_bytes);
  ASSERT_FALSE(pending.dual_purpose_selected);
  ASSERT_EQ(0U, pending.projection_version_number);
  ASSERT_TRUE(pending.pre_compaction_scores.empty());

  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       WindowWithHalfCommittedWritesSkipsRACPlanning) {
  ReopenOption2(/*window_size=*/4);
  MakeFileAtLevel(2, 0, 19, "l2-target-");
  MakeFileAtLevel(1, 5, 14, "l1-seed-");
  FileMetaData* seed = OnlyFileAtLevel(1);
  ASSERT_NE(seed, nullptr);

  IgnoreMinSeedFileSizeForExistingTests();
  PauseRACCompactionThread();
  const uint64_t sequence_before_writes = db_->GetLatestSequenceNumber();
  for (uint64_t i = 0; i < 4; ++i) {
    std::unique_ptr<RACOption2LookupRecord> lookup(
        new RACOption2LookupRecord());
    ASSERT_OK(DefaultCfd()->BeginRACOption2Lookup(
        lookup.get(), db_->GetLatestSequenceNumber()));
    lookup->RecordFileMiss(seed->fd.GetNumber(), seed->fd.GetFileSize(),
                           /*level=*/1, /*latency_nanos=*/1000);
    if (i == 0) {
      ASSERT_OK(Put("write-ratio-boundary-a", "value"));
      ASSERT_OK(Put("write-ratio-boundary-b", "value"));
      ASSERT_EQ(sequence_before_writes + 2,
                db_->GetLatestSequenceNumber());
    }
    ASSERT_EQ(i == 3, lookup->Finish(db_->GetLatestSequenceNumber()));
  }

  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  ASSERT_FALSE(DefaultCfd()->GetRACOption2Request().active);
  dbfull()->TEST_UnlockMutex();

  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       L0CandidateIsRejectedWhenWindowContainsCommittedWrite) {
  ReopenOption2(/*window_size=*/4);
  MakeFileAtLevel(1, 0, 19, "l1-target-");
  PutRange(5, 14, "l0-seed-");
  ASSERT_OK(Flush());
  ReopenOption2(/*window_size=*/4);
  FileMetaData* seed = OnlyFileAtLevel(0);
  ASSERT_NE(seed, nullptr);

  PauseRACCompactionThread();
  SealMixedWriteEpoch(DefaultCfd(), seed, /*level=*/0);

  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  const bool request_active = DefaultCfd()->GetRACOption2Request().active;
  dbfull()->TEST_UnlockMutex();
  ASSERT_FALSE(request_active);

  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       L0CandidateWithWriteIsAllowedWhenExplicitlyEnabled) {
  Options options = Option2Options(/*window_size=*/4);
  options.level0_file_num_compaction_trigger = 3;
  options.rac_option2_allow_l0_with_writes = true;
  options.rac_option2_l0_efficiency_threshold = 0.0;
  Reopen(options);
  MakeFileAtLevel(1, 0, 19, "l1-target-");
  PutRange(5, 14, "l0-seed-a-");
  ASSERT_OK(Flush());
  PutRange(10, 18, "l0-seed-b-");
  ASSERT_OK(Flush());
  const auto& l0_files = CurrentStorageInfo()->LevelFiles(0);
  ASSERT_EQ(2U, l0_files.size());
  FileMetaData* seed = l0_files.back();

  PauseRACCompactionThread();
  SealMixedWriteEpoch(DefaultCfd(), seed, /*level=*/0);

  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(request.active);
  ASSERT_EQ(0, request.source_level);
  ASSERT_EQ(seed->fd.GetNumber(), request.seed_file_number);

  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       MixedWindowL0CandidateRequiresPretriggerPressure) {
  Options options = Option2Options(/*window_size=*/4);
  options.level0_file_num_compaction_trigger = 3;
  options.rac_option2_allow_l0_with_writes = true;
  options.rac_option2_l0_efficiency_threshold = 0.0;
  Reopen(options);
  MakeFileAtLevel(1, 0, 19, "l1-target-");
  PutRange(5, 14, "l0-seed-");
  ASSERT_OK(Flush());
  FileMetaData* seed = OnlyFileAtLevel(0);
  ASSERT_NE(seed, nullptr);

  PauseRACCompactionThread();
  SealMixedWriteEpoch(DefaultCfd(), seed, /*level=*/0);

  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  ASSERT_FALSE(DefaultCfd()->GetRACOption2Request().active);
  dbfull()->TEST_UnlockMutex();

  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       MixedWindowL0CandidateUsesDedicatedEfficiencyThreshold) {
  Options options = Option2Options(/*window_size=*/4);
  options.level0_file_num_compaction_trigger = 3;
  options.rac_option2_allow_l0_with_writes = true;
  options.rac_option2_l0_efficiency_threshold = 1e300;
  Reopen(options);
  MakeFileAtLevel(1, 0, 19, "l1-target-");
  PutRange(5, 14, "l0-seed-a-");
  ASSERT_OK(Flush());
  PutRange(10, 18, "l0-seed-b-");
  ASSERT_OK(Flush());
  const auto& l0_files = CurrentStorageInfo()->LevelFiles(0);
  ASSERT_EQ(2U, l0_files.size());

  PauseRACCompactionThread();
  SealMixedWriteEpoch(DefaultCfd(), l0_files.back(), /*level=*/0);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  ASSERT_FALSE(DefaultCfd()->GetRACOption2Request().active);
  dbfull()->TEST_UnlockMutex();

  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest, MixedWindowL0CooldownUsesEpochDistance) {
  Options options = Option2Options();
  options.rac_option2_l0_cooldown_windows = 8;
  Reopen(options);

  dbfull()->TEST_LockMutex();
  ASSERT_FALSE(DefaultCfd()->RACOption2L0InCooldown(2));
  DefaultCfd()->RecordRACOption2L0Trigger(2);
  ASSERT_TRUE(DefaultCfd()->RACOption2L0InCooldown(2));
  ASSERT_TRUE(DefaultCfd()->RACOption2L0InCooldown(9));
  ASSERT_FALSE(DefaultCfd()->RACOption2L0InCooldown(10));
  dbfull()->TEST_UnlockMutex();
}

TEST_F(RACCompactionAdversarialTest,
       UniversalRequestUsesCompletePhysicalInputsAndRACOnlyDPFallback) {
  auto [source, target] = MakeUniversalRACCandidate(/*dual_purpose=*/true);
  ASSERT_NE(source, nullptr);
  ASSERT_NE(target, nullptr);
  const uint64_t expected_input_bytes =
      source->fd.GetFileSize() + target->fd.GetFileSize();
  const std::vector<int> expected_levels = {1, 2};
  const std::vector<uint64_t> expected_physical_files = {
      source->fd.GetNumber(), target->fd.GetNumber()};
  std::vector<uint64_t> expected_files = expected_physical_files;
  std::sort(expected_files.begin(), expected_files.end());

  PauseLowCompactionThread();
  SealMissEpoch(DefaultCfd(), source, /*level=*/1);
  dbfull()->TEST_ForceRACOption2Evaluation();

  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(request.active);
  ASSERT_EQ(kCompactionStyleUniversal, request.compaction_style);
  ASSERT_EQ(source->fd.GetNumber(), request.seed_file_number);
  ASSERT_EQ(1, request.source_level);
  ASSERT_EQ(2, request.output_level);
  ASSERT_EQ(expected_input_bytes, request.total_input_bytes);
  ASSERT_EQ(expected_levels, request.physical_input_levels);
  ASSERT_EQ(expected_physical_files, request.physical_input_file_numbers);
  ASSERT_EQ(expected_files, request.input_file_numbers);
  ASSERT_FALSE(request.dual_purpose_selected);
  ASSERT_EQ(0U, request.projection_version_number);
  ASSERT_EQ(0U, request.projected_input_bytes);
  ASSERT_EQ(0U, request.shared_input_bytes);
  ASSERT_FALSE(request.exact_projected_input_coverage);
  ASSERT_TRUE(request.pre_compaction_score_levels.empty());
  ASSERT_TRUE(request.pre_compaction_scores.empty());

  ResumeLowCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       UniversalCandidateBelowEfficiencyThresholdIsRejected) {
  ReopenOption2();
  MakeFileAtLevel(2, 0, 19, std::string(512, 't'));
  MakeFileAtLevel(1, 5, 14, "universal-source-");
  Options options = UniversalOption2Options(
      /*dual_purpose=*/false, /*efficiency_threshold=*/1e300);
  options.rac_window_size = 2;
  // This test isolates the efficiency gate; do not let the independent
  // high-miss override bypass it.
  options.rac_high_miss_ratio_threshold = 1.0;
  Reopen(options);
  FileMetaData* source = OnlyFileAtLevel(1);
  FileMetaData* target = OnlyFileAtLevel(2);
  ASSERT_NE(source, nullptr);
  ASSERT_NE(target, nullptr);

  SealMissRatioEpoch(DefaultCfd(), source, /*level=*/1, /*misses=*/1,
                     /*hits=*/1);
  dbfull()->TEST_ForceRACOption2Evaluation();

  dbfull()->TEST_LockMutex();
  const bool request_active = DefaultCfd()->GetRACOption2Request().active;
  dbfull()->TEST_UnlockMutex();
  ASSERT_FALSE(request_active);
}

TEST_F(RACCompactionAdversarialTest,
       UniversalRequestExecutesWhenNormalMaintenanceIsNotDue) {
  auto [source, target] = MakeUniversalRACCandidate();
  ASSERT_NE(source, nullptr);
  ASSERT_NE(target, nullptr);
  std::atomic<bool> rac_install_seen{false};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::InstallSuperVersionAndScheduleWork:RACAfterInstallCallback",
      [&](void*) {
        rac_install_seen.store(true, std::memory_order_release);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  PauseLowCompactionThread();
  SealMissEpoch(DefaultCfd(), source, /*level=*/1);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  ASSERT_TRUE(DefaultCfd()->GetRACOption2Request().active);
  dbfull()->TEST_UnlockMutex();
  ResumeLowCompactionThread();
  const Status wait_status = dbfull()->TEST_WaitForCompact();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(wait_status);
  ASSERT_TRUE(rac_install_seen.load(std::memory_order_acquire));
  dbfull()->TEST_LockMutex();
  ASSERT_FALSE(DefaultCfd()->GetRACOption2Request().active);
  dbfull()->TEST_UnlockMutex();
  ASSERT_EQ(0U, CooldownCount(DefaultCfd()));
}

TEST_F(RACCompactionAdversarialTest,
       UniversalRequestIsStaleWhenAnyPhysicalInputStartsCompacting) {
  auto [source, target] = MakeUniversalRACCandidate();
  ASSERT_NE(source, nullptr);
  ASSERT_NE(target, nullptr);

  PauseLowCompactionThread();
  SealMissEpoch(DefaultCfd(), source, /*level=*/1);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  ASSERT_TRUE(DefaultCfd()->GetRACOption2Request().active);
  ASSERT_FALSE(DefaultCfd()->IsRACOption2RequestStale());
  target->being_compacted = true;
  ASSERT_TRUE(DefaultCfd()->IsRACOption2RequestStale());
  dbfull()->TEST_UnlockMutex();

  ResumeLowCompactionThread();
  const Status wait_status = dbfull()->TEST_WaitForCompact();
  dbfull()->TEST_LockMutex();
  target->being_compacted = false;
  const bool request_active = DefaultCfd()->GetRACOption2Request().active;
  dbfull()->TEST_UnlockMutex();
  ASSERT_OK(wait_status);
  ASSERT_FALSE(request_active);
}

TEST_F(RACCompactionAdversarialTest,
       NormalUniversalMaintenancePreemptsAndClearsConflictingRACRequest) {
  ReopenOption2();
  MakeFileAtLevel(3, 0, 19, std::string(512, 'b'));
  MakeFileAtLevel(2, 0, 19, std::string(512, 't'));
  MakeFileAtLevel(1, 5, 14, "universal-source-");
  ReopenUniversalOption2();
  FileMetaData* source = OnlyFileAtLevel(1);
  FileMetaData* target = OnlyFileAtLevel(2);
  ASSERT_NE(source, nullptr);
  ASSERT_NE(target, nullptr);
  const uint64_t source_number = source->fd.GetNumber();
  const uint64_t target_number = target->fd.GetNumber();

  PauseLowCompactionThread();
  SealMissEpoch(DefaultCfd(), source, /*level=*/1);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  ASSERT_TRUE(DefaultCfd()->GetRACOption2Request().active);
  dbfull()->TEST_UnlockMutex();

  dbfull()->TEST_LockMutex();
  target->marked_for_compaction = true;
  target->marked_for_compaction_source = MarkedForCompactionSource::kOther;
  CurrentStorageInfo()->ComputeCompactionScore(
      DefaultCfd()->ioptions(), DefaultCfd()->GetLatestMutableCFOptions(),
      DefaultCfd()->GetFullHistoryTsLow());
  dbfull()->TEST_UnlockMutex();
  std::atomic<bool> rac_install_seen{false};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::InstallSuperVersionAndScheduleWork:RACAfterInstallCallback",
      [&](void*) {
        rac_install_seen.store(true, std::memory_order_release);
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ResumeLowCompactionThread();
  const Status wait_status = dbfull()->TEST_WaitForCompact();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  bool source_present = false;
  bool target_present = false;
  dbfull()->TEST_LockMutex();
  const bool request_active = DefaultCfd()->GetRACOption2Request().active;
  for (int level = 0; level < DefaultCfd()->NumberLevels(); ++level) {
    for (const FileMetaData* file : CurrentStorageInfo()->LevelFiles(level)) {
      source_present =
          source_present || file->fd.GetNumber() == source_number;
      target_present =
          target_present || file->fd.GetNumber() == target_number;
    }
  }
  dbfull()->TEST_UnlockMutex();
  ASSERT_OK(wait_status);
  ASSERT_FALSE(rac_install_seen.load(std::memory_order_acquire));
  ASSERT_FALSE(request_active);
  ASSERT_TRUE(source_present);
  ASSERT_FALSE(target_present);
}

TEST_F(RACCompactionAdversarialTest,
       UniversalRequestRejectsChangedPhysicalMergeGroup) {
  ReopenOption2();
  MakeFileAtLevel(2, 0, 19, std::string(512, 't'));
  PutRange(0, 9, "universal-seed-");
  ASSERT_OK(Flush());
  Options universal_options = UniversalOption2Options();
  universal_options.rac_window_size = 11;
  Reopen(universal_options);
  FileMetaData* seed = OnlyFileAtLevel(0);
  ASSERT_NE(seed, nullptr);

  PauseLowCompactionThread();
  SealMissRatioEpoch(DefaultCfd(), seed, /*level=*/0, /*misses=*/11,
                     /*hits=*/0);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest original_request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(original_request.active);

  PutRange(5, 14, "new-overlap-");
  ASSERT_OK(Flush());
  std::atomic<bool> rac_install_seen{false};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::InstallSuperVersionAndScheduleWork:RACAfterInstallCallback",
      [&](void*) {
        rac_install_seen.store(true, std::memory_order_release);
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ResumeLowCompactionThread();
  const Status wait_status = dbfull()->TEST_WaitForCompact();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  dbfull()->TEST_LockMutex();
  const bool request_active = DefaultCfd()->GetRACOption2Request().active;
  dbfull()->TEST_UnlockMutex();
  ASSERT_OK(wait_status);
  ASSERT_FALSE(rac_install_seen.load(std::memory_order_acquire));
  ASSERT_FALSE(request_active);
  ASSERT_EQ(2U, CurrentStorageInfo()->LevelFiles(0).size());
}

TEST_F(RACCompactionAdversarialTest,
       UniversalDuplicateCandidatesUseStableLowestSeedTieBreak) {
  IgnoreMinSeedFileSizeForExistingTests();
  ReopenOption2(/*window_size=*/2);
  MakeFileAtLevel(2, 0, 19, std::string(512, 't'));
  PutRange(0, 9, "universal-source-a-");
  ASSERT_OK(Flush());
  PutRange(5, 14, "universal-source-b-");
  ASSERT_OK(Flush());
  Options universal_options = UniversalOption2Options();
  universal_options.rac_window_size = 2;
  Reopen(universal_options);
  const auto& l0_files = CurrentStorageInfo()->LevelFiles(0);
  ASSERT_EQ(2U, l0_files.size());
  const uint64_t expected_seed =
      std::min(l0_files[0]->fd.GetNumber(), l0_files[1]->fd.GetNumber());

  PauseLowCompactionThread();
  std::unique_ptr<RACOption2LookupRecord> first;
  std::unique_ptr<RACOption2LookupRecord> second;
  ASSERT_OK(DefaultCfd()->BeginRACOption2Lookup(&first));
  ASSERT_OK(DefaultCfd()->BeginRACOption2Lookup(&second));
  first->RecordFileMiss(l0_files[0]->fd.GetNumber(),
                        l0_files[0]->fd.GetFileSize(), 0, 1000);
  second->RecordFileMiss(l0_files[1]->fd.GetNumber(),
                         l0_files[1]->fd.GetFileSize(), 0, 1000);
  ASSERT_FALSE(first->Finish());
  ASSERT_TRUE(second->Finish());
  dbfull()->TEST_ForceRACOption2Evaluation();

  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(request.active);
  ASSERT_EQ(expected_seed, request.seed_file_number);
  ResumeLowCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       TopKPrefersCumulativeMissLatencyOverMissCount) {
  IgnoreMinSeedFileSizeForExistingTests();
  Options options = Option2Options(/*window_size=*/3);
  options.rac_top_k = 1;
  options.compression = kNoCompression;
  Reopen(options);
  MakeFileAtLevel(1, 200, 209, "deepest-anchor-");
  PutRange(0, 9, std::string(512, 'f'));
  ASSERT_OK(Flush());
  PutRange(100, 109, std::string(512, 's'));
  ASSERT_OK(Flush());
  const auto& l0_files = CurrentStorageInfo()->LevelFiles(0);
  ASSERT_EQ(2U, l0_files.size());
  FileMetaData* frequent_miss = l0_files[0];
  FileMetaData* slow_miss = l0_files[1];

  PauseRACCompactionThread();
  const uint64_t sequence = db_->GetLatestSequenceNumber();
  for (uint64_t i = 0; i < options.rac_window_size; ++i) {
    std::unique_ptr<RACOption2LookupRecord> lookup(
        new RACOption2LookupRecord());
    ASSERT_OK(DefaultCfd()->BeginRACOption2Lookup(lookup.get(), sequence));
    ASSERT_NE(lookup, nullptr);
    lookup->RecordFileMiss(frequent_miss->fd.GetNumber(),
                           frequent_miss->fd.GetFileSize(), 0, 100);
    if (i == 0) {
      lookup->RecordFileMiss(slow_miss->fd.GetNumber(),
                             slow_miss->fd.GetFileSize(), 0, 1000);
    }
    ASSERT_EQ(i + 1 == options.rac_window_size, lookup->Finish(sequence));
  }
  dbfull()->TEST_ForceRACOption2Evaluation();

  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(request.active);
  ASSERT_EQ(slow_miss->fd.GetNumber(), request.seed_file_number);

  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       SelectsHigherEfficiencyOverHigherRawBenefit) {
  Options options = Option2Options(/*window_size=*/2);
  options.rac_top_k = 2;
  options.compression = kNoCompression;
  Reopen(options);
  MakeFileAtLevel(1, 200, 209, "deepest-anchor-");
  PutRange(0, 9, std::string(4096, 'l'));
  ASSERT_OK(Flush());
  PutRange(100, 109, "small-");
  ASSERT_OK(Flush());
  Reopen(options);
  const auto& l0_files = CurrentStorageInfo()->LevelFiles(0);
  ASSERT_EQ(2U, l0_files.size());
  FileMetaData* large_file = l0_files[0];
  FileMetaData* small_file = l0_files[1];
  if (large_file->fd.GetFileSize() < small_file->fd.GetFileSize()) {
    std::swap(large_file, small_file);
  }
  ASSERT_GT(large_file->fd.GetFileSize(),
            2 * small_file->fd.GetFileSize());

  // The large file has twice the raw benefit but lower benefit per input byte.
  ASSERT_GT(1000.0 / static_cast<double>(small_file->fd.GetFileSize()),
            2000.0 / static_cast<double>(large_file->fd.GetFileSize()));
  PauseRACCompactionThread();
  std::unique_ptr<RACOption2LookupRecord> large_lookup;
  std::unique_ptr<RACOption2LookupRecord> small_lookup;
  BeginTrackedLookup(DefaultCfd(), &large_lookup);
  BeginTrackedLookup(DefaultCfd(), &small_lookup);
  large_lookup->RecordFileMiss(large_file->fd.GetNumber(),
                               large_file->fd.GetFileSize(), 0, 2000);
  small_lookup->RecordFileMiss(small_file->fd.GetNumber(),
                               small_file->fd.GetFileSize(), 0, 1000);
  ASSERT_FALSE(FinishTrackedLookup(large_lookup.get()));
  ASSERT_TRUE(FinishTrackedLookup(small_lookup.get()));
  dbfull()->TEST_ForceRACOption2Evaluation();

  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(request.active);
  ASSERT_EQ(small_file->fd.GetNumber(), request.seed_file_number);
  ASSERT_GT(large_file->fd.GetFileSize(), request.total_input_bytes);

  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       HighMissCandidatesStillPreferCompleteInputEfficiency) {
  IgnoreMinSeedFileSizeForExistingTests();
  Options options = Option2Options(/*window_size=*/100);
  options.rac_top_k = 2;
  options.rac_high_miss_ratio_threshold = 0.80;
  options.compression = kNoCompression;
  Reopen(options);
  MakeFileAtLevel(1, 200, 209, "deepest-anchor-");
  PutRange(0, 9, std::string(4096, 'l'));
  ASSERT_OK(Flush());
  PutRange(100, 109, "small-");
  ASSERT_OK(Flush());
  Reopen(options);
  const auto& l0_files = CurrentStorageInfo()->LevelFiles(0);
  ASSERT_EQ(2U, l0_files.size());
  FileMetaData* large_file = l0_files[0];
  FileMetaData* small_file = l0_files[1];
  if (large_file->fd.GetFileSize() < small_file->fd.GetFileSize()) {
    std::swap(large_file, small_file);
  }
  ASSERT_GT(large_file->fd.GetFileSize(),
            2 * small_file->fd.GetFileSize());

  PauseRACCompactionThread();
  const uint64_t sequence = db_->GetLatestSequenceNumber();
  for (uint64_t i = 0; i < options.rac_window_size; ++i) {
    std::unique_ptr<RACOption2LookupRecord> lookup(
        new RACOption2LookupRecord());
    ASSERT_OK(DefaultCfd()->BeginRACOption2Lookup(lookup.get(), sequence));
    ASSERT_NE(lookup, nullptr);
    lookup->RecordFileMiss(large_file->fd.GetNumber(),
                           large_file->fd.GetFileSize(), 0, 100);
    if (i + 1 < options.rac_window_size) {
      lookup->RecordFileMiss(small_file->fd.GetNumber(),
                             small_file->fd.GetFileSize(), 0, 100);
    } else {
      lookup->RecordFileHit(small_file->fd.GetNumber(),
                            small_file->fd.GetFileSize(), 0);
    }
    ASSERT_EQ(i + 1 == options.rac_window_size, lookup->Finish(sequence));
  }
  dbfull()->TEST_ForceRACOption2Evaluation();

  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(request.active);
  ASSERT_TRUE(request.high_miss_triggered);
  ASSERT_EQ(small_file->fd.GetNumber(), request.seed_file_number);
  ASSERT_NEAR(request.miss_ratio, 0.99, 1e-12);

  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       RAC2DualPurposeProjectionTopKFindsFourthPrioritySeed) {
  auto run = [&](uint32_t projection_top_k, bool expect_dual_purpose) {
    Options options = RAC2Options(/*window_size=*/2);
    options.statistics = CreateDBStatistics();
    options.rac2_hotspot_persistence = 1.0;
    options.rac2_prediction_horizon = 1;
    options.rac_option2_dual_purpose_selection = true;
    options.rac_option2_dp_projection_top_k = projection_top_k;
    options.rac_option2_read_benefit_slack = 0.01;
    options.rac_option2_emerging_score_floor = 0.0;
    options.compaction_pri = kMinOverlappingRatio;
    options.ttl = 0;
    options.compression = kNoCompression;
    DestroyAndReopen(options);

    for (int range = 0; range < 5; ++range) {
      const int begin = range * 100;
      MakeFileAtLevel(2, begin, begin + 9, "target-");
    }
    for (int range = 0; range < 5; ++range) {
      const int begin = range * 100;
      MakeFileAtLevel(1, begin, begin + 9, "source-");
    }

    std::vector<RACOption2ProjectedCompaction> projections;
    dbfull()->TEST_LockMutex();
    static_cast<LevelCompactionPicker*>(DefaultCfd()->compaction_picker())
        ->ProjectRACOption2SizeCompactions(
            DefaultCfd()->GetName(),
            DefaultCfd()->GetLatestMutableCFOptions(), CurrentStorageInfo(),
            DefaultCfd()->current()->GetVersionNumber(), 1, 5,
            &projections);
    dbfull()->TEST_UnlockMutex();
    ASSERT_EQ(5U, projections.size());
    for (const RACOption2ProjectedCompaction& projection : projections) {
      ASSERT_EQ(RACOption2ProjectionStatus::kValid, projection.status);
      ASSERT_EQ(RACOption2IntentProvenance::kPureSize,
                projection.provenance);
    }

    auto source_for_projection = [&](size_t projection_index) {
      const uint64_t file_number =
          projections[projection_index].projected_seed_file_number;
      for (FileMetaData* source : CurrentStorageInfo()->LevelFiles(1)) {
        if (source->fd.GetNumber() == file_number) {
          return source;
        }
      }
      return static_cast<FileMetaData*>(nullptr);
    };
    FileMetaData* aligned_seed = source_for_projection(3);
    FileMetaData* baseline_seed = source_for_projection(4);
    ASSERT_NE(aligned_seed, nullptr);
    ASSERT_NE(baseline_seed, nullptr);

    RecordCompactionRewrite(/*source_level=*/1, /*output_level=*/2,
                            /*input_bytes=*/500, /*output_bytes=*/500,
                            /*elapsed_us=*/2);
    PauseRACCompactionThread();
    // A read-only interval has a known, unchanged sequence number. DP must
    // still find the fourth maintenance projection without foreground writes.
    IgnoreMinSeedFileSizeForExistingTests();
    const uint64_t read_only_sequence = db_->GetLatestSequenceNumber();
    RACOption2LookupRecord first_lookup;
    RACOption2LookupRecord second_lookup;
    ASSERT_OK(DefaultCfd()->BeginRACOption2Lookup(&first_lookup,
                                                 read_only_sequence));
    ASSERT_OK(DefaultCfd()->BeginRACOption2Lookup(&second_lookup,
                                                 read_only_sequence));
    first_lookup.RecordFileMiss(
        aligned_seed->fd.GetNumber(), aligned_seed->fd.GetFileSize(), 1,
        5 * projections[3].total_unique_input_bytes / 2);
    second_lookup.RecordFileMiss(
        baseline_seed->fd.GetNumber(), baseline_seed->fd.GetFileSize(), 1,
        3 * projections[4].total_unique_input_bytes);
    ASSERT_FALSE(first_lookup.Finish(read_only_sequence));
    ASSERT_TRUE(second_lookup.Finish(read_only_sequence));
    ASSERT_EQ(read_only_sequence, db_->GetLatestSequenceNumber());
    dbfull()->TEST_ForceRACOption2Evaluation();

    dbfull()->TEST_LockMutex();
    const RACOption2CompactionRequest request =
        DefaultCfd()->GetRACOption2Request();
    dbfull()->TEST_UnlockMutex();
    ASSERT_TRUE(request.active);
    ASSERT_EQ(baseline_seed->fd.GetNumber(),
              request.baseline_seed_file_number);
    ASSERT_EQ(expect_dual_purpose ? aligned_seed->fd.GetNumber()
                                  : baseline_seed->fd.GetNumber(),
              request.seed_file_number);
    ASSERT_EQ(expect_dual_purpose, request.dual_purpose_selected);
    ASSERT_EQ(expect_dual_purpose,
              request.exact_projected_input_coverage);

    ResumeRACCompactionThread();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ(options.statistics->getTickerCount(
                  RAC_OPTION2_DP_COMPACTIONS),
              expect_dual_purpose ? 1U : 0U);
  };

  // Neither admitted read candidate shares inputs with the first three
  // maintenance projections. The fourth projection is an exact match.
  run(/*projection_top_k=*/3, /*expect_dual_purpose=*/false);
  run(/*projection_top_k=*/4, /*expect_dual_purpose=*/true);
}

TEST_F(RACCompactionAdversarialTest,
       RAC2DualPurposeTtlPriorityRequiresOptInAndNoOtherReason) {
  auto run = [&](bool allow_ttl_priority, bool add_periodic_reason,
                 bool expect_dual_purpose) {
    Options options = RAC2Options(/*window_size=*/2);
    options.statistics = CreateDBStatistics();
    options.rac2_hotspot_persistence = 1.0;
    options.rac2_prediction_horizon = 1;
    options.rac_option2_dual_purpose_selection = true;
    options.rac_option2_dp_allow_ttl_priority = allow_ttl_priority;
    options.rac_option2_read_benefit_slack = 0.01;
    options.rac_option2_emerging_score_floor = 0.0;
    options.compaction_pri = kMinOverlappingRatio;
    options.ttl = 2048;
    options.compression = kNoCompression;
    DestroyAndReopen(options);

    int64_t current_time = 0;
    ASSERT_OK(env_->GetCurrentTime(&current_time));
    const uint64_t boosted_but_not_expired_time =
        static_cast<uint64_t>(current_time - 1700);
    SyncPoint::GetInstance()->SetCallBack(
        "FlushJob::WriteLevel0Table:oldest_ancester_time",
        [&](void* arg) {
          *static_cast<uint64_t*>(arg) = boosted_but_not_expired_time;
        });
    SyncPoint::GetInstance()->EnableProcessing();
    auto [aligned_seed, baseline_seed] =
        MakeDualPurposeSameLevelCandidates();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_NE(aligned_seed, nullptr);
    ASSERT_NE(baseline_seed, nullptr);

    if (add_periodic_reason) {
      dbfull()->TEST_LockMutex();
      CurrentStorageInfo()->TEST_AddFileMarkedForPeriodicCompaction(
          1, aligned_seed);
      dbfull()->TEST_UnlockMutex();
    }

    RACOption2ProjectedCompaction projection;
    dbfull()->TEST_LockMutex();
    static_cast<LevelCompactionPicker*>(DefaultCfd()->compaction_picker())
        ->ProjectRACOption2SizeCompaction(
            DefaultCfd()->GetName(),
            DefaultCfd()->GetLatestMutableCFOptions(), CurrentStorageInfo(),
            DefaultCfd()->current()->GetVersionNumber(), 1, &projection);
    dbfull()->TEST_UnlockMutex();
    ASSERT_EQ(RACOption2ProjectionStatus::kValid, projection.status);
    ASSERT_EQ(RACOption2IntentProvenance::kMixedTtlPriority,
              projection.provenance);
    ASSERT_FALSE(projection.marked_priority_applied);
    ASSERT_EQ(add_periodic_reason,
              projection.other_compaction_reason_present);
    ASSERT_GT(projection.ttl_boost_score, 1U);

    RecordCompactionRewrite(/*source_level=*/1, /*output_level=*/2,
                            /*input_bytes=*/500, /*output_bytes=*/500,
                            /*elapsed_us=*/2);
    PauseRACCompactionThread();
    SealTwoCandidateEpoch(
        aligned_seed, 5 * projection.total_unique_input_bytes / 2,
        baseline_seed, 3 * RACInputBytes(baseline_seed, /*output_level=*/2));
    dbfull()->TEST_ForceRACOption2Evaluation();

    dbfull()->TEST_LockMutex();
    const RACOption2CompactionRequest request =
        DefaultCfd()->GetRACOption2Request();
    dbfull()->TEST_UnlockMutex();
    ASSERT_TRUE(request.active);
    ASSERT_EQ(baseline_seed->fd.GetNumber(),
              request.baseline_seed_file_number);
    ASSERT_EQ(expect_dual_purpose ? aligned_seed->fd.GetNumber()
                                  : baseline_seed->fd.GetNumber(),
              request.seed_file_number);
    ASSERT_EQ(expect_dual_purpose, request.dual_purpose_selected);

    ResumeRACCompactionThread();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ(options.statistics->getTickerCount(
                  RAC_OPTION2_DP_COMPACTIONS),
              expect_dual_purpose ? 1U : 0U);
  };

  run(/*allow_ttl_priority=*/false, /*add_periodic_reason=*/false,
      /*expect_dual_purpose=*/false);
  run(/*allow_ttl_priority=*/true, /*add_periodic_reason=*/false,
      /*expect_dual_purpose=*/true);
  run(/*allow_ttl_priority=*/true, /*add_periodic_reason=*/true,
      /*expect_dual_purpose=*/false);
}

TEST_F(RACCompactionAdversarialTest,
       DualPurposeCounterCountsOnlyChangedWinner) {
  Options options = RAC2Options(/*window_size=*/2);
  options.statistics = CreateDBStatistics();
  options.rac2_hotspot_persistence = 1.0;
  options.rac2_prediction_horizon = 1;
  options.rac_option2_dual_purpose_selection = true;
  options.rac_option2_read_benefit_slack = 0.01;
  options.rac_option2_emerging_score_floor = 0.0;
  options.compaction_pri = kMinOverlappingRatio;
  options.ttl = 0;
  options.compression = kNoCompression;
  Reopen(options);
  auto [aligned_seed, other_seed] = MakeDualPurposeSameLevelCandidates();
  ASSERT_NE(aligned_seed, nullptr);
  ASSERT_NE(other_seed, nullptr);

  RecordCompactionRewrite(/*source_level=*/1, /*output_level=*/2,
                          /*input_bytes=*/500, /*output_bytes=*/500,
                          /*elapsed_us=*/2);
  PauseRACCompactionThread();
  SealTwoCandidateEpoch(
      aligned_seed, 3 * RACInputBytes(aligned_seed, /*output_level=*/2),
      other_seed,
      5 * RACInputBytes(other_seed, /*output_level=*/2) / 2);
  dbfull()->TEST_ForceRACOption2Evaluation();

  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(request.active);
  ASSERT_EQ(aligned_seed->fd.GetNumber(),
            request.baseline_seed_file_number);
  ASSERT_EQ(aligned_seed->fd.GetNumber(), request.seed_file_number);
  ASSERT_FALSE(request.dual_purpose_selected);

  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(options.statistics->getTickerCount(RAC_OPTION2_COMPACTIONS), 1U);
  ASSERT_EQ(options.statistics->getTickerCount(RAC_OPTION2_DP_COMPACTIONS),
            0U);
}

TEST_F(RACCompactionAdversarialTest,
       RAC2OrderingAndDualPurposeSlackUseBenefitCostRatio) {
  Options options = RAC2Options(/*window_size=*/2);
  options.rac2_hotspot_persistence = 1.0;
  options.rac2_prediction_horizon = 1;
  options.rac_option2_dual_purpose_selection = true;
  options.rac_option2_read_benefit_slack = 0.8;
  options.rac_option2_emerging_score_floor = 0.0;
  options.compaction_pri = kMinOverlappingRatio;
  options.ttl = 0;
  options.compression = kNoCompression;
  Reopen(options);
  auto [aligned_seed, baseline_seed] = MakeDualPurposeSameLevelCandidates();
  ASSERT_NE(aligned_seed, nullptr);
  ASSERT_NE(baseline_seed, nullptr);
  const uint64_t aligned_bytes =
      RACInputBytes(aligned_seed, /*output_level=*/2);
  const uint64_t baseline_bytes =
      RACInputBytes(baseline_seed, /*output_level=*/2);
  RecordCompactionRewrite(1, 2, 500, 500, 2);  // 500 bytes/us.

  PauseRACCompactionThread();
  // The maintenance candidate's ratio is approximately 1.25 and the
  // baseline's ratio is exactly 1.5. Alpha=0.8 admits 1.25/1.5.
  SealTwoCandidateEpoch(aligned_seed, 5 * aligned_bytes / 2,
                        baseline_seed, 3 * baseline_bytes);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(request.active);
  ASSERT_TRUE(request.uses_time_model);
  ASSERT_EQ(aligned_seed->fd.GetNumber(), request.seed_file_number);
  ASSERT_EQ(baseline_seed->fd.GetNumber(), request.baseline_seed_file_number);
  ASSERT_NEAR(1.25, request.efficiency, 1e-12);
  ASSERT_TRUE(request.dual_purpose_selected);

  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       DualPurposeSelectsExactEmergingMaintenanceWithinReadSlack) {
  Options options = Option2Options(/*window_size=*/2);
  options.statistics = CreateDBStatistics();
  options.rac_option2_dual_purpose_selection = true;
  // The full rewrite-cost model charges output I/O and future rewrite pressure,
  // so this fixture's physically aligned candidate needs wider slack than an
  // input-bytes-only model.
  options.rac_option2_read_benefit_slack = 0.01;
  options.rac_option2_emerging_score_floor = 0.0;
  options.compaction_pri = kMinOverlappingRatio;
  options.ttl = 0;
  options.compression = kNoCompression;
  Reopen(options);
  auto [aligned_seed, baseline_seed] =
      MakeDualPurposeSameLevelCandidates();
  ASSERT_NE(aligned_seed, nullptr);
  ASSERT_NE(baseline_seed, nullptr);
  RACOption2ProjectedCompaction projected;
  dbfull()->TEST_LockMutex();
  static_cast<LevelCompactionPicker*>(DefaultCfd()->compaction_picker())
      ->ProjectRACOption2SizeCompaction(
          DefaultCfd()->GetName(),
          DefaultCfd()->GetLatestMutableCFOptions(), CurrentStorageInfo(),
          DefaultCfd()->current()->GetVersionNumber(), 1, &projected);
  dbfull()->TEST_UnlockMutex();
  ASSERT_EQ(RACOption2ProjectionStatus::kValid, projected.status);
  ASSERT_EQ(RACOption2IntentProvenance::kPureSize, projected.provenance);
  ASSERT_EQ(aligned_seed->fd.GetNumber(),
            projected.projected_seed_file_number);

  PauseRACCompactionThread();
  SealTwoCandidateEpoch(aligned_seed, 950, baseline_seed, 1000);
  dbfull()->TEST_ForceRACOption2Evaluation();

  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(request.active);
  ASSERT_EQ(baseline_seed->fd.GetNumber(),
            request.baseline_seed_file_number);
  ASSERT_EQ(aligned_seed->fd.GetNumber(), request.seed_file_number);
  ASSERT_TRUE(request.dual_purpose_selected);
  ASSERT_LT(request.maintenance_score, 1.0);
  ASSERT_TRUE(request.exact_projected_input_coverage);
  ASSERT_EQ(request.projected_input_bytes, request.shared_input_bytes);
  ASSERT_FALSE(request.pre_compaction_scores.empty());
  ASSERT_EQ(request.pre_compaction_score_levels.size(),
            request.pre_compaction_scores.size());

  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(options.statistics->getTickerCount(RAC_OPTION2_COMPACTIONS), 1U);
  ASSERT_EQ(options.statistics->getTickerCount(RAC_OPTION2_DP_COMPACTIONS),
            1U);
  ASSERT_GT(options.statistics->getTickerCount(
                RAC_OPTION2_DP_COMPACTION_INPUT_BYTES),
            0U);
  ASSERT_GT(options.statistics->getTickerCount(
                RAC_OPTION2_DP_COMPACTION_OUTPUT_BYTES),
            0U);
  ASSERT_EQ(options.statistics->getTickerCount(
                RAC_OPTION2_DP_PROJECTED_INPUT_BYTES),
            request.projected_input_bytes);
  ASSERT_EQ(options.statistics->getTickerCount(
                RAC_OPTION2_DP_SHARED_INPUT_BYTES),
            request.shared_input_bytes);
  ASSERT_EQ(options.statistics->getTickerCount(
                RAC_OPTION2_DP_EXACT_COVERAGE_COMPACTIONS),
            1U);
}

TEST_F(RACCompactionAdversarialTest,
       DualPurposeCanSelectSecondPriorityAfterTrivialMove) {
  Options options = Option2Options(/*window_size=*/2);
  options.statistics = CreateDBStatistics();
  options.rac_option2_dual_purpose_selection = true;
  options.rac_option2_read_benefit_slack = 0.01;
  options.rac_option2_emerging_score_floor = 0.0;
  options.compaction_pri = kMinOverlappingRatio;
  options.ttl = 0;
  options.compression = kNoCompression;
  Reopen(options);

  MakeFileAtLevel(2, 100, 109, "target-a-");
  MakeFileAtLevel(2, 200, 209, std::string(512, 't'));
  MakeFileAtLevel(1, 0, 9, "trivial-");
  MakeFileAtLevel(1, 100, 109, std::string(512, 's'));
  MakeFileAtLevel(1, 200, 209, "source-b-");
  const auto& sources = CurrentStorageInfo()->LevelFiles(1);
  ASSERT_EQ(3U, sources.size());
  FileMetaData* aligned_seed = sources[1];
  FileMetaData* baseline_seed = sources[2];

  RACOption2ProjectedCompaction top_projection;
  dbfull()->TEST_LockMutex();
  static_cast<LevelCompactionPicker*>(DefaultCfd()->compaction_picker())
      ->ProjectRACOption2SizeCompaction(
          DefaultCfd()->GetName(),
          DefaultCfd()->GetLatestMutableCFOptions(), CurrentStorageInfo(),
          DefaultCfd()->current()->GetVersionNumber(), 1, &top_projection);
  dbfull()->TEST_UnlockMutex();
  ASSERT_EQ(RACOption2ProjectionStatus::kTrivialMove,
            top_projection.status);

  PauseRACCompactionThread();
  SealTwoCandidateEpoch(aligned_seed, 950, baseline_seed, 1000);
  dbfull()->TEST_ForceRACOption2Evaluation();

  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(request.active);
  ASSERT_EQ(baseline_seed->fd.GetNumber(),
            request.baseline_seed_file_number);
  ASSERT_EQ(aligned_seed->fd.GetNumber(), request.seed_file_number);
  ASSERT_TRUE(request.dual_purpose_selected);
  ASSERT_TRUE(request.exact_projected_input_coverage);
  ASSERT_EQ(request.projected_input_bytes, request.shared_input_bytes);

  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(options.statistics->getTickerCount(RAC_OPTION2_DP_COMPACTIONS),
            1U);
}

TEST_F(RACCompactionAdversarialTest,
       DualPurposeCannotSelectCandidateOutsideReadSlack) {
  Options options = Option2Options(/*window_size=*/2);
  options.statistics = CreateDBStatistics();
  options.rac_option2_dual_purpose_selection = true;
  options.rac_option2_read_benefit_slack = 0.96;
  options.rac_option2_emerging_score_floor = 0.0;
  options.compaction_pri = kMinOverlappingRatio;
  options.ttl = 0;
  options.compression = kNoCompression;
  Reopen(options);
  auto [aligned_seed, baseline_seed] =
      MakeDualPurposeSameLevelCandidates();
  ASSERT_NE(aligned_seed, nullptr);
  ASSERT_NE(baseline_seed, nullptr);

  PauseRACCompactionThread();
  SealTwoCandidateEpoch(aligned_seed, 950, baseline_seed, 1000);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(request.active);
  ASSERT_EQ(baseline_seed->fd.GetNumber(), request.seed_file_number);
  ASSERT_FALSE(request.dual_purpose_selected);

  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(options.statistics->getTickerCount(RAC_OPTION2_COMPACTIONS), 1U);
  ASSERT_EQ(options.statistics->getTickerCount(RAC_OPTION2_DP_COMPACTIONS),
            0U);
}

TEST_F(RACCompactionAdversarialTest,
       DualPurposeCannotSelectCandidateBelowEmergingScoreFloor) {
  Options options = Option2Options(/*window_size=*/2);
  options.rac_option2_dual_purpose_selection = true;
  options.rac_option2_read_benefit_slack = 0.90;
  options.rac_option2_emerging_score_floor = 0.99;
  options.compaction_pri = kMinOverlappingRatio;
  options.ttl = 0;
  options.compression = kNoCompression;
  Reopen(options);
  auto [aligned_seed, baseline_seed] =
      MakeDualPurposeSameLevelCandidates();
  ASSERT_NE(aligned_seed, nullptr);
  ASSERT_NE(baseline_seed, nullptr);
  double source_score = -1.0;
  dbfull()->TEST_LockMutex();
  for (int rank = 0; rank <= CurrentStorageInfo()->MaxInputLevel(); ++rank) {
    if (CurrentStorageInfo()->CompactionScoreLevel(rank) == 1) {
      source_score = CurrentStorageInfo()->CompactionScore(rank);
      break;
    }
  }
  dbfull()->TEST_UnlockMutex();
  ASSERT_GE(source_score, 0.0);
  ASSERT_LT(source_score, options.rac_option2_emerging_score_floor);

  PauseRACCompactionThread();
  SealTwoCandidateEpoch(aligned_seed, 950, baseline_seed, 1000);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(request.active);
  ASSERT_EQ(baseline_seed->fd.GetNumber(), request.seed_file_number);
  ASSERT_FALSE(request.dual_purpose_selected);

  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       DualPurposeScoreAtOrAboveOneRemainsTelemetryOnly) {
  Options options = Option2Options(/*window_size=*/2);
  options.rac_option2_dual_purpose_selection = true;
  options.rac_option2_read_benefit_slack = 0.90;
  options.rac_option2_emerging_score_floor = 0.0;
  options.compaction_pri = kMinOverlappingRatio;
  options.ttl = 0;
  Reopen(options);
  MakeFileAtLevel(3, 0, 9, "l3-");
  MakeFileAtLevel(2, 0, 9, "l2-");
  MakeFileAtLevel(1, 0, 9, std::string(2048, 'x'));

  auto low_blocker = std::make_shared<test::SleepingBackgroundTask>();
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, low_blocker.get(),
                 Env::LOW);
  low_blocker->WaitUntilSleeping();
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"max_bytes_for_level_base", "1024"},
                             {"max_bytes_for_level_multiplier", "10"}}));
  FileMetaData* l1 = OnlyFileAtLevel(1);
  FileMetaData* l2 = OnlyFileAtLevel(2);
  ASSERT_NE(l1, nullptr);
  ASSERT_NE(l2, nullptr);
  double l1_score = -1.0;
  double l2_score = -1.0;
  dbfull()->TEST_LockMutex();
  for (int rank = 0; rank <= CurrentStorageInfo()->MaxInputLevel(); ++rank) {
    if (CurrentStorageInfo()->CompactionScoreLevel(rank) == 1) {
      l1_score = CurrentStorageInfo()->CompactionScore(rank);
    } else if (CurrentStorageInfo()->CompactionScoreLevel(rank) == 2) {
      l2_score = CurrentStorageInfo()->CompactionScore(rank);
    }
  }
  dbfull()->TEST_UnlockMutex();
  ASSERT_GE(l1_score, 1.0);
  ASSERT_GE(l2_score, 0.0);
  ASSERT_LT(l2_score, 1.0);

  PauseRACCompactionThread();
  std::unique_ptr<RACOption2LookupRecord> first;
  std::unique_ptr<RACOption2LookupRecord> second;
  BeginTrackedLookup(DefaultCfd(), &first);
  BeginTrackedLookup(DefaultCfd(), &second);
  first->RecordFileMiss(l1->fd.GetNumber(), l1->fd.GetFileSize(), 1, 950);
  second->RecordFileMiss(l2->fd.GetNumber(), l2->fd.GetFileSize(), 2, 1000);
  ASSERT_FALSE(FinishTrackedLookup(first.get()));
  ASSERT_TRUE(FinishTrackedLookup(second.get()));
  dbfull()->TEST_ForceRACOption2Evaluation();

  dbfull()->TEST_LockMutex();
  const RACOption2CompactionRequest request =
      DefaultCfd()->GetRACOption2Request();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(request.active);
  ASSERT_EQ(l2->fd.GetNumber(), request.baseline_seed_file_number);
  ASSERT_EQ(l2->fd.GetNumber(), request.seed_file_number);
  ASSERT_FALSE(request.dual_purpose_selected);

  ResumeRACCompactionThread();
  low_blocker->WakeUp();
  low_blocker->WaitUntilDone();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       SuccessfulTerminalBottomInstallCreatesCooldown) {
  auto listener = std::make_shared<RACCompactionEventListener>();
  Options options = Option2Options(/*window_size=*/1, /*cooldown_windows=*/1);
  options.listeners.emplace_back(listener);
  Reopen(options);
  MakeFileAtLevel(3, 0, 9, "bottom-");
  MakeFileAtLevel(2, 0, 9, "source-");
  const int ordinary_before = listener->ordinary_compactions();
  FileMetaData* source = OnlyFileAtLevel(2);
  ASSERT_NE(source, nullptr);

  SealMissEpoch(DefaultCfd(), source, /*level=*/2);
  dbfull()->TEST_ForceRACOption2Evaluation();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(CooldownCount(DefaultCfd()), 1U);
  ASSERT_EQ(listener->rac_compactions(), 1);
  ASSERT_EQ(listener->ordinary_compactions(), ordinary_before);
}

TEST_F(RACCompactionAdversarialTest,
       CurrentDeepestLevelInstallCreatesCooldownBeforeConfiguredLastLevel) {
  auto listener = std::make_shared<RACCompactionEventListener>();
  Options options = Option2Options(/*window_size=*/1,
                                   /*cooldown_windows=*/100);
  options.num_levels = 7;
  options.listeners.emplace_back(listener);
  Reopen(options);
  MakeFileAtLevel(4, 0, 9, "current-bottom-");
  MakeFileAtLevel(3, 0, 9, "first-source-");
  FileMetaData* first_source = OnlyFileAtLevel(3);
  ASSERT_NE(first_source, nullptr);

  SealMissEpoch(DefaultCfd(), first_source, /*level=*/3);
  dbfull()->TEST_ForceRACOption2Evaluation();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(CooldownCount(DefaultCfd()), 1U);
  ASSERT_EQ(listener->rac_compactions(), 1);

  MakeFileAtLevel(3, 0, 9, "second-source-");
  FileMetaData* second_source = OnlyFileAtLevel(3);
  ASSERT_NE(second_source, nullptr);
  const uint64_t cooldown_watermark =
      DefaultCfd()->GetLatestCompletedRACOption2Epoch();
  ASSERT_TRUE(InCooldown(DefaultCfd(), second_source->smallest,
                         second_source->largest, cooldown_watermark));

  PauseRACCompactionThread();
  SealMissEpoch(DefaultCfd(), second_source, /*level=*/3);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  ASSERT_FALSE(DefaultCfd()->GetRACOption2Request().active);
  dbfull()->TEST_UnlockMutex();
  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(listener->rac_compactions(), 1);
}

TEST_F(RACCompactionAdversarialTest,
       OrdinaryCompactionListenerIsNotMarkedAsRACOption2) {
  auto listener = std::make_shared<RACCompactionEventListener>();
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.enable_read_triggered_compaction = false;
  options.disable_auto_compactions = true;
  options.listeners.emplace_back(listener);
  Reopen(options);

  PutRange(0, 9, "ordinary-");
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_EQ(listener->rac_compactions(), 0);
  ASSERT_GE(listener->ordinary_compactions(), 1);
}

TEST_F(RACCompactionAdversarialTest,
       SuccessfulTerminalTrivialMoveClearsMarkBeforeScheduling) {
  ReopenOption2(/*window_size=*/1, /*cooldown_windows=*/1);
  // Keep the configured terminal level non-empty while leaving this source
  // range disjoint, so the RAC compaction is a genuine L2->L3 trivial move.
  MakeFileAtLevel(3, 100, 109, "bottom-disjoint-");
  MakeFileAtLevel(2, 0, 9, "source-");
  FileMetaData* source = OnlyFileAtLevel(2);
  ASSERT_NE(source, nullptr);
  std::atomic<bool> callback_seen{false};
  std::atomic<bool> output_unmarked{false};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::InstallSuperVersionAndScheduleWork:RACAfterInstallCallback",
      [&](void*) {
        callback_seen.store(true, std::memory_order_release);
        const auto& outputs = CurrentStorageInfo()->LevelFiles(3);
        output_unmarked.store(
            outputs.size() == 2 &&
                std::all_of(outputs.begin(), outputs.end(), [](const auto* f) {
                  return !f->marked_for_compaction;
                }),
            std::memory_order_release);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  SealMissEpoch(DefaultCfd(), source, /*level=*/2);
  dbfull()->TEST_ForceRACOption2Evaluation();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_TRUE(callback_seen.load(std::memory_order_acquire));
  ASSERT_TRUE(output_unmarked.load(std::memory_order_acquire));
  ASSERT_EQ(CooldownCount(DefaultCfd()), 1U);
}

TEST_F(RACCompactionAdversarialTest,
       Option2CompactionRunsWhileLowPriPoolIsBlocked) {
  ReopenOption2(/*window_size=*/1);
  MakeFileAtLevel(3, 100, 109, "deepest-anchor-");
  MakeFileAtLevel(2, 0, 9, "rac-seed-");
  FileMetaData* seed = OnlyFileAtLevel(2);
  ASSERT_NE(seed, nullptr);

  auto low_blocker = std::make_shared<test::SleepingBackgroundTask>();
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, low_blocker.get(),
                 Env::LOW);
  low_blocker->WaitUntilSleeping();

  std::atomic<bool> bottom_worker_seen{false};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BGWorkBottomCompaction",
      [&](void*) { bottom_worker_seen.store(true, std::memory_order_release); });
  SyncPoint::GetInstance()->EnableProcessing();

  SealMissEpoch(DefaultCfd(), seed, /*level=*/2);
  dbfull()->TEST_ForceRACOption2Evaluation();
  const Status wait_status = dbfull()->TEST_WaitForCompact();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  low_blocker->WakeUp();
  low_blocker->WaitUntilDone();

  ASSERT_OK(wait_status);
  ASSERT_TRUE(bottom_worker_seen.load(std::memory_order_acquire));
  dbfull()->TEST_LockMutex();
  ASSERT_FALSE(DefaultCfd()->GetRACOption2Request().active);
  dbfull()->TEST_UnlockMutex();
}

TEST_F(RACCompactionAdversarialTest,
       Option2UsesSharedLowPriPoolWhenBottomPoolIsDisabled) {
  env_->SetBackgroundThreads(0, Env::Priority::BOTTOM);
  ReopenOption2(/*window_size=*/1);
  MakeFileAtLevel(3, 100, 109, "deepest-anchor-");
  MakeFileAtLevel(2, 0, 9, "rac-seed-");
  FileMetaData* seed = OnlyFileAtLevel(2);
  ASSERT_NE(seed, nullptr);

  PauseLowCompactionThread();
  std::atomic<bool> low_worker_seen{false};
  std::atomic<bool> bottom_worker_seen{false};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BGWorkCompaction",
      [&](void*) { low_worker_seen.store(true, std::memory_order_release); });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BGWorkBottomCompaction",
      [&](void*) { bottom_worker_seen.store(true, std::memory_order_release); });
  SyncPoint::GetInstance()->EnableProcessing();

  SealMissEpoch(DefaultCfd(), seed, /*level=*/2);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  ASSERT_TRUE(DefaultCfd()->GetRACOption2Request().active);
  dbfull()->TEST_UnlockMutex();
  ASSERT_FALSE(low_worker_seen.load(std::memory_order_acquire));
  ASSERT_FALSE(bottom_worker_seen.load(std::memory_order_acquire));

  ResumeLowCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_TRUE(low_worker_seen.load(std::memory_order_acquire));
  ASSERT_FALSE(bottom_worker_seen.load(std::memory_order_acquire));
  dbfull()->TEST_LockMutex();
  ASSERT_FALSE(DefaultCfd()->GetRACOption2Request().active);
  dbfull()->TEST_UnlockMutex();
}

TEST_F(RACCompactionAdversarialTest,
       StaleSeedIsRejectedWhileUnrelatedMarkedCompactionProceeds) {
  ReopenOption2(/*window_size=*/1);
  MakeFileAtLevel(2, 100, 109, "unrelated-");
  PutRange(0, 9, "stale-seed-");
  ASSERT_OK(Flush());
  FileMetaData* seed = OnlyFileAtLevel(0);
  ASSERT_NE(seed, nullptr);

  PauseRACCompactionThread();
  SealMissEpoch(DefaultCfd(), seed, /*level=*/0);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  ASSERT_TRUE(DefaultCfd()->GetRACOption2Request().active);
  dbfull()->TEST_UnlockMutex();

  // Make the exact request stale, then give the already queued picker
  // unrelated marked work. The unrelated compaction must not keep the stale
  // evidence epoch pending.
  dbfull()->TEST_LockMutex();
  seed->marked_for_compaction = false;
  seed->marked_for_compaction_source = MarkedForCompactionSource::kUnknown;
  FileMetaData* unrelated =
      DefaultCfd()->current()->storage_info()->LevelFiles(2).front();
  unrelated->marked_for_compaction = true;
  unrelated->marked_for_compaction_source =
      MarkedForCompactionSource::kOther;
  DefaultCfd()->current()->storage_info()->ComputeCompactionScore(
      DefaultCfd()->ioptions(), DefaultCfd()->GetLatestMutableCFOptions(),
      DefaultCfd()->GetFullHistoryTsLow());
  dbfull()->TEST_UnlockMutex();

  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  dbfull()->TEST_LockMutex();
  ASSERT_FALSE(DefaultCfd()->GetRACOption2Request().active);
  dbfull()->TEST_UnlockMutex();
}

TEST_F(RACCompactionAdversarialTest,
       SuggestedRangeSupersedesRACMarkWithoutBeingErased) {
  ReopenOption2(/*window_size=*/1);
  MakeFileAtLevel(1, 100, 109, "level-anchor-");
  PutRange(0, 9, "manual-seed-");
  ASSERT_OK(Flush());
  FileMetaData* seed = OnlyFileAtLevel(0);
  ASSERT_NE(seed, nullptr);

  PauseRACCompactionThread();
  SealMissEpoch(DefaultCfd(), seed, /*level=*/0);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  ASSERT_TRUE(DefaultCfd()->GetRACOption2Request().active);
  dbfull()->TEST_UnlockMutex();

  ASSERT_OK(db_->SuggestCompactRange(db_->DefaultColumnFamily(), nullptr,
                                     nullptr));
  dbfull()->TEST_LockMutex();
  ASSERT_TRUE(seed->marked_for_compaction);
  ASSERT_EQ(seed->marked_for_compaction_source,
            MarkedForCompactionSource::kOther);
  dbfull()->TEST_UnlockMutex();

  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  dbfull()->TEST_LockMutex();
  ASSERT_FALSE(DefaultCfd()->GetRACOption2Request().active);
  dbfull()->TEST_UnlockMutex();
}

TEST_F(RACCompactionAdversarialTest,
       DuplicateExpandedCandidatesUseStableLowestSeedTieBreak) {
  ReopenOption2(/*window_size=*/2);
  MakeFileAtLevel(1, 0, 19, "target-");
  PutRange(0, 9, "source-a-");
  ASSERT_OK(Flush());
  PutRange(5, 14, "source-b-");
  ASSERT_OK(Flush());
  const auto& l0_files = CurrentStorageInfo()->LevelFiles(0);
  ASSERT_EQ(l0_files.size(), 2U);
  const uint64_t expected_seed =
      std::min(l0_files[0]->fd.GetNumber(), l0_files[1]->fd.GetNumber());

  PauseRACCompactionThread();
  std::unique_ptr<RACOption2LookupRecord> first;
  std::unique_ptr<RACOption2LookupRecord> second;
  BeginTrackedLookup(DefaultCfd(), &first);
  BeginTrackedLookup(DefaultCfd(), &second);
  first->RecordFileMiss(l0_files[0]->fd.GetNumber(),
                        l0_files[0]->fd.GetFileSize(), 0, 1000);
  second->RecordFileMiss(l0_files[1]->fd.GetNumber(),
                         l0_files[1]->fd.GetFileSize(), 0, 1000);
  ASSERT_FALSE(FinishTrackedLookup(first.get()));
  ASSERT_TRUE(FinishTrackedLookup(second.get()));
  dbfull()->TEST_ForceRACOption2Evaluation();

  dbfull()->TEST_LockMutex();
  ASSERT_TRUE(DefaultCfd()->GetRACOption2Request().active);
  ASSERT_EQ(DefaultCfd()->GetRACOption2Request().seed_file_number,
            expected_seed);
  dbfull()->TEST_UnlockMutex();
  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       CandidateIncludesEveryOverlappingTargetFile) {
  ReopenOption2(/*window_size=*/1);
  MakeFileAtLevel(1, 0, 9, "target-a-");
  MakeFileAtLevel(1, 10, 19, "target-b-");
  PutRange(5, 14, "source-");
  ASSERT_OK(Flush());
  const auto& targets = CurrentStorageInfo()->LevelFiles(1);
  ASSERT_EQ(targets.size(), 2U);
  FileMetaData* source = OnlyFileAtLevel(0);
  ASSERT_NE(source, nullptr);

  std::vector<uint64_t> expected_inputs = {source->fd.GetNumber()};
  for (const auto* target : targets) {
    expected_inputs.push_back(target->fd.GetNumber());
  }
  std::sort(expected_inputs.begin(), expected_inputs.end());

  PauseRACCompactionThread();
  SealMissEpoch(DefaultCfd(), source, /*level=*/0);
  dbfull()->TEST_ForceRACOption2Evaluation();
  dbfull()->TEST_LockMutex();
  auto actual_inputs = DefaultCfd()->GetRACOption2Request().input_file_numbers;
  dbfull()->TEST_UnlockMutex();
  std::sort(actual_inputs.begin(), actual_inputs.end());
  ASSERT_EQ(actual_inputs, expected_inputs);
  ResumeRACCompactionThread();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(RACCompactionAdversarialTest,
       CancelledForSpaceRACCompactionCreatesNoCooldown) {
  std::shared_ptr<SstFileManager> manager(NewSstFileManager(env_));
  auto* sfm = static_cast<SstFileManagerImpl*>(manager.get());
  Options options = Option2Options(/*window_size=*/1);
  options.sst_file_manager = manager;
  options.statistics = CreateDBStatistics();
  Reopen(options);
  MakeFileAtLevel(3, 0, 9, "bottom-");
  MakeFileAtLevel(2, 0, 9, "source-");
  FileMetaData* source = OnlyFileAtLevel(2);
  ASSERT_NE(source, nullptr);
  sfm->SetMaxAllowedSpaceUsage(sfm->GetTotalSize() + 1);

  PauseRACCompactionThread();
  SealMissEpoch(DefaultCfd(), source, /*level=*/2);
  dbfull()->TEST_ForceRACOption2Evaluation();
  ResumeRACCompactionThread();
  dbfull()->TEST_WaitForCompact().PermitUncheckedError();

  dbfull()->TEST_LockMutex();
  ASSERT_FALSE(DefaultCfd()->GetRACOption2Request().active);
  dbfull()->TEST_UnlockMutex();
  ASSERT_EQ(CooldownCount(DefaultCfd()), 0U);
  ASSERT_EQ(sfm->GetCompactionsReservedSize(), 0U);
}

TEST_F(RACCompactionAdversarialTest,
       ConcurrentReadsFlushesAndBackgroundCompactionsPreserveAccounting) {
  Options options = Option2Options(/*window_size=*/8);
  options.level0_file_num_compaction_trigger = 2;
  Reopen(options);
  std::atomic<bool> writer_done{false};
  std::thread writer([&] {
    for (int batch = 0; batch < 3; ++batch) {
      for (int key = 0; key < 10; ++key) {
        ASSERT_OK(Put(Key(key), "v" + std::to_string(batch)));
      }
      ASSERT_OK(Flush());
    }
    writer_done.store(true, std::memory_order_release);
  });
  std::thread reader([&] {
    std::string value;
    while (!writer_done.load(std::memory_order_acquire)) {
      for (int key = 0; key < 10; ++key) {
        db_->Get(ReadOptions(), Key(key), &value).PermitUncheckedError();
      }
    }
  });
  writer.join();
  reader.join();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(DefaultCfd()->GetRACOption2CollectorStats().invariant_violations,
            0U);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
