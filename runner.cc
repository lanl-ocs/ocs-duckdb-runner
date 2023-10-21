/*
 * Copyright (c) 2021 Triad National Security, LLC, as operator of Los Alamos
 * National Laboratory with the U.S. Department of Energy/National Nuclear
 * Security Administration. All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * with the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. Neither the name of TRIAD, Los Alamos National Laboratory, LANL, the
 *    U.S. Government, nor the names of its contributors may be used to endorse
 *    or promote products derived from this software without specific prior
 *    written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#include "pthread-helper.h"
#include "time.h"

#include <duckdb.hpp>
#include <duckdb/common/virtual_file_system.hpp>

#include <stdio.h>

namespace ocs {

struct ReadStats {
  ReadStats() : read_ops(0), read_bytes(0) {}
  uint64_t read_ops;  // Number of read operations
  // Number of bytes read
  uint64_t read_bytes;
};

class MonitoredFileHandle : public duckdb::FileHandle {
 public:
  duckdb::unique_ptr<duckdb::FileHandle> base_;
  ReadStats* stats_;

  MonitoredFileHandle(duckdb::FileSystem& file_system, const std::string& path,
                      duckdb::unique_ptr<duckdb::FileHandle> base,
                      ReadStats* stats)
      : FileHandle(file_system, path), base_(std::move(base)), stats_(stats) {}

  ~MonitoredFileHandle() override { Close(); }

  void Close() override {
    if (base_) {
      base_->Close();
    }
  };
};

class MonitoredFileSystem : public duckdb::FileSystem {
 public:
  explicit MonitoredFileSystem(duckdb::unique_ptr<duckdb::FileSystem> base)
      : base_(std::move(base)) {}

  duckdb::unique_ptr<duckdb::FileHandle> OpenFile(
      const std::string& path, uint8_t flags, duckdb::FileLockType lock,
      duckdb::FileCompressionType compression,
      duckdb::FileOpener* opener) override {
    duckdb::unique_ptr<duckdb::FileHandle> r =
        base_->OpenFile(path, flags, lock, compression, opener);
    if (path.size() > 2 && path[0] == 's' && path[1] == '3') {
      ReadStats* const mystats = new ReadStats();
      stats_.push_back(mystats);
      return duckdb::make_uniq<MonitoredFileHandle>(*this, path, std::move(r),
                                                    mystats);
    } else {
      return duckdb::make_uniq<MonitoredFileHandle>(*this, path, std::move(r),
                                                    nullptr);
    }
  }

  bool DirectoryExists(const std::string& directory) override {
    return base_->DirectoryExists(directory);
  }

  bool FileExists(const std::string& filename) override {
    return base_->FileExists(filename);
  }

  int64_t GetFileSize(duckdb::FileHandle& handle) override {
    return base_->GetFileSize(*static_cast<MonitoredFileHandle&>(handle).base_);
  }

  void Read(duckdb::FileHandle& handle, void* buffer, int64_t nr_bytes,
            idx_t location) override {
    MonitoredFileHandle* h = &static_cast<MonitoredFileHandle&>(handle);
    base_->Read(*h->base_, buffer, nr_bytes, location);
    if (h->stats_) {
      h->stats_->read_bytes += nr_bytes;
      h->stats_->read_ops += 1;
    }
  }

  time_t GetLastModifiedTime(duckdb::FileHandle& handle) override {
    return base_->GetLastModifiedTime(handle);
  }

  duckdb::vector<std::string> Glob(const std::string& path,
                                   duckdb::FileOpener* opener) override {
    return base_->Glob(path, opener);
  }

  std::string GetName() const override { return "MonitoredFileSystem"; }

  void RegisterSubSystem(
      duckdb::unique_ptr<duckdb::FileSystem> sub_fs) override {
    base_->RegisterSubSystem(std::move(sub_fs));
  }

  void RegisterSubSystem(duckdb::FileCompressionType compression_type,
                         duckdb::unique_ptr<duckdb::FileSystem> fs) override {
    base_->RegisterSubSystem(compression_type, std::move(fs));
  }

  bool CanSeek() override { return true; }

  bool OnDiskFile(duckdb::FileHandle& handle) override {
    MonitoredFileHandle* h = &static_cast<MonitoredFileHandle&>(handle);
    return h->base_->OnDiskFile();
  }

  uint64_t GetTotalReadOps() const {
    uint64_t ops = 0;
    for (ReadStats* it : stats_) {
      ops += it->read_ops;
    }
    return ops;
  }

  uint64_t GetTotalReadBytes() const {
    uint64_t bytes = 0;
    for (ReadStats* it : stats_) {
      bytes += it->read_bytes;
    }
    return bytes;
  }

  ~MonitoredFileSystem() override {
    for (ReadStats* it : stats_) {
      delete it;
    }
  }

 private:
  std::vector<ReadStats*> stats_;
  duckdb::unique_ptr<duckdb::FileSystem> base_;
};

std::string ToSql(const std::string& source) {
  char tmp[500];
  snprintf(tmp, sizeof(tmp),
           "SELECT min(vertex_id) AS VID, min(x) as X, min(y) as Y, min(z) as "
           "Z, avg(e) AS E FROM %s WHERE x > 1.5 AND x < 1.6 AND y > 1.5 AND "
           "y < 1.6 AND z > 1.5 AND z < 1.6 GROUP BY vertex_id ORDER BY E;",
           source.c_str());
  return tmp;
}

int RunQuery(ReadStats* stats, const std::string& source, bool print = true) {
  int nrows = 0;
  duckdb::DBConfig conf;
  conf.file_system = duckdb::make_uniq<MonitoredFileSystem>(
      duckdb::make_uniq<duckdb::VirtualFileSystem>());
  conf.options.maximum_threads = 1;
  duckdb::DuckDB db(nullptr, &conf);
  {
    duckdb::Connection con(db);
    con.Query("SET s3_endpoint='127.0.0.1:9000'");
    con.Query("SET s3_region='us-east-1'");
    con.Query("SET s3_url_style='path'");
    con.Query("SET s3_use_ssl=false");
    std::unique_ptr<duckdb::QueryResult> r = con.SendQuery(ToSql(source));
    std::unique_ptr<duckdb::DataChunk> d = r->FetchRaw();
    while (d) {
      if (print) {
        d->Print();
      }
      nrows += int(d->size());
      d = r->FetchRaw();
    }
  }
  MonitoredFileSystem& fs =
      static_cast<MonitoredFileSystem&>(db.GetFileSystem());
  stats->read_bytes = fs.GetTotalReadBytes();
  stats->read_ops = fs.GetTotalReadOps();
  return nrows;
}

class QueryRunner {
 public:
  explicit QueryRunner(int max_jobs);
  ~QueryRunner();
  void AddTask(const std::string& input_source);
  const ReadStats& stats() const { return stats_; }
  int TotalRows() const { return nrows_; }
  void Wait();

 private:
  struct Task {
    QueryRunner* me;
    std::string input_file;
  };
  static void RunTask(void*);
  QueryRunner(const QueryRunner&);
  void operator=(const QueryRunner& other);
  ThreadPool* const pool_;
  // State below protected by cv_;
  ReadStats stats_;
  int nrows_;  // Total number of rows retrieved
  port::Mutex mu_;
  port::CondVar cv_;
  int bg_scheduled_;
  int bg_completed_;
};

QueryRunner::QueryRunner(int max_jobs)
    : pool_(new ThreadPool(max_jobs)),
      nrows_(0),
      cv_(&mu_),
      bg_scheduled_(0),
      bg_completed_(0) {}

void QueryRunner::Wait() {
  MutexLock ml(&mu_);
  while (bg_completed_ < bg_scheduled_) {
    cv_.Wait();
  }
}

void QueryRunner::AddTask(const std::string& input_file) {
  Task* const t = new Task;
  t->me = this;
  t->input_file = input_file;
  MutexLock ml(&mu_);
  bg_scheduled_++;
  pool_->Schedule(RunTask, t);
}

void QueryRunner::RunTask(void* arg) {
  Task* const t = static_cast<Task*>(arg);
  ReadStats stats;
  int n = 0;
  try {
    n = RunQuery(&stats, t->input_file);
  } catch (const std::exception& e) {
    fprintf(stderr, "Error running query: %s\n", e.what());
  }
  QueryRunner* const me = t->me;
  {
    MutexLock ml(&me->mu_);
    me->bg_completed_++;
    me->stats_.read_bytes += stats.read_bytes;
    me->stats_.read_ops += stats.read_ops;
    me->nrows_ += n;
    me->cv_.SignalAll();
  }
  delete t;
}

QueryRunner::~QueryRunner() {
  {
    MutexLock ml(&mu_);
    while (bg_completed_ < bg_scheduled_) {
      cv_.Wait();
    }
  }
  delete pool_;
}

}  // namespace ocs

void process_queries(int j) {
  ocs::QueryRunner runner(j);
  const uint64_t start = CurrentMicros();
  runner.AddTask("read_parquet('s3://ocs/xx_036785.parquet')");
  runner.Wait();
  const uint64_t end = CurrentMicros();
  fprintf(stderr, "Threads: %d\n", j);
  fprintf(stderr, "Query time: %.2f s\n", double(end - start) / 1000000);
  fprintf(stderr, "Total rows: %d\n", runner.TotalRows());
  fprintf(stderr, "Total read ops: %lld\n",
          static_cast<long long unsigned>(runner.stats().read_ops));
  fprintf(stderr, "Total read bytes: %lld\n",
          static_cast<long long unsigned>(runner.stats().read_bytes));
  fprintf(stderr, "Done\n");
}

int main() {
  process_queries(1);
  return 0;
}
