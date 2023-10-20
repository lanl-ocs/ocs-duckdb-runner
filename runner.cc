#include <duckdb.hpp>
#include <duckdb/common/virtual_file_system.hpp>

#include <stdio.h>

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

  unsigned long long GetTotalReadOps() const {
    uint64_t ops = 0;
    for (ReadStats* it : stats_) {
      ops += it->read_ops;
    }
    return ops;
  }

  unsigned long long GetTotalReadBytes() const {
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

int main() {
  duckdb::DBConfig conf;
  conf.file_system = duckdb::make_uniq<MonitoredFileSystem>(
      duckdb::make_uniq<duckdb::VirtualFileSystem>());
  conf.options.autoinstall_known_extensions =
      conf.options.autoload_known_extensions = true;
  duckdb::DuckDB db(nullptr, &conf);
  {
    duckdb::Connection con(db);
    con.Query("SET s3_endpoint='127.0.0.1:9000'");
    con.Query("SET s3_region='us-east-1'");
    con.Query("SET s3_url_style='path'");
    con.Query("SET s3_use_ssl=false");
    std::unique_ptr<duckdb::QueryResult> r = con.SendQuery(
        "SELECT element_id FROM read_parquet('s3://ocs/xx_036785.parquet') "
        "LIMIT 10");
    std::unique_ptr<duckdb::DataChunk> d = r->FetchRaw();
    while (d) {
      d->Print();
      d = r->FetchRaw();
    }
  }
  MonitoredFileSystem& fs =
      static_cast<MonitoredFileSystem&>(db.GetFileSystem());
  printf("ops: %llu\n", fs.GetTotalReadOps());
  printf("bytes: %llu\n", fs.GetTotalReadBytes());
  return 0;
}
