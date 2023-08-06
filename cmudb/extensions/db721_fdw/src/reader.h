#pragma once

// clang-format off
extern "C" {
#include "postgres.h"
#include "executor/tuptable.h"
#include "utils/memutils.h"
#include "utils/memdebug.h"
}
// TODO(WAN): Hack.
//  Because PostgreSQL tries to be portable, it makes a bunch of global
//  definitions that can make your C++ libraries very sad.
//  We're just going to undefine those.
#undef vsnprintf
#undef snprintf
#undef vsprintf
#undef sprintf
#undef vfprintf
#undef fprintf
#undef vprintf
#undef printf
#undef gettext
#undef dgettext
#undef ngettext
#undef dngettext
// clang-format on

#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>

constexpr uint8_t str_max_len = 32;

enum class DB721Type { Float, Int, String };

union DB721Data {
  float f;
  int i;
  char *s;
};

class DB721BlockStat {
public:
  // the number of values in this block
  uint16_t num_vals_;
  // the minimum value in this block
  DB721Data min_val_;
  // the maximum value in this block
  DB721Data max_val_;
  // only used for string column
  uint8_t min_str_len_;
  uint8_t max_str_len_;
};

class DB721Column {
public:
  ~DB721Column();

  std::string name_;
  DB721Type type_;
  // start offset in this file
  uint32_t start_offset_;
  std::vector<DB721BlockStat> block_stat_;
};

class DB721Table {
public:
  DB721Table() = delete;
  DB721Table(const char *name);
  bool Open(const char *name);
  bool IsOpen() { return columns_.size(); };
  void EstimateRows(Cardinality &match, Cardinality &total);

private:
  std::string name_;
  // the maximum number of values in each block
  uint16_t max_val_block_;
  std::vector<DB721Column> columns_;
  std::ifstream ifs_;
  friend class DB721ExecState;
};

class DB721Allocator {
public:
  void *Alloc(Size size);
  void Free(void *pointer);
  MemoryContext ctx_;
};

class ExecStateColumn {
public:
  ExecStateColumn(DB721Allocator *mem, DB721Column *c);
  ~ExecStateColumn() { ClearBlock(); };
  std::pair<DB721Data, bool> Next(std::ifstream &ifs, char *buffer);
  void ClearBlock();
  // shared column definition
  DB721Column *c_;
  // current offset in this file
  uint32_t cur_offset_;
  uint16_t next_blk_ = 0;
  uint16_t blk_offset_ = 0;
  std::vector<DB721Data> block_;
  DB721Allocator *mem_;
};

class DB721ExecState {
public:
  DB721ExecState(MemoryContext ctx, DB721Table *t);
  void Init(MemoryContext ctx, DB721Table *t);
  bool Next(TupleTableSlot *slot);
  void ReScan();

  DB721Allocator mem_;
  // shared table definition
  DB721Table *t_;
  char buffer[str_max_len + 1];
  std::vector<ExecStateColumn> columns_;
};
