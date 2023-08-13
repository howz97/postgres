#pragma once

extern "C" {
#include "postgres.h"

#include "commands/defrem.h"
#include "executor/tuptable.h"
#include "foreign/foreign.h"
#include "utils/lsyscache.h"
#include "utils/memdebug.h"
#include "utils/memutils.h"
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

#include <fstream>
#include <list>
#include <string>
#include <unordered_map>
#include <vector>

/*
 * Restriction
 */
struct Filter {
  AttrNumber attnum;
  int strategy;
  Const *value;
};

enum class DB721Type : uint8_t { Float, Int, String };
constexpr Oid pg_oid[] = {FLOAT4OID, INT4OID, TEXTOID};
constexpr uint8_t str_sz = 32;
constexpr uint8_t data_size[] = {4, 4, str_sz};

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
  Bitmapset *ApplyFilter(Bitmapset *bms, const Filter *filter);

  std::string name_;
  DB721Type type_;
  // start offset in this file
  uint32_t start_offset_;
  std::vector<DB721BlockStat> block_stat_;
};

class DB721Table {
public:
  DB721Table() = delete;
  DB721Table(Oid oid);
  bool Open(Oid oid);
  bool IsOpen() { return columns_.size(); };
  Cardinality TotalRows();

  std::string name_;
  // the maximum number of values in each block
  uint16_t max_val_block_;
  std::vector<DB721Column> columns_;
  std::ifstream ifs_;
  friend class DB721ExecState;
};

struct DB721PlanState {
  Cardinality EstimateRows(const std::list<Filter> &filters);
  DB721Table *table_;
  Bitmapset *attrs_used_;
  List *skip_blocks_;
};

class DB721Allocator {
public:
  DB721Allocator(MemoryContext ctx) : ctx_(ctx){};
  void *Alloc(Size size);
  void Free(void *pointer);
  MemoryContext ctx_;
};

class ExecStateColumn {
public:
  ExecStateColumn(DB721Allocator *mem, DB721Column *, Bitmapset *skip_blk);
  ~ExecStateColumn() { ClearBlock(); };
  uint32_t Next(std::ifstream &ifs, char *buffer, uint32_t step);
  DB721Data Current() { return mem_block_[blk_offset_]; };
  uint32_t CurRowID();
  void ClearBlock();
  // shared column definition
  DB721Column *c_;
  // current offset in this file
  uint32_t file_offset_;
  Bitmapset *skip_blk_;
  uint32_t rowid_ = 0;
  int32_t cur_blk_ = -1;
  std::vector<DB721Data> mem_block_;
  int32_t blk_offset_ = -1;
  DB721Allocator *mem_;
};

class DB721ExecState {
public:
  DB721ExecState(MemoryContext ctx, DB721Table *t, TupleDesc tpdesc,
                 Bitmapset *attrs_used, List *skip_blk);
  bool Next(TupleTableSlot *slot);
  void ReScan();

  DB721Allocator mem_;
  // shared table definition
  DB721Table *t_;
  TupleDesc tuple_desc_;
  char buffer_[str_sz + 1];
  std::vector<int8> map_;
  std::vector<ExecStateColumn> columns_;
};
