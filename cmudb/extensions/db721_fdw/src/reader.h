#pragma once

extern "C" {
#include "postgres.h"

#include "access/nbtree.h"
#include "commands/defrem.h"
#include "executor/tuptable.h"
#include "foreign/foreign.h"
#include "utils/lsyscache.h"
#include "utils/memdebug.h"
#include "utils/memutils.h"
#include "utils/typcache.h"
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

enum class DB721Type : uint8_t { Float, Int, String };
constexpr Oid pg_oid[] = {FLOAT4OID, INT4OID, TEXTOID};
constexpr uint8_t str_sz = 32;
constexpr uint8_t data_size[] = {4, 4, str_sz};

/*
 * Restriction
 */
struct Filter {
  void Init(DB721Type typ) { // look up FmgrInfo
    Oid cmp_proc_oid;
    TypeCacheEntry *tce_1, *tce_2;
    tce_1 = lookup_type_cache(pg_oid[uint8_t(typ)], TYPECACHE_BTREE_OPFAMILY);
    tce_2 = lookup_type_cache(value->consttype, TYPECACHE_BTREE_OPFAMILY);
    cmp_proc_oid = get_opfamily_proc(tce_1->btree_opf, tce_1->btree_opintype,
                                     tce_2->btree_opintype, BTORDER_PROC);
    fmgr_info(cmp_proc_oid, &finfo);
  };
  bool Check(Datum val) {
    int cmpres =
        FunctionCall2Coll(&finfo, value->constcollid, val, value->constvalue);
    switch (strategy) {
    case BTLessStrategyNumber:
      return cmpres < 0;
    case BTLessEqualStrategyNumber:
      return cmpres <= 0;
    case BTGreaterStrategyNumber:
      return cmpres > 0;
    case BTGreaterEqualStrategyNumber:
      return cmpres >= 0;
    case BTEqualStrategyNumber:
      return cmpres == 0;
    case RTNotEqualStrategyNumber:
      return cmpres != 0;
    default:
      Assert(false);
    }
  };
  // attribute number, start from 1
  AttrNumber attnum;
  int strategy;
  Const *value;
  FmgrInfo finfo;
};

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
  Bitmapset *ApplyFilter(Bitmapset *bms, Filter *filter);

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
  uint32_t TotalRows();

  std::string name_;
  // the maximum number of values in each block
  uint16_t max_val_block_;
  std::vector<DB721Column> columns_;
  std::ifstream ifs_;
  friend class DB721ExecState;
};

struct DB721PlanState {
  uint32_t EstimateRows();
  DB721Table *table_;
  Bitmapset *attrs_used_;
  List *skip_blocks_;
  List *filters_;
  List *ret_filters_;
  List *estimate_;
  uint32_t estm_rows_;
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
  ExecStateColumn(DB721Allocator *mem, DB721Column *c, Bitmapset *skip_blk,
                  List *filters, int estimate, uint16_t blk_sz);
  // ~ExecStateColumn() { mem_->Free(block_begin_); };
  uint32_t Next(std::ifstream &ifs, uint32_t step);
  DB721Data Current() {
    DB721Data d;
    switch (c_->type_) {
    case DB721Type::Float:
      memcpy(&d.f, blk_cursor_, data_size[uint8_t(DB721Type::Float)]);
      break;
    case DB721Type::Int:
      memcpy(&d.i, blk_cursor_, data_size[uint8_t(DB721Type::Int)]);
      break;
    case DB721Type::String:
      d.s = blk_cursor_;
      break;
    default:
      Assert(false);
    }
    return d;
  };
  uint32_t CurRowID();
  void RewindBlock();
  inline uint32_t NumBefVal();
  // shared column definition
  DB721Column *c_;
  // current offset in this file
  uint32_t file_offset_;
  Bitmapset *skip_blk_;
  List *filters_;
  uint32_t estimate_;
  uint32_t rowid_ = 0;
  int32_t blk_no_ = -1;
  char *block_begin_;
  char *block_end_;
  char *blk_cursor_;
  // DB721Allocator *mem_;
};

class DB721ExecState {
public:
  DB721ExecState(MemoryContext ctx, DB721Table *t, TupleDesc tpdesc,
                 DB721PlanState *plan);
  ~DB721ExecState() { MemoryContextDelete(mem_.ctx_); };
  bool Next(TupleTableSlot *slot);
  void ReScan();

  // shared table definition
  DB721Table *t_;
  TupleDesc tuple_desc_;
  char buffer_[VARHDRSZ + str_sz];
  std::vector<int8> map_;
  std::vector<ExecStateColumn *> columns_p_;
  std::vector<ExecStateColumn> columns_;
  uint32_t estm_rows_;
  DB721Allocator mem_;
};
