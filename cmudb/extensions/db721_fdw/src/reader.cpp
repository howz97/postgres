#include "reader.h"
#include <nlohmann/json.hpp>

extern "C" {
#include "access/nbtree.h"
#include "access/sysattr.h"
#include "utils/builtins.h"
#include "utils/typcache.h"
}

Datum DB721GetDatum(DB721Type typ, DB721Data data) {
  switch (typ) {
  case DB721Type::Float:
    return Float4GetDatum(data.f);
  case DB721Type::Int:
    return Int32GetDatum(data.i);
  case DB721Type::String:
    return CStringGetTextDatum(data.s);
  default:
    Assert(false);
  }
}

DB721Column::~DB721Column() {
  if (type_ == DB721Type::String) {
    for (auto &stat : block_stat_) {
      free(stat.min_val_.s);
      free(stat.max_val_.s);
    }
  }
}

Bitmapset *DB721Column::ApplyFilter(Bitmapset *flt_out, const Filter *filter) {
  FmgrInfo finfo;
  int collid = filter->value->constcollid;
  int strategy = filter->strategy;
  Datum val = filter->value->constvalue;

  {
    // look up FmgrInfo
    Oid cmp_proc_oid;
    TypeCacheEntry *tce_1, *tce_2;
    tce_1 =
        lookup_type_cache(filter->value->consttype, TYPECACHE_BTREE_OPFAMILY);
    tce_2 = lookup_type_cache(pg_oid[uint8_t(type_)], TYPECACHE_BTREE_OPFAMILY);
    cmp_proc_oid = get_opfamily_proc(tce_1->btree_opf, tce_1->btree_opintype,
                                     tce_2->btree_opintype, BTORDER_PROC);
    fmgr_info(cmp_proc_oid, &finfo);
  }

  for (uint32_t blk_i = 0; blk_i < block_stat_.size(); ++blk_i) {
    if (bms_is_member(blk_i, flt_out))
      continue;
    DB721BlockStat &blk_stat = block_stat_[blk_i];
    bool satisfies;
    switch (strategy) {
    case BTLessStrategyNumber:
    case BTLessEqualStrategyNumber: {
      Datum lower = DB721GetDatum(type_, blk_stat.min_val_);
      int cmpres = FunctionCall2Coll(&finfo, collid, val, lower);
      satisfies = (strategy == BTLessStrategyNumber && cmpres > 0) ||
                  (strategy == BTLessEqualStrategyNumber && cmpres >= 0);
      break;
    }
    case BTGreaterStrategyNumber:
    case BTGreaterEqualStrategyNumber: {
      Datum upper = DB721GetDatum(type_, blk_stat.max_val_);
      int cmpres = FunctionCall2Coll(&finfo, collid, val, upper);
      satisfies = (strategy == BTGreaterStrategyNumber && cmpres < 0) ||
                  (strategy == BTGreaterEqualStrategyNumber && cmpres <= 0);
      break;
    }
    case BTEqualStrategyNumber: {
      Datum lower = DB721GetDatum(type_, blk_stat.min_val_);
      Datum upper = DB721GetDatum(type_, blk_stat.max_val_);
      int l = FunctionCall2Coll(&finfo, collid, val, lower);
      int u = FunctionCall2Coll(&finfo, collid, val, upper);
      satisfies = (l >= 0 || u <= 0);
      break;
    }
    default:
      Assert(false);
    }
    if (!satisfies)
      flt_out = bms_add_member(flt_out, blk_i);
  }
  return flt_out;
}

DB721Table::DB721Table(Oid oid) { Open(oid); }

bool DB721Table::Open(Oid oid) {
  // open file
  ForeignTable *table = GetForeignTable(oid);
  ListCell *lc;
  char *file_path = nullptr;
  foreach (lc, table->options) {
    DefElem *def = (DefElem *)lfirst(lc);
    if (strcmp(def->defname, "filename") == 0) {
      file_path = defGetString(def);
    } else if (strcmp(def->defname, "tablename") == 0) {
      name_ = defGetString(def);
    } else
      elog(ERROR, "unknown option '%s'", def->defname);
  }

  ifs_.open(file_path, std::ios::binary | std::ios::ate);
  if (!ifs_) {
    ifs_.exceptions();
    elog(ERROR, "db721_fdw failed to open file %s", file_path);
    return false;
  }

  // read size of metadata
  uint32_t fsize = ifs_.tellg();
  uint32_t meta_size = 0;
  assert(fsize > sizeof(meta_size));
  ifs_.seekg(fsize - sizeof(meta_size));
  ifs_.read(reinterpret_cast<char *>(&meta_size), sizeof(meta_size));
  if (!ifs_) {
    elog(ERROR, "db721_fdw file %s: faild to read size of metadata", file_path);
    return false;
  }

  // read metadata
  ifs_.seekg(fsize - sizeof(meta_size) - meta_size);
  std::string metadata;
  metadata.resize(meta_size);
  ifs_.read(metadata.data(), meta_size);
  if (!ifs_) {
    elog(ERROR, "db721_fdw file %s: faild to read metadata", file_path);
    return false;
  }
  // order of columns_ should match with table definition
  nlohmann::ordered_json meta_json = nlohmann::ordered_json ::parse(metadata);

  // parse metadata
  max_val_block_ = meta_json["Max Values Per Block"];
  columns_.reserve(meta_json["Columns"].size());
  for (auto &[cn, col_j] : meta_json["Columns"].items()) {
    DB721Column &col = columns_.emplace_back();
    col.name_ = cn;
    col.type_ = col_j["type"] == "float" ? DB721Type::Float
                : col_j["type"] == "int" ? DB721Type::Int
                                         : DB721Type::String;
    col.start_offset_ = col_j["start_offset"];
    col.block_stat_.resize(col_j["num_blocks"]);
    for (auto &[idx, stat_j] : col_j["block_stats"].items()) {
      DB721BlockStat &stat = col.block_stat_.at(std::stoul(idx));
      stat.num_vals_ = stat_j["num"];
      switch (col.type_) {
      case DB721Type::Float:
        stat.min_val_.f = float(stat_j["min"]);
        stat.max_val_.f = float(stat_j["max"]);
        break;
      case DB721Type::Int:
        stat.min_val_.i = int(stat_j["min"]);
        stat.max_val_.i = int(stat_j["max"]);
        break;
      case DB721Type::String:
        std::string s = stat_j["min"];
        stat.min_val_.s = (char *)malloc(s.size() + 1);
        strcpy(stat.min_val_.s, s.c_str());
        s = stat_j["max"];
        stat.max_val_.s = (char *)malloc(s.size() + 1);
        strcpy(stat.max_val_.s, s.c_str());
        stat.min_str_len_ = stat_j["min_len"];
        stat.max_str_len_ = stat_j["max_len"];
        break;
      }
    }
  }
  return true;
}

Cardinality DB721Table::TotalRows() {
  Cardinality total = 0;
  for (DB721BlockStat &b : columns_[0].block_stat_)
    total += b.num_vals_;
  return total;
}

// Using bitmap can estimate more precisely, while cost more memory too.
Cardinality DB721PlanState::EstimateRows(const std::list<Filter> &filters) {
  Cardinality est_match = INFINITY;
  AttrNumber attnum = -1;
  skip_blocks_ = NIL;
  while ((attnum = bms_next_member(attrs_used_, attnum)) >= 0) {
    AttrNumber i = attnum + FirstLowInvalidHeapAttributeNumber - 1;
    auto &col = table_->columns_[i];
    Bitmapset *flt_out = NULL;
    for (const Filter &f : filters) {
      if (f.attnum - 1 != i)
        continue;
      flt_out = col.ApplyFilter(flt_out, &f);
    }

    skip_blocks_ = lappend(skip_blocks_, flt_out);
    Cardinality m = 0;
    for (uint32_t bi = 0; bi < col.block_stat_.size(); ++bi) {
      if (!bms_is_member(bi, flt_out))
        m += col.block_stat_[bi].num_vals_;
    }
    if (m < est_match)
      est_match = m;
    ereport(LOG,
            (errmsg("attribute %d filterd %d of %ld blocks, at most %f match",
                    i, bms_num_members(flt_out), col.block_stat_.size(), m)));
  }
  return est_match;
}

ExecStateColumn::ExecStateColumn(DB721Allocator *mem, DB721Column *c,
                                 Bitmapset *skip_blk)
    : c_(c), skip_blk_(skip_blk), mem_(mem) {
  file_offset_ = c_->start_offset_;
}

void ExecStateColumn::ClearBlock() {
  if (c_->type_ == DB721Type::String) {
    for (auto d : mem_block_) {
      mem_->Free(d.s);
    }
  }
  mem_block_.resize(0);
}

uint32_t ExecStateColumn::Next(std::ifstream &ifs, char *buffer,
                               uint32_t step) {
  blk_offset_ += step;
  rowid_ += step;
  Assert(blk_offset_ >= 0);
  if (unlikely(uint32_t(blk_offset_) >= mem_block_.size())) {
    // clear current block
    blk_offset_ -= mem_block_.size();
    ClearBlock();
    uint8_t dsize = data_size[uint8_t(c_->type_)];
  read_blk:
    cur_blk_++;
    Assert(cur_blk_ >= 0);
    if (uint32_t(cur_blk_) >= c_->block_stat_.size())
      return 0;
    uint16_t num_val = c_->block_stat_[cur_blk_].num_vals_;
    if (blk_offset_ >= num_val) {
      blk_offset_ -= num_val;
      file_offset_ += num_val * dsize;
      goto read_blk;
    }
    if (bms_is_member(cur_blk_, skip_blk_)) {
      file_offset_ += num_val * dsize;
      rowid_ += (num_val - blk_offset_);
      blk_offset_ = 0;
      goto read_blk;
    }
    ereport(LOG, (errmsg("attribute '%s' load block-%d offset %d(num_val=%d)",
                         c_->name_.c_str(), cur_blk_, blk_offset_, num_val)));
    if (blk_offset_) {
      file_offset_ += blk_offset_ * dsize;
      num_val -= blk_offset_;
      blk_offset_ = 0;
    }
    ifs.seekg(file_offset_);
    for (uint16_t i = 0; i < num_val; ++i) {
      DB721Data &data = mem_block_.emplace_back();
      switch (c_->type_) {
      case DB721Type::Float:
        ifs.read(buffer, dsize);
        memcpy(&data.f, buffer, dsize);
        break;
      case DB721Type::Int:
        ifs.read(buffer, dsize);
        memcpy(&data.i, buffer, dsize);
        break;
      case DB721Type::String:
        ifs.read(buffer, dsize);
        uint8_t l = strlen(buffer);
        data.s = (char *)mem_->Alloc(l + 1);
        strcpy(data.s, buffer);
        break;
      }
    }
    file_offset_ = ifs.tellg();
  }
  Assert(rowid_ == CurRowID());
  return rowid_;
}

uint32_t ExecStateColumn::CurRowID() {
  if (unlikely(cur_blk_ < 0))
    return 0;
  uint16_t prefix = c_->block_stat_[cur_blk_].num_vals_ - mem_block_.size();
  uint32_t rid = 1 + prefix + blk_offset_;
  for (auto i = 0; i < cur_blk_; ++i) {
    rid += c_->block_stat_[i].num_vals_;
  }
  return rid;
}

char *tolowercase(const char *input, char *output) {
  Assert(strlen(input) < NAMEDATALEN - 1);
  int i = 0;
  do {
    output[i] = tolower(input[i]);
  } while (input[i++]);
  return output;
}

DB721ExecState::DB721ExecState(MemoryContext ctx, DB721Table *t,
                               TupleDesc tpdesc, Bitmapset *attrs_used,
                               List *skip_blk)
    : mem_(ctx), t_(t), tuple_desc_(tpdesc) {
  buffer_[str_sz] = 0;
  map_.resize(tpdesc->natts);
  char field_name[255];
  char col_name[255];
  for (int i = 0; i < tpdesc->natts; i++) {
    map_[i] = -1;
    /* Skip columns we don't intend to use in query */
    AttrNumber attnum = i + 1 - FirstLowInvalidHeapAttributeNumber;
    if (!bms_is_member(attnum, attrs_used))
      continue;
    const char *attname = NameStr(TupleDescAttr(tpdesc, i)->attname);
    tolowercase(attname, field_name);
    DB721Column *col = nullptr;
    for (DB721Column &c : t_->columns_) {
      tolowercase(c.name_.c_str(), col_name);
      if (strcmp(field_name, col_name) == 0) {
        col = &c;
        break;
      }
    }
    Assert(col);
    uint16_t j = columns_.size();
    columns_.emplace_back(&mem_, col, (Bitmapset *)list_nth(skip_blk, j));
    map_[i] = j;
  }
}

bool DB721ExecState::Next(TupleTableSlot *slot) {
  uint32_t max_rid = 0;
  for (ExecStateColumn &col : columns_) {
    uint32_t rid = col.Next(t_->ifs_, buffer_, 1);
    if (unlikely(!rid))
      return false;
    if (rid > max_rid)
      max_rid = rid;
  }
  for (int16_t i = 0; i < int16_t(columns_.size()); ++i) {
    ExecStateColumn &col = columns_[i];
    Assert(col.rowid_ <= max_rid);
    if (col.rowid_ == max_rid)
      continue;
    uint32_t rid = col.Next(t_->ifs_, buffer_, max_rid - col.rowid_);
    if (unlikely(!rid))
      return false;
    Assert(col.rowid_ >= max_rid);
    if (rid > max_rid) {
      max_rid = rid;
      i = -1;
    }
  }
  for (int attr = 0; attr < slot->tts_tupleDescriptor->natts; attr++) {
    if (map_[attr] < 0) {
      slot->tts_isnull[attr] = true;
      continue;
    }
    slot->tts_isnull[attr] = false;
    ExecStateColumn &col = columns_[map_[attr]];
    slot->tts_values[attr] = DB721GetDatum(col.c_->type_, col.Current());
  }
  ExecStoreVirtualTuple(slot);
  return true;
}

void DB721ExecState::ReScan() {
  for (ExecStateColumn &col : columns_) {
    if (col.cur_blk_ > 0) {
      col.ClearBlock();
      col.file_offset_ = col.c_->start_offset_;
      col.cur_blk_ = -1;
    }
    col.cur_blk_ = 0;
    col.blk_offset_ = -1;
    col.rowid_ = 0;
  }
}

void *DB721Allocator::Alloc(Size size) {
  MemoryContext oldctx = MemoryContextSwitchTo(ctx_);
  void *p = palloc(size);
  MemoryContextSwitchTo(oldctx);
  return p;
}

void DB721Allocator::Free(void *pointer) {
  MemoryContext oldctx = MemoryContextSwitchTo(ctx_);
  pfree(pointer);
  MemoryContextSwitchTo(oldctx);
}
