#include "reader.h"
#include <nlohmann/json.hpp>

extern "C" {
#include "access/nbtree.h"
#include "access/sysattr.h"
#include "utils/builtins.h"
}

Datum DB721GetDatum(DB721Type typ, DB721Data data, char *buffer) {
  switch (typ) {
  case DB721Type::Float:
    return Float4GetDatum(data.f);
  case DB721Type::Int:
    return Int32GetDatum(data.i);
  case DB721Type::String: {
    uint8_t len = strlen(data.s);
    SET_VARSIZE(buffer, len + VARHDRSZ);
    memcpy(VARDATA(buffer), data.s, len);
    return PointerGetDatum(buffer);
  }
  default:
    Assert(false);
  }
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

Bitmapset *DB721Column::ApplyFilter(Bitmapset *flt_out, Filter *filter) {
  FmgrInfo *finfo = &filter->finfo;
  int collid = filter->value->constcollid;
  Datum val = filter->value->constvalue;
  char buffer[VARHDRSZ + str_sz];
  for (uint32_t blk_i = 0; blk_i < block_stat_.size(); ++blk_i) {
    if (bms_is_member(blk_i, flt_out))
      continue;
    DB721BlockStat &blk_stat = block_stat_[blk_i];
    bool satisfies;
    switch (filter->strategy) {
    case BTLessStrategyNumber:
    case BTLessEqualStrategyNumber: {
      Datum lower = DB721GetDatum(type_, blk_stat.min_val_, buffer);
      satisfies = filter->Check(lower);
      break;
    }
    case BTGreaterStrategyNumber:
    case BTGreaterEqualStrategyNumber: {
      Datum upper = DB721GetDatum(type_, blk_stat.max_val_, buffer);
      satisfies = filter->Check(upper);
      break;
    }
    case BTEqualStrategyNumber: {
      Datum lower = DB721GetDatum(type_, blk_stat.min_val_, buffer);
      Datum upper = DB721GetDatum(type_, blk_stat.max_val_, buffer);
      int l = FunctionCall2Coll(finfo, collid, lower, val);
      int u = FunctionCall2Coll(finfo, collid, upper, val);
      satisfies = (l <= 0 || u >= 0);
      break;
    }
    case RTNotEqualStrategyNumber:
      satisfies = true;
      break;
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

uint32_t DB721Table::TotalRows() {
  uint32_t total = 0;
  for (DB721BlockStat &b : columns_[0].block_stat_)
    total += b.num_vals_;
  return total;
}

// Using bitmap can estimate more precisely, while cost more memory too.
uint32_t DB721PlanState::EstimateRows() {
  estm_rows_ = UINT_MAX;
  AttrNumber attnum = -1;
  skip_blocks_ = NIL;
  estimate_ = NIL;
  ListCell *lc;
  while ((attnum = bms_next_member(attrs_used_, attnum)) >= 0) {
    AttrNumber attn = attnum + FirstLowInvalidHeapAttributeNumber;
    auto &col = table_->columns_[attn - 1];
    Bitmapset *flt_out = NULL;
    foreach (lc, (List *)list_nth(filters_, list_length(skip_blocks_))) {
      Filter *f = (Filter *)lfirst(lc);
      flt_out = col.ApplyFilter(flt_out, f);
    }

    skip_blocks_ = lappend(skip_blocks_, flt_out);
    uint32_t m = 0;
    for (uint32_t bi = 0; bi < col.block_stat_.size(); ++bi) {
      if (!bms_is_member(bi, flt_out))
        m += col.block_stat_[bi].num_vals_;
    }
    estimate_ = lappend_int(estimate_, m);
    if (m < estm_rows_)
      estm_rows_ = m;
    /*
    ereport(LOG,
            (errmsg("attribute %d filterd %d of %ld blocks, at most %f match",
                    i, bms_num_members(flt_out), col.block_stat_.size(), m)));
    **/
  }
  return estm_rows_;
}

ExecStateColumn::ExecStateColumn(DB721Allocator *mem, DB721Column *c,
                                 Bitmapset *skip_blk, List *filters,
                                 int estimate, uint16_t blk_sz)
    : c_(c), skip_blk_(skip_blk), filters_(filters), estimate_(estimate)
/*,mem_(mem)*/ {
  file_offset_ = c_->start_offset_;
  uint32_t dsize = data_size[uint8_t(c_->type_)];
  block_begin_ = (char *)mem->Alloc(dsize * blk_sz);
  block_end_ = block_begin_;
  blk_cursor_ = block_begin_ - dsize;
}

void ExecStateColumn::RewindBlock() {
  blk_cursor_ = block_begin_ - data_size[uint8_t(c_->type_)];
}

uint32_t ExecStateColumn::NumBefVal() {
  return (blk_cursor_ - block_begin_) / data_size[uint8_t(c_->type_)];
}

uint32_t ExecStateColumn::Next(std::ifstream &ifs, uint32_t step) {
  uint8_t dsize = data_size[uint8_t(c_->type_)];
  blk_cursor_ += step * dsize;
  rowid_ += step;
  Assert(blk_cursor_ >= block_begin_);
  if (unlikely(blk_cursor_ >= block_end_)) {
    // clear current block
    blk_cursor_ -= (block_end_ - block_begin_);
  read_blk:
    blk_no_++;
    Assert(blk_no_ >= 0);
    if (uint32_t(blk_no_) >= c_->block_stat_.size())
      return 0;
    uint16_t num_val = c_->block_stat_[blk_no_].num_vals_;
    if (NumBefVal() >= num_val) {
      blk_cursor_ -= num_val * dsize;
      file_offset_ += num_val * dsize;
      goto read_blk;
    }
    if (bms_is_member(blk_no_, skip_blk_)) {
      file_offset_ += num_val * dsize;
      rowid_ += (num_val - NumBefVal());
      blk_cursor_ = block_begin_;
      goto read_blk;
    }
    /*
    ereport(LOG, (errmsg("attribute '%s' load block-%d offset
       %d(num_val=%d)", c_->name_.c_str(), cur_blk_, blk_offset_, num_val)));
    */
    if (NumBefVal()) {
      file_offset_ += blk_cursor_ - block_begin_;
      num_val -= NumBefVal();
      blk_cursor_ = block_begin_;
    }
    ifs.seekg(file_offset_);
    ifs.read(block_begin_, num_val * dsize);
    block_end_ = block_begin_ + (num_val * dsize);
    file_offset_ = ifs.tellg();
  }
  // Assert(rowid_ == CurRowID());
  return rowid_;
}

uint32_t ExecStateColumn::CurRowID() {
  if (unlikely(blk_no_ < 0))
    return 0;
  uint32_t prefix =
      c_->block_stat_[blk_no_].num_vals_ -
      ((block_end_ - block_begin_) / data_size[uint8_t(c_->type_)]);
  uint32_t rid = 1 + prefix + NumBefVal();
  for (auto i = 0; i < blk_no_; ++i) {
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
                               TupleDesc tpdesc, DB721PlanState *plan)
    : t_(t), tuple_desc_(tpdesc), estm_rows_(plan->estm_rows_), mem_(ctx) {
  if (!estm_rows_)
    return;
  map_.resize(tpdesc->natts);
  char field_name[255];
  char col_name[255];
  columns_.reserve(tpdesc->natts);
  for (int i = 0; i < tpdesc->natts; i++) {
    map_[i] = -1;
    /* Skip columns we don't intend to use in query */
    AttrNumber attnum = i + 1 - FirstLowInvalidHeapAttributeNumber;
    if (!bms_is_member(attnum, plan->attrs_used_))
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
    columns_.emplace_back(&mem_, col,
                          (Bitmapset *)list_nth(plan->skip_blocks_, j),
                          (List *)list_nth(plan->filters_, j),
                          list_nth_int(plan->estimate_, j), t->max_val_block_);
    map_[i] = j;
  }
  columns_p_.reserve(columns_.size());
  for (ExecStateColumn &c : columns_) {
    columns_p_.push_back(&c);
  }
  std::sort(columns_p_.begin(), columns_p_.end(),
            [](ExecStateColumn *r, ExecStateColumn *l) -> bool {
              return r->estimate_ < l->estimate_;
            });
  // for (ExecStateColumn *c : columns_p_) {
  //   ereport(LOG, (errmsg("column %s estimate match row %d",
  //                        c->c_->name_.c_str(), c->estimate_)));
  // }
}

bool DB721ExecState::Next(TupleTableSlot *slot) {
  if (unlikely(!estm_rows_))
    return false;
  uint32_t max_rid = columns_p_[0]->rowid_ + 1;
  ListCell *lc;
  for (uint16_t i = 0; i < uint16_t(columns_p_.size());) {
    ExecStateColumn *col = columns_p_[i];
    Assert(col->rowid_ <= max_rid);
    uint32_t rid = col->Next(t_->ifs_, max_rid - col->rowid_);
    if (unlikely(!rid))
      return false;
    Assert(rid >= max_rid);
    while (true) {
      bool pass = true;
      Datum d = DB721GetDatum(col->c_->type_, col->Current(), buffer_);
      foreach (lc, col->filters_) {
        Filter *f = (Filter *)lfirst(lc);
        if (!f->Check(d)) {
          rid = col->Next(t_->ifs_, 1);
          if (unlikely(!rid))
            return false;
          pass = false;
          break;
        }
      }
      if (pass)
        break;
    }
    Assert(rid >= max_rid);
    if (rid > max_rid) {
      max_rid = rid;
      if (i > 0) {
        i = 0;
        continue;
      }
    }
    i++;
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
    if (col.blk_no_ > 0) {
      col.file_offset_ = col.c_->start_offset_;
      col.blk_no_ = -1;
    }
    col.RewindBlock();
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
