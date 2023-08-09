#include "reader.h"
#include <nlohmann/json.hpp>

#include "access/sysattr.h"

DB721Column::~DB721Column() {
  if (type_ == DB721Type::String) {
    for (auto &stat : block_stat_) {
      free(stat.min_val_.s);
      free(stat.max_val_.s);
    }
  }
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

void DB721Table::EstimateRows(Cardinality &match, Cardinality &total) {
  for (auto &stat : columns_[0].block_stat_) {
    total += stat.num_vals_;
  }
  // TODO: calculate match
  match = total;
}

ExecStateColumn::ExecStateColumn(DB721Allocator *mem, DB721Column *c)
    : c_(c), mem_(mem) {
  cur_offset_ = c_->start_offset_;
}

void ExecStateColumn::ClearBlock() {
  if (c_->type_ == DB721Type::String) {
    for (auto d : block_) {
      mem_->Free(d.s);
    }
  }
  block_.resize(0);
}

std::pair<DB721Data, bool> ExecStateColumn::Next(std::ifstream &ifs,
                                                 char *buffer) {
  if (unlikely(blk_offset_ >= block_.size())) {
    // clear current block
    if (likely(block_.size())) {
      ClearBlock();
      blk_offset_ = 0;
    }
    // read next block
    if (next_blk_ >= c_->block_stat_.size()) {
      return {DB721Data(), false};
    }
    uint16_t num_val = c_->block_stat_[next_blk_++].num_vals_;
    ifs.seekg(cur_offset_);
    for (uint16_t i = 0; i < num_val; ++i) {
      DB721Data &data = block_.emplace_back();
      switch (c_->type_) {
      case DB721Type::Float:
        ifs.read(buffer, 4);
        memcpy(&data.f, buffer, 4);
        break;
      case DB721Type::Int:
        ifs.read(buffer, 4);
        memcpy(&data.i, buffer, 4);
        break;
      case DB721Type::String:
        ifs.read(buffer, 32);
        uint8_t l = strlen(buffer);
        data.s = (char *)mem_->Alloc(l + 1);
        strcpy(data.s, buffer);
        break;
      }
    }
    cur_offset_ = ifs.tellg();
  }
  return {block_[blk_offset_++], true};
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
                               TupleDesc tpdesc, Bitmapset *attrs_used)
    : mem_(ctx), t_(t), tuple_desc_(tpdesc) {
  buffer_[str_max_len] = 0;
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
    columns_.emplace_back(&mem_, col);
    map_[i] = columns_.size() - 1;
  }
}

bool DB721ExecState::Next(TupleTableSlot *slot) {
  for (int attr = 0; attr < slot->tts_tupleDescriptor->natts; attr++) {
    if (map_[attr] < 0) {
      slot->tts_isnull[attr] = true;
      continue;
    }
    slot->tts_isnull[attr] = false;
    ExecStateColumn &col = columns_[map_[attr]];
    auto [data, ok] = col.Next(t_->ifs_, buffer_);
    if (unlikely(!ok)) {
      return false;
    }
    switch (col.c_->type_) {
    case DB721Type::Float:
      slot->tts_values[attr] = Float4GetDatum(data.f);
      break;
    case DB721Type::Int:
      slot->tts_values[attr] = Int32GetDatum(data.i);
      break;
    case DB721Type::String:
      uint8_t vallen = strlen(data.s);
      int64 bytea_len = vallen + VARHDRSZ;
      bytea *b = (bytea *)mem_.Alloc(bytea_len);
      SET_VARSIZE(b, bytea_len);
      memcpy(VARDATA(b), data.s, vallen);
      slot->tts_values[attr] = PointerGetDatum(b);
      break;
    }
  }
  ExecStoreVirtualTuple(slot);
  return true;
}

void DB721ExecState::ReScan() {
  for (ExecStateColumn &col : columns_) {
    // Only reset blk_offset_ if block[0] is in memory
    if (col.next_blk_ > 1) {
      col.cur_offset_ = col.c_->start_offset_;
      col.next_blk_ = 0;
      col.ClearBlock();
    }
    col.blk_offset_ = 0;
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
