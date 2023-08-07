// If you choose to use C++, read this very carefully:
// https://www.postgresql.org/docs/15/xfunc-c.html#EXTEND-CPP

// clang-format off
extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
}
// clang-format on

#include "reader.h"

std::unordered_map<Oid, DB721Table> tables;

DB721Table *open_table(Oid oid) {
  auto it = tables.find(oid);
  if (it == tables.end()) {
    auto [it2, ok] = tables.emplace(oid, oid);
    Assert(ok);
    it = it2;
  }
  if (!it->second.IsOpen()) {
    Assert(false);
  }
  return &it->second;
}

extern "C" void db721_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
                                        Oid foreigntableid) {
  DB721Table *table = open_table(foreigntableid);
  table->EstimateRows(baserel->rows, baserel->tuples);
  baserel->fdw_private = table;
}

extern "C" void db721_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
                                      Oid foreigntableid) {
  ForeignPath *path =
      create_foreignscan_path(root, baserel, NULL, 100, 100, 100, NIL,
                              baserel->lateral_relids, NULL, NIL);
  add_path(baserel, (Path *)path);
}

extern "C" ForeignScan *
db721_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
                     ForeignPath *best_path, List *tlist, List *scan_clauses,
                     Plan *outer_plan) {
  return make_foreignscan(tlist, scan_clauses, baserel->relid, NIL,
                          (List *)baserel->fdw_private, NIL, NIL, outer_plan);
}

extern "C" void db721_BeginForeignScan(ForeignScanState *node, int eflags) {
  ForeignScan *plan = (ForeignScan *)node->ss.ps.plan;
  DB721Table *table = (DB721Table *)plan->fdw_private;
  MemoryContext cxt = node->ss.ps.state->es_query_cxt;
  // exec_ctx holds all heap memory allocated by db721_fdw execution
  MemoryContext exec_ctx =
      AllocSetContextCreate(cxt, "db721 tuple data", ALLOCSET_DEFAULT_SIZES);
  DB721ExecState *fdw_state = new DB721ExecState(exec_ctx, table);
  node->fdw_state = fdw_state;
}

extern "C" TupleTableSlot *db721_IterateForeignScan(ForeignScanState *node) {
  DB721ExecState *fdw_state = (DB721ExecState *)node->fdw_state;
  TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
  ExecClearTuple(slot);
  bool ok = fdw_state->Next(slot);
  if (!ok) {
    return nullptr;
  }
  return slot;
}

extern "C" void db721_ReScanForeignScan(ForeignScanState *node) {
  DB721ExecState *fdw_state = (DB721ExecState *)node->fdw_state;
  fdw_state->ReScan();
}

extern "C" void db721_EndForeignScan(ForeignScanState *node) {
  DB721ExecState *fdw_state = (DB721ExecState *)node->fdw_state;
  MemoryContext ctx = fdw_state->mem_.ctx_;
  delete fdw_state;
  MemoryContextDelete(ctx);
}