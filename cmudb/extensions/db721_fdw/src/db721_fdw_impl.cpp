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

std::unordered_map<std::string, DB721Table> tables;

DB721Table *open_table(const char *name) {
  auto it = tables.find(name);
  if (it == tables.end()) {
    auto [it2, ok] = tables.emplace(name, name);
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
  DB721Table *table = open_table("farms");
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
  DB721ExecState *fdw_state = (DB721ExecState *)palloc(sizeof(DB721ExecState));
  fdw_state->Init(table);
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
  fdw_state->~DB721ExecState();
  pfree(fdw_state);
  node->fdw_state = nullptr;
}