// If you choose to use C++, read this very carefully:
// https://www.postgresql.org/docs/15/xfunc-c.html#EXTEND-CPP

extern "C" {
#include "postgres.h"

#include "catalog/pg_am_d.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/jsonb.h"
}

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

static Bitmapset *extract_used_attributes(RelOptInfo *baserel) {
  Bitmapset *attrs_used = nullptr;
  ListCell *lc;
  pull_varattnos((Node *)baserel->reltarget->exprs, baserel->relid,
                 &attrs_used);

  foreach (lc, baserel->baserestrictinfo) {
    RestrictInfo *rinfo = (RestrictInfo *)lfirst(lc);
    pull_varattnos((Node *)rinfo->clause, baserel->relid, &attrs_used);
  }

  if (bms_is_empty(attrs_used)) {
    bms_free(attrs_used);
    attrs_used = bms_make_singleton(1 - FirstLowInvalidHeapAttributeNumber);
  }
  return attrs_used;
}

static int get_strategy(Oid type, Oid opno, Oid am) {
  Oid opclass;
  Oid opfamily;

  opclass = GetDefaultOpClass(type, am);

  if (!OidIsValid(opclass))
    return 0;

  opfamily = get_opclass_family(opclass);

  return get_op_opfamily_strategy(opno, opfamily);
}

// Build a list of expressions we can use to filter out row groups.
std::list<Filter> extract_filters(List *scan_clauses) {
  std::list<Filter> filters;
  ListCell *lc;
  foreach (lc, scan_clauses) {
    Expr *clause = (Expr *)lfirst(lc);
    OpExpr *expr;
    Expr *left, *right;
    int strategy;
    Const *c;
    Var *v;
    Oid opno;

    if (IsA(clause, RestrictInfo))
      clause = ((RestrictInfo *)clause)->clause;

    if (IsA(clause, OpExpr)) {
      expr = (OpExpr *)clause;

      /* Only interested in binary opexprs */
      if (list_length(expr->args) != 2)
        continue;

      left = (Expr *)linitial(expr->args);
      right = (Expr *)lsecond(expr->args);

      /*
       * Looking for expressions like "EXPR OP CONST" or "CONST OP EXPR"
       *
       * XXX Currently only Var as expression is supported. Will be
       * extended in future.
       */
      if (IsA(right, Const)) {
        if (!IsA(left, Var))
          continue;
        v = (Var *)left;
        c = (Const *)right;
        opno = expr->opno;
      } else if (IsA(left, Const)) {
        /* reverse order (CONST OP VAR) */
        if (!IsA(right, Var))
          continue;
        v = (Var *)right;
        c = (Const *)left;
        opno = get_commutator(expr->opno);
      } else
        continue;

      /* Not a btree family operator? */
      if ((strategy = get_strategy(v->vartype, opno, BTREE_AM_OID)) == 0) {
        /*
         * Maybe it's a gin family operator? (We only support
         * jsonb 'exists' operator at the moment)
         */
        if ((strategy = get_strategy(v->vartype, opno, GIN_AM_OID)) == 0 ||
            strategy != JsonbExistsStrategyNumber)
          continue;
      }
    } else if (IsA(clause, Var)) {
      /*
       * Trivial expression containing only a single boolean Var. This
       * also covers cases "BOOL_VAR = true"
       */
      v = (Var *)clause;
      strategy = BTEqualStrategyNumber;
      c = (Const *)makeBoolConst(true, false);
    } else if (IsA(clause, BoolExpr)) {
      /*
       * Similar to previous case but for expressions like "!BOOL_VAR" or
       * "BOOL_VAR = false"
       */
      BoolExpr *boolExpr = (BoolExpr *)clause;

      if (boolExpr->args && list_length(boolExpr->args) != 1)
        continue;

      if (!IsA(linitial(boolExpr->args), Var))
        continue;

      v = (Var *)linitial(boolExpr->args);
      strategy = BTEqualStrategyNumber;
      c = (Const *)makeBoolConst(false, false);
    } else
      continue;

    Filter f{
        .attnum = v->varattno,
        .strategy = strategy,
        .value = c,
    };

    /* potentially inserting elements may throw exceptions */
    bool error = false;
    try {
      filters.push_back(f);
    } catch (std::exception &e) {
      error = true;
    }
    if (error)
      elog(ERROR, "extracting row filters failed");
  }
  return filters;
}

extern "C" void db721_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
                                        Oid foreigntableid) {
  DB721PlanState *fdw_private =
      (DB721PlanState *)palloc(sizeof(DB721PlanState));
  fdw_private->table_ = open_table(foreigntableid);
  fdw_private->attrs_used_ = extract_used_attributes(baserel);
  std::list<Filter> filters = extract_filters(baserel->baserestrictinfo);
  baserel->tuples = fdw_private->table_->TotalRows();
  baserel->rows = fdw_private->EstimateRows(filters);
  baserel->fdw_private = fdw_private;
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
  scan_clauses = extract_actual_clauses(scan_clauses, false);
  return make_foreignscan(tlist, scan_clauses, baserel->relid, NIL,
                          (List *)baserel->fdw_private, NIL, NIL, outer_plan);
}

extern "C" void db721_BeginForeignScan(ForeignScanState *node, int eflags) {
  ForeignScan *plan = (ForeignScan *)node->ss.ps.plan;
  DB721PlanState *plan_state = (DB721PlanState *)plan->fdw_private;
  MemoryContext cxt = node->ss.ps.state->es_query_cxt;
  TupleDesc tupleDesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
  // exec_ctx holds all heap memory allocated by db721_fdw execution
  MemoryContext exec_ctx =
      AllocSetContextCreate(cxt, "db721 tuple data", ALLOCSET_DEFAULT_SIZES);
  DB721ExecState *fdw_state =
      new DB721ExecState(exec_ctx, plan_state->table_, tupleDesc,
                         plan_state->attrs_used_, plan_state->skip_blocks_);
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