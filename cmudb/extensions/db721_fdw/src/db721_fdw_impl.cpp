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

static int get_strategy(Oid type, Oid opno, Oid am) {
  Oid opclass;
  Oid opfamily;

  opclass = GetDefaultOpClass(type, am);

  if (!OidIsValid(opclass))
    return 0;

  opfamily = get_opclass_family(opclass);

  return get_op_opfamily_strategy(opno, opfamily);
}

void extract_filters(DB721PlanState *plan_state, List *scan_clauses) {
  plan_state->filters_ = NIL;
  plan_state->ret_filters_ = NIL;
  int num = bms_num_members(plan_state->attrs_used_);
  for (int i = 0; i < num; ++i) {
    plan_state->filters_ = lappend(plan_state->filters_, NIL);
  }
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

    plan_state->ret_filters_ = lappend(plan_state->ret_filters_, clause);

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
        if (IsA(left, Var))
          v = (Var *)left;
        else if (IsA(left, RelabelType))
          v = (Var *)((RelabelType *)left)->arg;
        else
          continue;
        c = (Const *)right;
        opno = expr->opno;
      } else if (IsA(left, Const)) {
        /* reverse order (CONST OP VAR) */
        if (IsA(right, Var))
          v = (Var *)right;
        else if (IsA(right, RelabelType))
          v = (Var *)((RelabelType *)right)->arg;
        else
          continue;
        c = (Const *)left;
        opno = get_commutator(expr->opno);
      } else
        continue;

      if ((strategy = get_strategy(c->consttype, opno, BTREE_AM_OID)) == 0)
        continue;
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

    plan_state->ret_filters_ = list_delete_last(plan_state->ret_filters_);

    Filter *f = (Filter *)palloc(sizeof(Filter));
    f->attnum = v->varattno;
    f->strategy = strategy;
    f->value = c;
    int i = bms_member_index(plan_state->attrs_used_,
                             f->attnum - FirstLowInvalidHeapAttributeNumber);
    Assert(i >= 0);
    f->Init(plan_state->table_->columns_[f->attnum - 1].type_);
    ListCell *elems = plan_state->filters_->elements;
    elems[i].ptr_value = lappend((List *)elems[i].ptr_value, f);
  }
}

static void extract_used_attributes(DB721PlanState *plan_state,
                                    RelOptInfo *baserel) {
  plan_state->attrs_used_ = nullptr;
  ListCell *lc;
  pull_varattnos((Node *)baserel->reltarget->exprs, baserel->relid,
                 &plan_state->attrs_used_);

  foreach (lc, baserel->baserestrictinfo) {
    RestrictInfo *rinfo = (RestrictInfo *)lfirst(lc);
    pull_varattnos((Node *)rinfo->clause, baserel->relid,
                   &plan_state->attrs_used_);
  }

  if (bms_is_empty(plan_state->attrs_used_)) {
    bms_free(plan_state->attrs_used_);
    plan_state->attrs_used_ =
        bms_make_singleton(1 - FirstLowInvalidHeapAttributeNumber);
  }
  extract_filters(plan_state, baserel->baserestrictinfo);
}

extern "C" void db721_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
                                        Oid foreigntableid) {
  DB721PlanState *fdw_private =
      (DB721PlanState *)palloc(sizeof(DB721PlanState));
  fdw_private->table_ = open_table(foreigntableid);
  extract_used_attributes(fdw_private, baserel);
  baserel->tuples = fdw_private->table_->TotalRows();
  baserel->rows = fdw_private->EstimateRows();
  baserel->fdw_private = fdw_private;
  // ereport(LOG, (errmsg("GetForeignRelSize total %f rows, at most %f match",
  //                      baserel->tuples, baserel->rows)));
}

extern "C" void db721_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
                                      Oid foreigntableid) {
  ForeignPath *path =
      create_foreignscan_path(root, baserel, NULL, baserel->rows, 100, 100, NIL,
                              baserel->lateral_relids, NULL, NIL);
  add_path(baserel, (Path *)path);
}

extern "C" ForeignScan *
db721_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
                     ForeignPath *best_path, List *tlist, List *scan_clauses,
                     Plan *outer_plan) {
  // scan_clauses = extract_actual_clauses(scan_clauses, false);
  DB721PlanState *plan_state = (DB721PlanState *)baserel->fdw_private;
  return make_foreignscan(tlist, plan_state->ret_filters_, baserel->relid, NIL,
                          (List *)plan_state, NIL, NIL, outer_plan);
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
      new DB721ExecState(exec_ctx, plan_state->table_, tupleDesc, plan_state);
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