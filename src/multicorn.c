/*
 * The Multicorn Foreign Data Wrapper allows you to fetch foreign data in
 * Python in your PostgreSQL server
 *
 * This software is released under the postgresql licence
 *
 * author: Kozea
 */
#include "multicorn.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/clauses.h"
#if PG_VERSION_NUM < 120000
#include "optimizer/var.h"
#endif
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "nodes/makefuncs.h"
#include "catalog/pg_type.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "parser/parsetree.h"
#include "fmgr.h"

#if PG_VERSION_NUM < 100000
#include "executor/spi.h"
#endif


PG_MODULE_MAGIC;


extern Datum multicorn_handler(PG_FUNCTION_ARGS);
extern Datum multicorn_validator(PG_FUNCTION_ARGS);


PG_FUNCTION_INFO_V1(multicorn_handler);
PG_FUNCTION_INFO_V1(multicorn_validator);


void		_PG_init(void);
void		_PG_fini(void);

/*
 * FDW functions declarations
 */

static void multicornGetForeignRelSize(PlannerInfo *root,
						   RelOptInfo *baserel,
						   Oid foreigntableid);
static void multicornGetForeignPaths(PlannerInfo *root,
						 RelOptInfo *baserel,
						 Oid foreigntableid);
static ForeignScan *multicornGetForeignPlan(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid,
						ForeignPath *best_path,
						List *tlist,
						List *scan_clauses
#if PG_VERSION_NUM >= 90500
						, Plan *outer_plan
#endif
		);
static void multicornExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void multicornBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *multicornIterateForeignScan(ForeignScanState *node);
static void multicornReScanForeignScan(ForeignScanState *node);
static void multicornEndForeignScan(ForeignScanState *node);

#if PG_VERSION_NUM >= 90300
static void multicornAddForeignUpdateTargets(Query *parsetree,
								 RangeTblEntry *target_rte,
								 Relation target_relation);

static List *multicornPlanForeignModify(PlannerInfo *root,
						   ModifyTable *plan,
						   Index resultRelation,
						   int subplan_index);
static void multicornBeginForeignModify(ModifyTableState *mtstate,
							ResultRelInfo *resultRelInfo,
							List *fdw_private,
							int subplan_index,
							int eflags);
static TupleTableSlot *multicornExecForeignInsert(EState *estate, ResultRelInfo *resultRelInfo,
						   TupleTableSlot *slot,
						   TupleTableSlot *planslot);
static TupleTableSlot *multicornExecForeignDelete(EState *estate, ResultRelInfo *resultRelInfo,
						   TupleTableSlot *slot, TupleTableSlot *planSlot);
static TupleTableSlot *multicornExecForeignUpdate(EState *estate, ResultRelInfo *resultRelInfo,
						   TupleTableSlot *slot, TupleTableSlot *planSlot);
static void multicornEndForeignModify(EState *estate, ResultRelInfo *resultRelInfo);

static void multicorn_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
						   SubTransactionId parentSubid, void *arg);
#endif

#if PG_VERSION_NUM >= 90500
static List *multicornImportForeignSchema(ImportForeignSchemaStmt * stmt,
							 Oid serverOid);
#endif

static void multicorn_xact_callback(XactEvent event, void *arg);

/*	Helpers functions */
void	   *serializePlanState(MulticornPlanState * planstate);
MulticornExecState *initializeExecState(void *internal_plan_state);

/* Hash table mapping oid to fdw instances */
HTAB	   *InstancesHash;

PGFunction multicorn_plpython_inline_handler = NULL;

/* We need to grab a copy of this right away
   so we can handle OOM errors */
PyObject   *tracebackModule = NULL;

void
multicorn_init()
{
	static bool inited = false;
#if PY_MAJOR_VERSION >= 3
	static char *plpython_module = "plpython3";
	static char *inline_function_name = "plpython3_inline_handler";
#else
	static char *plpython_module = "plpython2";
	static char *inline_function_name = "plpython_inline_handler";
#endif

	if (inited == true)
	{
		return;
	}

	inited = true;

	/* Try to load plpython and let it do the init. */
	PG_TRY();
	{
	multicorn_plpython_inline_handler  = load_external_function(plpython_module,
								    inline_function_name,
								    true, NULL);
	/* Do nothing, but let plpython init everything */
	multicorn_call_plpython("pass");
	}
	PG_CATCH();
	{
		ereport(INFO, (errmsg("%s", "Unable to find plpython."), errhint("Install plpython if you wish to use plpy functions from multicorn")));
		Py_Initialize();
	}
	PG_END_TRY();

	/* load traceback now so oom problems are not quite as bad. */
	tracebackModule = PyImport_ImportModule("traceback");	
	errorCheck();
}


void
multicorn_call_plpython(const char *python_script)
{
	InlineCodeBlock *codeblock = makeNode(InlineCodeBlock);
	multicorn_init();
	if (multicorn_plpython_inline_handler == NULL)
	{
		ereport(ERROR, (errmsg("%s", "No plpython_inline_handler avaiable"), errhint("%s", "Install plpython")));
	}

	/* We need a copy of the python script, so it's not
	 * const, and so plpython is free to free it or not. 
	 */
	codeblock->source_text = pstrdup(python_script);
	/* XXXXX FIXME, look this up at init time. */
	codeblock->langIsTrusted = false;
	codeblock->langOid = InvalidOid;
#if PG_VERSION_NUM >= 110000
	codeblock->atomic = true;
#endif

	/* Version 9.6 and earlier we need to
	 * do an SPI_push/pop so we can do an
	 * SPI_connect.
	 */
#if PG_VERSION_NUM < 100000
	{
		bool spi_pushed = SPI_push_conditional();
#endif

		DirectFunctionCall1(multicorn_plpython_inline_handler,
				    PointerGetDatum(codeblock));
#if PG_VERSION_NUM < 100000
		SPI_pop_conditional(spi_pushed);
	}
#endif
}

TrampolineData *multicorn_trampoline_data = NULL;

void
multicornCallTrampoline(TrampolineData *td)
{
	Assert(multicorn_trampoline_data == NULL);
	/* We could be really carefull with MemoryContexts,
	 * or we could be lazy and just switch back
	 * and forth.
	 */
	td->target_context = CurrentMemoryContext;
	multicorn_trampoline_data = td;
	multicorn_call_plpython("from multicorn.utils import trampoline; trampoline()");
}

/* Call an instance by oid
 * without any arguments or
 * return values.
 *
 * Useful to bypass 
 * the trampoline.
 * Could use it with arguments
 * so lon as we can convert them 
 * back and forth to strings easily.
 *
 * entry is used if available
 * and we are not going through
 * plpython.  Set it to NULL
 * if it's not readily
 * available and we will look
 * it up.
 *
 */
#if PY_MAJOR_VERSION >= 3 
#define PY_LONG_CHAR ' '
#else
#define PY_LONG_CHAR 'L'
#endif

static void
multicornCallInstanceByOid(Oid ftable_oid, CacheEntry *entry, char *method)
{	
	char *buff;
	size_t buff_len;
	PyObject *result=NULL;

	multicorn_init();
	if (multicorn_plpython_inline_handler == NULL)
	{
		/* call directly. */
		if (entry == NULL)
		{
			bool found = false;
			entry = hash_search(InstancesHash,
					    &ftable_oid,
					    HASH_FIND,
					    &found);
			if (!found || entry == NULL || entry->value == NULL)
			{
				ereport(ERROR, (errmsg("%s", "Multicorn Table OID not found")));
				
			}
		}
		result = PyObject_CallMethod(entry->value, method, "()");
		if (result != NULL)
		{
			Py_DECREF(result);
		}
		return;
	}

	/* Macros are the easiest way to make this constant and easy to change. */
#define PYTHON_TEMPLATE  "from multicorn.utils import getInstanceByOid as gio; gio(%u%c).%s()"
#define PYTHON_TEMPLATE_LEN sizeof(PYTHON_TEMPLATE)

	/* +14 allows for the oid and a little bit of extra. */
	buff_len = strlen(method) + PYTHON_TEMPLATE_LEN + 14;
	buff = (char *)alloca(buff_len); /* it's on the stack. no need to free.*/
	
	snprintf (buff, buff_len, PYTHON_TEMPLATE, ftable_oid, PY_LONG_CHAR, method);
#undef PYTHON_TEMPLATE
#undef PYTHON_TEMPLAET_LENGTH

	multicorn_call_plpython(buff);

	return;
}

/*
 * Process 1 integer argument
 * through to a python method.
 * We could build a generic one,
 " but we would have to
 * be able to turn everything
 * into a string, and the fallback
 * method would be difficult to code
 * without abusing the C calling
 * convention and risking
 * some portability issues.
 */
static void
multicornCallInstanceByOidInt(Oid ftable_oid, CacheEntry *entry, char *method,  int arg)
{
	char *buff;
	size_t buff_len;
	PyObject *result=NULL;
	
	multicorn_init();
	if (multicorn_plpython_inline_handler == NULL)
	{
		/* call directly. */
		if (entry == NULL)
		{
			bool found = false;
			entry = hash_search(InstancesHash,
					    &ftable_oid,
					    HASH_FIND,
					    &found);
			if (!found || entry == NULL || entry->value == NULL)
			{
				ereport(ERROR, (errmsg("%s", "Multicorn Table OID not found")));
				
			}
		}
		result = PyObject_CallMethod(entry->value, method, "(i)", arg);
		if (result != NULL)
		{
			Py_DecRef(result);
		}
		return;
	}

/* Macros are the easiest way to make this constant and easy to change. */
#define PYTHON_TEMPLATE  "from multicorn.utils import getInstanceByOid as gio; gio(%u%c).%s(%d)"
#define PYTHON_TEMPLATE_LEN sizeof(PYTHON_TEMPLATE)

	/* +28 allows for the oid, the integer and a little bit of extra. */
	buff_len = strlen(method) + PYTHON_TEMPLATE_LEN + 28;
	buff = (char *)alloca(buff_len); /* it's on the stack. no need to free.*/
	
	snprintf (buff, buff_len, PYTHON_TEMPLATE, ftable_oid, PY_LONG_CHAR, method, arg);
#undef PYTHON_TEMPLATE
#undef PYTHON_TEMPLAET_LENGTH

	multicorn_call_plpython(buff);

	return;
}

void
_PG_init()
{
	HASHCTL		ctl;
	MemoryContext oldctx = MemoryContextSwitchTo(CacheMemoryContext);

	/*
	 * Save multicorn init for later so we can
	 * just call plpython if it's available.
	 */
	
	RegisterXactCallback(multicorn_xact_callback, NULL);
#if PG_VERSION_NUM >= 90300
	RegisterSubXactCallback(multicorn_subxact_callback, NULL);
#endif
	/* Initialize the global oid -> python instances hash */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(CacheEntry);
	ctl.hash = oid_hash;
	ctl.hcxt = CacheMemoryContext;
	InstancesHash = hash_create("multicorn instances", 32,
								&ctl,
								HASH_ELEM | HASH_FUNCTION);
	MemoryContextSwitchTo(oldctx);
}

void
_PG_fini()
{
	if (tracebackModule != NULL)
	{
		Py_DECREF(tracebackModule);
	}
		
	Py_Finalize();
}


Datum
multicorn_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdw_routine = makeNode(FdwRoutine);

	/* Plan phase */
	fdw_routine->GetForeignRelSize = multicornGetForeignRelSize;
	fdw_routine->GetForeignPaths = multicornGetForeignPaths;
	fdw_routine->GetForeignPlan = multicornGetForeignPlan;
	fdw_routine->ExplainForeignScan = multicornExplainForeignScan;

	/* Scan phase */
	fdw_routine->BeginForeignScan = multicornBeginForeignScan;
	fdw_routine->IterateForeignScan = multicornIterateForeignScan;
	fdw_routine->ReScanForeignScan = multicornReScanForeignScan;
	fdw_routine->EndForeignScan = multicornEndForeignScan;

#if PG_VERSION_NUM >= 90300
	/* Code for 9.3 */
	fdw_routine->AddForeignUpdateTargets = multicornAddForeignUpdateTargets;
	/* Writable API */
	fdw_routine->PlanForeignModify = multicornPlanForeignModify;
	fdw_routine->BeginForeignModify = multicornBeginForeignModify;
	fdw_routine->ExecForeignInsert = multicornExecForeignInsert;
	fdw_routine->ExecForeignDelete = multicornExecForeignDelete;
	fdw_routine->ExecForeignUpdate = multicornExecForeignUpdate;
	fdw_routine->EndForeignModify = multicornEndForeignModify;
#endif

#if PG_VERSION_NUM >= 90500
	fdw_routine->ImportForeignSchema = multicornImportForeignSchema;
#endif

	PG_RETURN_POINTER(fdw_routine);
}

static Datum
multicorn_validator_real(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	char	   *className = NULL;
	ListCell   *cell;
	PyObject   *p_class;
	
	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));
	
	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (strcmp(def->defname, "wrapper") == 0)
		{
			/* Only at server creation can we set the wrapper,	*/
			/* for security issues. */
			if (catalog == ForeignTableRelationId)
			{
				ereport(ERROR, (errmsg("%s", "Cannot set the wrapper class on the table"),
								errhint("%s", "Set it on the server")));
			}
			else
			{
				className = (char *) defGetString(def);
			}
		}
	}
	if (catalog == ForeignServerRelationId)
	{
		if (className == NULL)
		{
			ereport(ERROR, (errmsg("%s", "The wrapper parameter is mandatory, specify a valid class name")));
		}
		/* Try to import the class. */
		p_class = getClassString(className);
		errorCheck();
		Py_DECREF(p_class);
	}
	PG_RETURN_VOID();
}

/* A wrapper that might use the trampoline */
Datum
multicorn_validator(PG_FUNCTION_ARGS)
{
	multicorn_init();
	if (multicorn_plpython_inline_handler != NULL) {
		TrampolineData td;
		td.func = (TrampolineFunc)multicorn_validator_real;
		td.return_data = NULL;
		td.args[0] = (void *)fcinfo;
		td.args[1] = NULL;
		td.args[2] = NULL;
		td.args[3] = NULL;
		td.args[4] = NULL;
		multicornCallTrampoline(&td);
		return (Datum)td.return_data;
	}
	return multicorn_validator_real(fcinfo);
}

/*
 * multicornGetForeignRelSize
 *		Obtain relation size estimates for a foreign table.
 *		This is done by calling the
 */
static void
multicornGetForeignRelSizeReal(PlannerInfo *root,
						   RelOptInfo *baserel,
						   Oid foreigntableid)
{
	MulticornPlanState *planstate = palloc0(sizeof(MulticornPlanState));
	ForeignTable *ftable = GetForeignTable(foreigntableid);
	ListCell   *lc;
	bool		needWholeRow = false;
	TupleDesc	desc;
	
	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));
	
	baserel->fdw_private = planstate;
	planstate->fdw_instance = getInstance(foreigntableid);
	planstate->foreigntableid = foreigntableid;
	/* Initialize the conversion info array */
	{
		Relation	rel = RelationIdGetRelation(ftable->relid);
		AttInMetadata *attinmeta;

		desc = RelationGetDescr(rel);
		attinmeta = TupleDescGetAttInMetadata(desc);
		planstate->numattrs = RelationGetNumberOfAttributes(rel);

		planstate->cinfos = palloc0(sizeof(ConversionInfo *) *
									planstate->numattrs);
		initConversioninfo(planstate->cinfos, attinmeta);
		needWholeRow = rel->trigdesc && rel->trigdesc->trig_insert_after_row;
		RelationClose(rel);
	}
	if (needWholeRow)
	{
		int			i;

		for (i = 0; i < desc->natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(desc, i);

			if (!att->attisdropped)
			{
				planstate->target_list = lappend(planstate->target_list, makeString(NameStr(att->attname)));
			}
		}
	}
	else
	{
		/* Pull "var" clauses to build an appropriate target list */
#if PG_VERSION_NUM >= 90600
		foreach(lc, extractColumns(baserel->reltarget->exprs, baserel->baserestrictinfo))
#else
		foreach(lc, extractColumns(baserel->reltargetlist, baserel->baserestrictinfo))
#endif
		{
			Var		   *var = (Var *) lfirst(lc);
			Value	   *colname;

			/*
			 * Store only a Value node containing the string name of the
			 * column.
			 */
			colname = colnameFromVar(var, root, planstate);
			if (colname != NULL && strVal(colname) != NULL)
			{
				planstate->target_list = lappend(planstate->target_list, colname);
			}
		}
	}
	/* Extract the restrictions from the plan. */
	foreach(lc, baserel->baserestrictinfo)
	{
		extractRestrictions(baserel->relids, ((RestrictInfo *) lfirst(lc))->clause,
							&planstate->qual_list);

	}
	/* Inject the "rows" and "width" attribute into the baserel */
#if PG_VERSION_NUM >= 90600
	getRelSize(planstate, root, &baserel->rows, &baserel->reltarget->width);
	planstate->width = baserel->reltarget->width;
#else
	getRelSize(planstate, root, &baserel->rows, &baserel->width);
#endif
}
/* Trampoline Version */
static void
multicornGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,  Oid foreigntableid)
{
	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));
	
	multicorn_init();
	if (multicorn_plpython_inline_handler != NULL) {
		TrampolineData td;
		td.func = (TrampolineFunc)multicornGetForeignRelSizeReal;
		td.return_data = NULL;
		td.args[0] = (void *)root;
		td.args[1] = (void *)baserel;
		td.args[2] = (void *)(unsigned long)foreigntableid;
		td.args[3] = NULL;
		td.args[4] = NULL;
		multicornCallTrampoline(&td);
		return;
	}
	multicornGetForeignRelSizeReal(root, baserel, foreigntableid);
	return;
}

/*
 * multicornGetForeignPaths
 *		Create possible access paths for a scan on the foreign table.
 *		This is done by calling the "get_path_keys method on the python side,
 *		and parsing its result to build parameterized paths according to the
 *		equivalence classes found in the plan.
 */
static void
multicornGetForeignPathsReal(PlannerInfo *root,
						 RelOptInfo *baserel,
						 Oid foreigntableid)
{
	List				*pathes; /* List of ForeignPath */
	MulticornPlanState	*planstate = baserel->fdw_private;
	ListCell		    *lc;

	/* These lists are used to handle sort pushdown */
	List				*apply_pathkeys = NULL;
	List				*deparsed_pathkeys = NULL;

	/* Extract a friendly version of the pathkeys. */
	List	   *possiblePaths = pathKeys(planstate);

	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));

        /* Try to find parameterized paths */
	pathes = findPaths(root, baserel, possiblePaths, planstate->startupCost,
			planstate, apply_pathkeys, deparsed_pathkeys);

	/* Add a simple default path */
	pathes = lappend(pathes, create_foreignscan_path(root, baserel,
#if PG_VERSION_NUM >= 90600
												 	  NULL,  /* default pathtarget */
#endif
			baserel->rows,
			planstate->startupCost,
#if PG_VERSION_NUM >= 90600
			baserel->rows * baserel->reltarget->width,
#else
			baserel->rows * baserel->width,
#endif
			NIL,		/* no pathkeys */
		    NULL,
#if PG_VERSION_NUM >= 90500
			NULL,
#endif
			NULL));

	/* Handle sort pushdown */
	if (root->query_pathkeys)
	{
		List		*deparsed = deparse_sortgroup(root, foreigntableid, baserel);

		if (deparsed)
		{
			/* Update the sort_*_pathkeys lists if needed */
			computeDeparsedSortGroup(deparsed, planstate, &apply_pathkeys,
					&deparsed_pathkeys);
		}
	}

	/* Add each ForeignPath previously found */
	foreach(lc, pathes)
	{
		ForeignPath *path = (ForeignPath *) lfirst(lc);

		/* Add the path without modification */
		add_path(baserel, (Path *) path);

		/* Add the path with sort pusdown if possible */
		if (apply_pathkeys && deparsed_pathkeys)
		{
			ForeignPath *newpath;

			newpath = create_foreignscan_path(root, baserel,
#if PG_VERSION_NUM >= 90600
												 	  NULL,  /* default pathtarget */
#endif
					path->path.rows,
					path->path.startup_cost, path->path.total_cost,
					apply_pathkeys, NULL,
#if PG_VERSION_NUM >= 90500
					NULL,
#endif
					(void *) deparsed_pathkeys);

			newpath->path.param_info = path->path.param_info;
			add_path(baserel, (Path *) newpath);
		}
	}
	errorCheck();
}
static void
multicornGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));
	multicorn_init();
	if (multicorn_plpython_inline_handler != NULL) {
		TrampolineData td;
		td.func = (TrampolineFunc)multicornGetForeignPathsReal;
		td.return_data = NULL;
		td.args[0] = (void *)root;
		td.args[1] = (void *)baserel;
		td.args[2] = (void *)(unsigned long)foreigntableid;
		td.args[3] = NULL;
		td.args[4] = NULL;
		multicornCallTrampoline(&td);
		return;
	}
	multicornGetForeignPathsReal(root, baserel, foreigntableid);
	return;
  
}

/*
 * multicornGetForeignPlan
 *		Create a ForeignScan plan node for scanning the foreign table
 */
/* XXXXX FIXME: This one would be a pain to wrap
 * and I don't see an execution path that hits python.
 * So long as that's the case, no need to wrap it.
 */
static ForeignScan *
multicornGetForeignPlan(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid,
						ForeignPath *best_path,
						List *tlist,
						List *scan_clauses
#if PG_VERSION_NUM >= 90500
						, Plan *outer_plan
#endif
		)
{
	Index		scan_relid = baserel->relid;
	MulticornPlanState *planstate = (MulticornPlanState *) baserel->fdw_private;
	ListCell   *lc;
	
	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));
	
#if PG_VERSION_NUM >= 90600
	best_path->path.pathtarget->width = planstate->width;
#endif
	scan_clauses = extract_actual_clauses(scan_clauses, false);
	/* Extract the quals coming from a parameterized path, if any */
	if (best_path->path.param_info)
	{

		foreach(lc, scan_clauses)
		{
			extractRestrictions(baserel->relids, (Expr *) lfirst(lc),
								&planstate->qual_list);
		}
	}
	planstate->pathkeys = (List *) best_path->fdw_private;
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							scan_clauses,		/* no expressions to evaluate */
							serializePlanState(planstate)
#if PG_VERSION_NUM >= 90500
							, NULL
							, NULL /* All quals are meant to be rechecked */
							, NULL
#endif
							);
}

/*
 * multicornExplainForeignScan
 *		Placeholder for additional "EXPLAIN" information.
 *		This should (at least) output the python class name, as well
 *		as information that was taken into account for the choice of a path.
 */
static void
multicornExplainForeignScanReal(ForeignScanState *node, ExplainState *es)
{
	PyObject *p_iterable = execute(node, es),
			 *p_item,
			 *p_str;
	
	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));
	
	Py_INCREF(p_iterable);
	while((p_item = PyIter_Next(p_iterable))){
		p_str = PyObject_Str(p_item);
		ExplainPropertyText("Multicorn", PyString_AsString(p_str), es);
		Py_DECREF(p_str);
	}
	Py_DECREF(p_iterable);
	errorCheck();
}

/*
 * Check if we should use trampoline
 */
void
multicornExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	multicorn_init();
	if (multicorn_plpython_inline_handler != NULL) {
		TrampolineData td;
		td.func = (TrampolineFunc)multicornExplainForeignScanReal;
		td.return_data = NULL;
		td.args[0] = (void *)node;
		td.args[1] = (void *)es;
		td.args[2] = NULL;
		td.args[3] = NULL;
		td.args[4] = NULL;
		multicornCallTrampoline(&td);
		return;
	}
	multicornExplainForeignScanReal(node, es);
	return;
}

/*
 *	multicornBeginForeignScan
 *		Initialize the foreign scan.
 *		This (primarily) involves :
 *			- retrieving cached info from the plan phase
 *			- initializing various buffers
 */
static void
multicornBeginForeignScanReal(ForeignScanState *node, int eflags)
{
	ForeignScan *fscan = (ForeignScan *) node->ss.ps.plan;
	MulticornExecState *execstate;
	TupleDesc	tupdesc = RelationGetDescr(node->ss.ss_currentRelation);
	ListCell   *lc;

	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));

	execstate = initializeExecState(fscan->fdw_private);
	execstate->ftable_oid = node->ss.ss_currentRelation->rd_id;
	execstate->values = palloc(sizeof(Datum) * tupdesc->natts);
	execstate->nulls = palloc(sizeof(bool) * tupdesc->natts);
	execstate->qual_list = NULL;
	foreach(lc, fscan->fdw_exprs)
	{
		extractRestrictions(bms_make_singleton(fscan->scan.scanrelid),
							((Expr *) lfirst(lc)),
							&execstate->qual_list);
	}
	initConversioninfo(execstate->cinfos, TupleDescGetAttInMetadata(tupdesc));
	node->fdw_state = execstate;
}

/*
 * Check if we should use trampoline
 */
static void
multicornBeginForeignScan(ForeignScanState *node, int eflags)
{
	multicorn_init();
	if (multicorn_plpython_inline_handler != NULL) {
		TrampolineData td;
		td.func = (TrampolineFunc)multicornBeginForeignScanReal;
		td.return_data = NULL;
		td.args[0] = (void *)node;
		td.args[1] = (void *)(unsigned long)eflags;
		td.args[2] = NULL;
		td.args[3] = NULL;
		td.args[4] = NULL;
		multicornCallTrampoline(&td);
		return;
	}
	multicornBeginForeignScanReal(node, eflags);
	return;
}

/*
 * multicornIterateForeignScan
 *		Retrieve next row from the result set, or clear tuple slot to indicate
 *		EOF.
 *
 *		This is done by iterating over the result from the "execute" python
 *		method.
 */
static TupleTableSlot *
multicornIterateForeignScanReal(ForeignScanState *node)
{
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	MulticornExecState *execstate = node->fdw_state;
	PyObject   *p_value;

	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));
	
	assert (execstate->ftable_oid == node->ss.ss_currentRelation->rd_id);
	
	if (execstate->p_iterator == NULL)
	{
		execute(node, NULL);
	}
	ExecClearTuple(slot);
	if (execstate->p_iterator == Py_None)
	{
		/* No iterator returned from get_iterator */
		Py_DECREF(execstate->p_iterator);
		return slot;
	}
	p_value = PyIter_Next(execstate->p_iterator);
	errorCheck();
	/* A none value results in an empty slot. */
	if (p_value == NULL || p_value == Py_None)
	{
		Py_XDECREF(p_value);
		return slot;
	}
	slot->tts_values = execstate->values;
	slot->tts_isnull = execstate->nulls;
	pythonResultToTuple(p_value, slot, execstate->cinfos, execstate->buffer);
	ExecStoreVirtualTuple(slot);
	Py_DECREF(p_value);

	return slot;
}

/*
 * Check if we should use trampoline
 */
static TupleTableSlot *
multicornIterateForeignScan(ForeignScanState *node)
{
	multicorn_init();
	if (multicorn_plpython_inline_handler != NULL) {
		TrampolineData td;
		td.func = (TrampolineFunc)multicornIterateForeignScanReal;
		td.return_data = NULL;
		td.args[0] = (void *)node;
		td.args[1] = NULL;
		td.args[2] = NULL;
		td.args[3] = NULL;
		td.args[4] = NULL;
		multicornCallTrampoline(&td);
		return (TupleTableSlot *)td.return_data;
	}
	return multicornIterateForeignScanReal(node);
}

/*
 * multicornReScanForeignScan
 *		Restart the scan
 */
/*
 * XXXXX FIXME: Should this be wrapped.
 * Py_DECREF could theoretically enter
 * python to execute destructors.
 * But that seems unlikely.
 */
static void
multicornReScanForeignScan(ForeignScanState *node)
{
	MulticornExecState *state = node->fdw_state;
	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));

	if (state->p_iterator)
	{
		Py_DECREF(state->p_iterator);
		state->p_iterator = NULL;
	}
}

/*
 *	multicornEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan.
 */
/* No need to wrap this one.
 * It's simple enough that we
 * can just call directly.
 */
static void
multicornEndForeignScan(ForeignScanState *node)
{
	MulticornExecState *state = node->fdw_state;
	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));
	
	multicornCallInstanceByOid(state->ftable_oid,
				   NULL,
				   "end_modify");

	errorCheck();
	Py_DECREF(state->fdw_instance);
	Py_XDECREF(state->p_iterator);
	state->p_iterator = NULL;


	/* Free this up so that
	 * Valgrind can find issues
	 * if we try to use it again 
	 * later. 
	 */
	pfree(state->values);
	pfree(state->nulls);
	pfree(state->cinfos);

	state->values = NULL;
	state->nulls = NULL;
	state->cinfos = NULL;
}



#if PG_VERSION_NUM >= 90300
/*
 * multicornAddForeigUpdateTargets
 *		Add resjunk columns needed for update/delete.
 */
static void
multicornAddForeignUpdateTargetsReal(Query *parsetree,
								 RangeTblEntry *target_rte,
								 Relation target_relation)
{
	Var		   *var = NULL;
	TargetEntry *tle,
			   *returningTle;
	PyObject   *instance = getInstance(target_relation->rd_id);
	const char *attrname = getRowIdColumn(instance);
	TupleDesc	desc = target_relation->rd_att;
	int			i;
	ListCell   *cell;

	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));
	
	foreach(cell, parsetree->returningList)
	{
		returningTle = lfirst(cell);
		tle = copyObject(returningTle);
		tle->resjunk = true;
		parsetree->targetList = lappend(parsetree->targetList, tle);
	}


	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);

		if (!att->attisdropped)
		{
			if (strcmp(NameStr(att->attname), attrname) == 0)
			{
				var = makeVar(parsetree->resultRelation,
							  att->attnum,
							  att->atttypid,
							  att->atttypmod,
							  att->attcollation,
							  0);
				break;
			}
		}
	}
	if (var == NULL)
	{
		ereport(ERROR, (errmsg("%s", "The rowid attribute does not exist")));
	}
	tle = makeTargetEntry((Expr *) var,
						  list_length(parsetree->targetList) + 1,
						  strdup(attrname),
						  true);
	parsetree->targetList = lappend(parsetree->targetList, tle);
	Py_DECREF(instance);
}

/* Use the trampoline */
static void
multicornAddForeignUpdateTargets(Query *parsetree,
				 RangeTblEntry *target_rte,
				 Relation target_relation)
{

	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));
	
	multicorn_init();
	if (multicorn_plpython_inline_handler != NULL) {
		TrampolineData td;
		td.func = (TrampolineFunc)multicornAddForeignUpdateTargetsReal;
		td.return_data = NULL;
		td.args[0] = (void *)parsetree;
		td.args[1] = (void *)target_rte;
		td.args[2] = (void *)target_relation;
		td.args[3] = NULL;
		td.args[4] = NULL;
		multicornCallTrampoline(&td);
		return;
	}
	multicornAddForeignUpdateTargetsReal(parsetree, target_rte, target_relation);
	return;
}

/*
 * multicornPlanForeignModify
 *		Plan a foreign write operation.
 *		This is done by checking the "supported operations" attribute
 *		on the python class.
 */
static List *
multicornPlanForeignModify(PlannerInfo *root,
						   ModifyTable *plan,
						   Index resultRelation,
						   int subplan_index)
{
	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));
	return NULL;
}


/*
 * multicornBeginForeignModify
 *		Initialize a foreign write operation.
 */
static void
multicornBeginForeignModifyReal(ModifyTableState *mtstate,
							ResultRelInfo *resultRelInfo,
							List *fdw_private,
							int subplan_index,
							int eflags)
{
	MulticornModifyState *modstate = palloc0(sizeof(MulticornModifyState));
	Relation	rel = resultRelInfo->ri_RelationDesc;
	TupleDesc	desc = RelationGetDescr(rel);
	PlanState  *ps = mtstate->mt_plans[subplan_index];
	Plan	   *subplan = ps->plan;
	MemoryContext oldcontext;
	int			i;

	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));

	modstate->cinfos = palloc0(sizeof(ConversionInfo *) *
							   desc->natts);
	modstate->buffer = makeStringInfo();
	modstate->ftable_oid = rel->rd_id;
	modstate->fdw_instance = getInstance(rel->rd_id);
	modstate->rowidAttrName = getRowIdColumn(modstate->fdw_instance);
	initConversioninfo(modstate->cinfos, TupleDescGetAttInMetadata(desc));
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	MemoryContextSwitchTo(oldcontext);
	if (ps->ps_ResultTupleSlot)
	{
		TupleDesc	resultTupleDesc = ps->ps_ResultTupleSlot->tts_tupleDescriptor;

		modstate->resultCinfos = palloc0(sizeof(ConversionInfo *) *
										 resultTupleDesc->natts);
		initConversioninfo(modstate->resultCinfos, TupleDescGetAttInMetadata(resultTupleDesc));
	}
	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);

		if (!att->attisdropped)
		{
			if (strcmp(NameStr(att->attname), modstate->rowidAttrName) == 0)
			{
				modstate->rowidCinfo = modstate->cinfos[i];
				break;
			}
		}
	}
	modstate->rowidAttno = ExecFindJunkAttributeInTlist(subplan->targetlist, modstate->rowidAttrName);
	resultRelInfo->ri_FdwState = modstate;
}

static void
multicornBeginForeignModify(ModifyTableState *mtstate,
			    ResultRelInfo *resultRelInfo,
			    List *fdw_private,
			    int subplan_index,
			    int eflags)
{
	
	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));

	multicorn_init();
	if (multicorn_plpython_inline_handler != NULL) {
		TrampolineData td;
		td.func = (TrampolineFunc)multicornBeginForeignModifyReal;
		td.return_data = NULL;
		td.args[0] = (void *)mtstate;
		td.args[1] = (void *)resultRelInfo;
		td.args[2] = (void *)fdw_private;
		td.args[3] = (void *)(unsigned long)subplan_index;
		td.args[4] = (void *)(unsigned long)eflags;
		multicornCallTrampoline(&td);
		return;
	}
	multicornBeginForeignModifyReal(mtstate, resultRelInfo, fdw_private, subplan_index,
					eflags);
	return;
}

/* The 3 mod functions are similiar enough to make a macro for
 * the trampoline function.
 */

#define MODTRAMPOLINE(funcname)						\
static TupleTableSlot *							\
funcname(EState *estate, ResultRelInfo *resultRelInfo,			\
	 TupleTableSlot *slot, TupleTableSlot *planSlot)		\
{									\
	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__))); \
	multicorn_init();						\
	if (multicorn_plpython_inline_handler != NULL)			\
	{								\
		TrampolineData td;					\
		td.func = (TrampolineFunc)funcname##Real;		\
		td.return_data = NULL;					\
		td.args[0] = (void *)estate;				\
		td.args[1] = (void *)resultRelInfo;			\
		td.args[2] = (void *)slot;				\
		td.args[3] = (void *)planSlot;				\
		td.args[4] = NULL;					\
		multicornCallTrampoline(&td);				\
		return (TupleTableSlot *)td.return_data;		\
	}								\
	return funcname##Real(estate, resultRelInfo,			\
			      slot, planSlot);				\
}


/*
 * multicornExecForeignInsert
 *		Execute a foreign insert operation
 *		This is done by calling the python "insert" method.
 */

/* The actual function */
static TupleTableSlot *
multicornExecForeignInsertReal(EState *estate, ResultRelInfo *resultRelInfo,
			       TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	MulticornModifyState *modstate = resultRelInfo->ri_FdwState;
	PyObject   *fdw_instance = modstate->fdw_instance;
	PyObject   *values = tupleTableSlotToPyObject(slot, modstate->cinfos);
	PyObject   *p_new_value = PyObject_CallMethod(fdw_instance, "insert", "(O)", values);

	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));
	
	errorCheck();
	if (p_new_value && p_new_value != Py_None)
	{
		ExecClearTuple(slot);
		pythonResultToTuple(p_new_value, slot, modstate->cinfos, modstate->buffer);
		ExecStoreVirtualTuple(slot);
	}
	Py_XDECREF(p_new_value);
	Py_DECREF(values);
	errorCheck();
	return slot;
}

MODTRAMPOLINE(multicornExecForeignInsert)

/*
 * multicornExecForeignDelete
 *		Execute a foreign delete operation
 *		This is done by calling the python "delete" method, with the opaque
 *		rowid that was supplied.
 */
static TupleTableSlot *
multicornExecForeignDeleteReal(EState *estate, ResultRelInfo *resultRelInfo,
						   TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	MulticornModifyState *modstate = resultRelInfo->ri_FdwState;
	PyObject   *fdw_instance = modstate->fdw_instance,
			   *p_row_id,
			   *p_new_value;
	bool		is_null;
	ConversionInfo *cinfo = modstate->rowidCinfo;
	Datum		value = ExecGetJunkAttribute(planSlot, modstate->rowidAttno, &is_null);

	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));

	p_row_id = datumToPython(value, cinfo->atttypoid, cinfo);
	p_new_value = PyObject_CallMethod(fdw_instance, "delete", "(O)", p_row_id);
	errorCheck();
	if (p_new_value == NULL || p_new_value == Py_None)
	{
		Py_XDECREF(p_new_value);
		p_new_value = tupleTableSlotToPyObject(planSlot, modstate->resultCinfos);
	}
	ExecClearTuple(slot);
	pythonResultToTuple(p_new_value, slot, modstate->cinfos, modstate->buffer);
	ExecStoreVirtualTuple(slot);
	Py_DECREF(p_new_value);
	Py_DECREF(p_row_id);
	errorCheck();
	return slot;
}
MODTRAMPOLINE(multicornExecForeignDelete)

/*
 * multicornExecForeignUpdate
 *		Execute a foreign update operation
 *		This is done by calling the python "update" method, with the opaque
 *		rowid that was supplied.
 */
static TupleTableSlot *
multicornExecForeignUpdateReal(EState *estate, ResultRelInfo *resultRelInfo,
						   TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	MulticornModifyState *modstate = resultRelInfo->ri_FdwState;
	PyObject   *fdw_instance = modstate->fdw_instance,
			   *p_row_id,
			   *p_new_value,
			   *p_value = tupleTableSlotToPyObject(slot, modstate->cinfos);
	bool		is_null;
	ConversionInfo *cinfo = modstate->rowidCinfo;
	Datum		value = ExecGetJunkAttribute(planSlot, modstate->rowidAttno, &is_null);

	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));

	p_row_id = datumToPython(value, cinfo->atttypoid, cinfo);
	p_new_value = PyObject_CallMethod(fdw_instance, "update", "(O,O)", p_row_id,
									  p_value);
	errorCheck();
	if (p_new_value != NULL && p_new_value != Py_None)
	{
		ExecClearTuple(slot);
		pythonResultToTuple(p_new_value, slot, modstate->cinfos, modstate->buffer);
		ExecStoreVirtualTuple(slot);
	}
	Py_XDECREF(p_new_value);
	Py_DECREF(p_row_id);
	errorCheck();
	return slot;
}

MODTRAMPOLINE(multicornExecForeignUpdate)

#undef MODTRAMPOLINE
/*
 * multicornEndForeignModify
 *		Clean internal state after a modify operation.
 */
static void
multicornEndForeignModify(EState *estate, ResultRelInfo *resultRelInfo)

{
	MulticornModifyState *modstate = resultRelInfo->ri_FdwState;
	multicornCallInstanceByOid(modstate->ftable_oid,
				   NULL,
				   "end_modify");
	errorCheck();
	Py_DECREF(modstate->fdw_instance);
}

/*
 * Callback used to propagate a subtransaction end.
 */
static void
multicorn_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
						   SubTransactionId parentSubid, void *arg)
{
	int			curlevel;
	HASH_SEQ_STATUS status;
	CacheEntry *entry;

	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));

        /* Nothing to do after commit or subtransaction start. */
	if (event == SUBXACT_EVENT_COMMIT_SUB || event == SUBXACT_EVENT_START_SUB)
		return;

	curlevel = GetCurrentTransactionNestLevel();

	hash_seq_init(&status, InstancesHash);

	while ((entry = (CacheEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->xact_depth < curlevel)
			continue;

		if (event == SUBXACT_EVENT_PRE_COMMIT_SUB)
		{

			multicornCallInstanceByOidInt(entry->hashkey,
						      entry,
						      "sub_commit",
						      curlevel);
		}
		else
		{
			multicornCallInstanceByOidInt(entry->hashkey,
						      entry,
						      "sub_rollback",
						      curlevel);
		}
		errorCheck();
		entry->xact_depth--;
	}
}
#endif

/*
 * Callback used to propagate pre-commit / commit / rollback.
 */
static void
multicorn_xact_callback(XactEvent event, void *arg)
{
	HASH_SEQ_STATUS status;
	CacheEntry *entry;

	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));

	hash_seq_init(&status, InstancesHash);
	while ((entry = (CacheEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->xact_depth == 0)
			continue;

		switch (event)
		{
#if PG_VERSION_NUM >= 90300
			case XACT_EVENT_PRE_COMMIT:
				multicornCallInstanceByOid(entry->hashkey,
							   entry,
							   "pre_commit");
				break;
#endif
			case XACT_EVENT_COMMIT:
				multicornCallInstanceByOid(entry->hashkey,
							   entry,
							   "commit");
				entry->xact_depth = 0;
				break;
			case XACT_EVENT_ABORT:
				/* XXXXX FIXME: An exception here is really bad.
				   The process will crash.  However, that may
				   be the best we can do.
				*/
				multicornCallInstanceByOid(entry->hashkey,
							   entry,
							   "rollback");
				entry->xact_depth = 0;
				break;
			default:
				break;
		}
		errorCheck();
	}
}

#if PG_VERSION_NUM >= 90500
static List *
multicornImportForeignSchemaReal(ImportForeignSchemaStmt * stmt,
							 Oid serverOid)
{
	List	   *cmds = NULL;
	List	   *options = NULL;
	UserMapping *mapping;
	ForeignServer *f_server;
	char	   *restrict_type = NULL;
	PyObject   *p_class = NULL;
	PyObject   *p_tables,
			   *p_srv_options,
			   *p_options,
			   *p_restrict_list,
			   *p_iter,
			   *p_item;
	ListCell   *lc;

	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));

	f_server = GetForeignServer(serverOid);
	foreach(lc, f_server->options)
	{
		DefElem    *option = (DefElem *) lfirst(lc);

		if (strcmp(option->defname, "wrapper") == 0)
		{
			p_class = getClassString(defGetString(option));
			errorCheck();
		}
		else
		{
			options = lappend(options, option);
		}
	}
	mapping = multicorn_GetUserMapping(GetUserId(), serverOid);
	if (mapping)
		options = list_concat(options, mapping->options);

	if (p_class == NULL)
	{
		/*
		 * This should never happen, since we validate the wrapper parameter
		 * at
		 */
		/* object creation time. */
		ereport(ERROR, (errmsg("%s", "The wrapper parameter is mandatory, specify a valid class name")));
	}
	switch (stmt->list_type)
	{
		case FDW_IMPORT_SCHEMA_LIMIT_TO:
			restrict_type = "limit";
			break;
		case FDW_IMPORT_SCHEMA_EXCEPT:
			restrict_type = "except";
			break;
		case FDW_IMPORT_SCHEMA_ALL:
			break;
	}
	p_srv_options = optionsListToPyDict(options);
	p_options = optionsListToPyDict(stmt->options);
	p_restrict_list = PyList_New(0);
	foreach(lc, stmt->table_list)
	{
		RangeVar   *rv = (RangeVar *) lfirst(lc);
		PyObject   *p_tablename = PyUnicode_Decode(
											rv->relname, strlen(rv->relname),
												   getPythonEncodingName(),
												   NULL);

		errorCheck();
		PyList_Append(p_restrict_list, p_tablename);
		Py_DECREF(p_tablename);
	}
	errorCheck();
	p_tables = PyObject_CallMethod(p_class, "import_schema", "(s, O, O, s, O)",
							   stmt->remote_schema, p_srv_options, p_options,
								   restrict_type, p_restrict_list);
	errorCheck();
	Py_DECREF(p_class);
	Py_DECREF(p_options);
	Py_DECREF(p_srv_options);
	Py_DECREF(p_restrict_list);
	errorCheck();
	p_iter = PyObject_GetIter(p_tables);
	while ((p_item = PyIter_Next(p_iter)))
	{
		PyObject   *p_string;
		char	   *value;

		p_string = PyObject_CallMethod(p_item, "to_statement", "(s,s)",
								   stmt->local_schema, f_server->servername);
		errorCheck();
		value = PyString_AsString(p_string);
		errorCheck();
		cmds = lappend(cmds, pstrdup(value));
		Py_DECREF(p_string);
		Py_DECREF(p_item);
	}
	errorCheck();
	Py_DECREF(p_iter);
	Py_DECREF(p_tables);
	return cmds;
}
/* The version that might do the trampoline. */
static List *
multicornImportForeignSchema(ImportForeignSchemaStmt * stmt,
			     Oid serverOid)
{
	multicorn_init();
	if (multicorn_plpython_inline_handler != NULL) {
		TrampolineData td;
		td.func = (TrampolineFunc)multicornImportForeignSchemaReal;
		td.return_data = NULL;
		td.args[0] = (void *)stmt;
		td.args[1] = (void *)(unsigned long)serverOid;
		td.args[2] = NULL;
		td.args[3] = NULL;
		td.args[4] = NULL;
		multicornCallTrampoline(&td);
		return (List *)(td.return_data);
	}
	return multicornImportForeignSchemaReal(stmt, serverOid);
}

#endif


/*
 *	"Serialize" a MulticornPlanState, so that it is safe to be carried
 *	between the plan and the execution safe.
 */
void *
serializePlanState(MulticornPlanState * state)
{
	List	   *result = NULL;
	
	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));
	
	result = lappend(result, makeConst(INT4OID,
						  -1, InvalidOid, 4, Int32GetDatum(state->numattrs), false, true));
	result = lappend(result, makeConst(INT4OID,
					-1, InvalidOid, 4, Int32GetDatum(state->foreigntableid), false, true));
	result = lappend(result, state->target_list);

	result = lappend(result, serializeDeparsedSortGroup(state->pathkeys));

	return result;
}

/*
 *	"Deserialize" an internal state and inject it in an
 *	MulticornExecState
 */
MulticornExecState *
initializeExecState(void *internalstate)
{
	MulticornExecState *execstate = palloc0(sizeof(MulticornExecState));
	List	   *values = (List *) internalstate;
	AttrNumber	attnum = ((Const *) linitial(values))->constvalue;
	Oid			foreigntableid = ((Const *) lsecond(values))->constvalue;
	List		*pathkeys;

	ereport(DEBUG5, (errmsg("MULTICORN FILE=%s LINE=%d FUNC=%s",  __FILE__, __LINE__,__PRETTY_FUNCTION__)));	

	/* Those list must be copied, because their memory context can become */
	/* invalid during the execution (in particular with the cursor interface) */
	execstate->target_list = copyObject(lthird(values));
	pathkeys = lfourth(values);
	execstate->pathkeys = deserializeDeparsedSortGroup(pathkeys);
	execstate->fdw_instance = getInstance(foreigntableid);
	execstate->buffer = makeStringInfo();
	execstate->cinfos = palloc0(sizeof(ConversionInfo *) * attnum);
	execstate->values = palloc(attnum * sizeof(Datum));
	execstate->nulls = palloc(attnum * sizeof(bool));
	execstate->ftable_oid = foreigntableid;
	return execstate;
}
