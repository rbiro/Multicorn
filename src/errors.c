/*-------------------------------------------------------------------------
 *
 * The Multicorn Foreign Data Wrapper allows you to fetch foreign data in
 * Python in your PostgreSQL server.
 *
 * This module contains error handling functions.
 *
 * This software is released under the postgresql licence
 *
 * author: Kozea
 *
 *
 *-------------------------------------------------------------------------
 */
#include "multicorn.h"
#include "bytesobject.h"
#include "access/xact.h"

void reportException(PyObject *pErrType,
				PyObject *pErrValue,
				PyObject *pErrTraceback);


void
errorCheck()
{
	PyObject   *pErrType,
			   *pErrValue,
			   *pErrTraceback;

	PyErr_Fetch(&pErrType, &pErrValue, &pErrTraceback);
	if (pErrType)
	{
		reportException(pErrType, pErrValue, pErrTraceback);
	}
}

void
reportException(PyObject *pErrType, PyObject *pErrValue, PyObject *pErrTraceback)
{
	char	   *errName,
			   *errValue,
			   *errTraceback = "";
	PyObject   *traceback_list;
	PyObject   *pTemp;
	/* tracebackModule is now global.  Could make format_exception
	   global as well. */
	PyObject   *format_exception = NULL;
	PyObject   *newline = PyString_FromString("\n");
	int			severity;

	/* If traceback failed to load, We need to try to say why, and
	   not just dump core. */
	if ( tracebackModule != NULL) 
	{
		format_exception = PyObject_GetAttrString(tracebackModule,
							  "format_exception");
	} else {
		pErrTraceback = NULL;
	}

	PyErr_NormalizeException(&pErrType, &pErrValue, &pErrTraceback);
	pTemp = PyObject_GetAttrString(pErrType, "__name__");
	errName = PyString_AsString(pTemp);
	errValue = PyString_AsString(PyObject_Str(pErrValue));
	if (pErrTraceback != NULL)
	{
		traceback_list = PyObject_CallFunction(format_exception, "(O,O,O)", pErrType, pErrValue, pErrTraceback);
		errTraceback = PyString_AsString(PyObject_CallMethod(newline, "join", "(O)", traceback_list));
		Py_DECREF(pErrTraceback);
		Py_DECREF(traceback_list);
	}

	if (IsAbortedTransactionBlockState())
	{
		severity = WARNING;
	}
	else
	{
		severity = ERROR;
	}
	if (errstart(severity, __FILE__, __LINE__, PG_FUNCNAME_MACRO, TEXTDOMAIN))
	{
		if (errstart(severity, __FILE__, __LINE__, PG_FUNCNAME_MACRO, TEXTDOMAIN))
			errmsg("Multicorn: Error in python: %s", errName);
		errdetail("%s", errValue);
		errdetail_log("%s", errTraceback);
	}
	Py_DECREF(pErrType);
	Py_DECREF(pErrValue);
	if (format_exception != NULL) 
	{
		Py_DECREF(format_exception);
	}
	Py_DECREF(newline);
	Py_DECREF(pTemp);
	errfinish(0);
}
