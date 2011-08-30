/*
Copyright (C) 2010- The University of Notre Dame.
This software is distributed under the GNU General Public License.
See the file LICENSE for details.
*/

#include <Python.h>
#include <structmember.h>

#include "work_queue.h"
#include "debug.h"

#include <errno.h>
#include <time.h>
#include <string.h>

/* Constants ------------------------------------------------------------- */

#define MODULE_NAME "workqueue"

/* Macros ---------------------------------------------------------------- */

#define PASTE(x, y)		x##y
#define	PY_FCN(p, f)		PASTE(p##_, f)

#define	MODULE_METHOD(p, f)	{#f, (PyCFunction)PY_FCN(p, f), METH_VARARGS | METH_KEYWORDS}
#define TASK_METHOD(f)		MODULE_METHOD(Task, f)
#define WORKQUEUE_METHOD(f)	MODULE_METHOD(WorkQueue, f)

#define	MODULE_GETTER(p, f)	{#f, (getter)PY_FCN(p##_get, f), NULL, NULL, NULL}
#define	TASK_GETTER(f)		MODULE_GETTER(Task, f)
#define	STATS_GETTER(f)		MODULE_GETTER(Stats, f)
#define	WORKQUEUE_GETTER(f)	MODULE_GETTER(WorkQueue, f)

#define	MODULE_GETSETTER(p, f)	{#f, (getter)PY_FCN(p##_get, f), (setter)PY_FCN(p##_set, f), NULL, NULL}
#define	TASK_GETSETTER(f)	MODULE_GETSETTER(Task, f)

#ifndef Py_TYPE
#  define Py_TYPE(o) ((o)->ob_type)
#endif

#ifndef TRUE
#define TRUE 1
#endif

#ifndef FALSE
#define FALSE 0
#endif

/* Task ------------------------------------------------------------------ */

typedef struct {
    PyObject_HEAD
    struct work_queue_task *tp;
} Task;

static int
Task_init(Task *self, PyObject *args, PyObject *kwds)
{
    char *command = NULL;

    if (!PyArg_ParseTuple(args, "s", &command))
	return -1;

    self->tp = work_queue_task_create(command);
    if (!self->tp) {
	PyErr_Format(PyExc_Exception, "could not create task: %s", strerror(errno));
	return -1;
    }

    return 0;
}

static int
Task_dealloc(Task *self, PyObject *args, PyObject *kwds)
{
#ifndef	NDEBUG
    PySys_WriteStderr("[d] Task dealloc\n");
#endif

    if (self->tp)
	work_queue_task_delete(self->tp);

    Py_TYPE(self)->tp_free((PyObject *)self);
    return 0;
}

static PyObject *
Task_specify_algorithm(Task *self, PyObject *args, PyObject *kwds)
{
    int alg;

    if (!PyArg_ParseTuple(args, "i", &alg))
	return NULL;

    work_queue_task_specify_algorithm(self->tp, alg);

    Py_RETURN_NONE;
}

static PyObject *
Task_specify_tag(Task *self, PyObject *args, PyObject *kwds)
{
    PyObject *value;

    if (!PyArg_ParseTuple(args, "O!", &PyString_Type, &value))
	return NULL;

    work_queue_task_specify_tag(self->tp, PyString_AsString(value));
    Py_RETURN_NONE;
}

static PyObject *
Task_specify_input_buffer(Task *self, PyObject *args, PyObject *kwds)
{
    char *kwlist[]  = { "buffer", "remote_name", "cache", NULL };
    PyObject *buffer;
    PyObject *rname;
    int cache = TRUE;
    int flags = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O!O!|i", kwlist, &PyString_Type, &buffer, &PyString_Type, &rname, &cache))
	return NULL;
    
    if (cache)
    	flags = WORK_QUEUE_CACHE;
    else
    	flags = WORK_QUEUE_NOCACHE;

    work_queue_task_specify_buffer(self->tp, PyString_AsString(buffer), PyString_Size(buffer), PyString_AsString(rname), flags);
    Py_RETURN_NONE;
}

static PyObject *
Task_specify_file(Task *self, PyObject *args, PyObject *kwds, int type)
{
    char *kwlist[]  = { "local_name", "remote_name", "cache", NULL };
    PyObject *rname;
    PyObject *lname;
    int cache = TRUE;
    int flags = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O!O!|i", kwlist, &PyString_Type, &lname, &PyString_Type, &rname, &cache))
	return NULL;

    if (cache)
    	flags = WORK_QUEUE_CACHE;
    else
    	flags = WORK_QUEUE_NOCACHE;

    work_queue_task_specify_file(self->tp, PyString_AsString(lname), PyString_AsString(rname), type, flags);
    Py_RETURN_NONE;
}

static PyObject *
Task_specify_input_file(Task *self, PyObject *args, PyObject *kwds)
{
    return Task_specify_file(self, args, kwds, WORK_QUEUE_INPUT);
}

static PyObject *
Task_specify_output_file(Task *self, PyObject *args, PyObject *kwds)
{
    return Task_specify_file(self, args, kwds, WORK_QUEUE_OUTPUT);
}

static PyObject *
Task_get_algorithm(Task *self, void *closure)
{
    return PyInt_FromLong(self->tp->worker_selection_algorithm);
}

static int
Task_set_algorithm(Task *self, PyObject *value, void *closure)
{
    if (!PyInt_Check(value)) {
	PyErr_Format(PyExc_Exception, "command must be a string");
	return -1;
    }

    work_queue_task_specify_algorithm(self->tp, PyInt_AsLong(value));
    return 0;
}

static PyObject *
Task_get_command(Task *self, void *closure)
{
    if (self->tp->command_line)
	return PyString_FromString(self->tp->command_line);

    Py_RETURN_NONE;
}

static int
Task_set_command(Task *self, PyObject *value, void *closure)
{
    if (!PyString_Check(value)) {
	PyErr_Format(PyExc_Exception, "command must be a string");
	return -1;
    }

    if (self->tp->command_line)
	free(self->tp->command_line);

    self->tp->command_line = strdup(PyString_AsString(value));
    return 0;
}

static PyObject *
Task_get_tag(Task *self, void *closure)
{
    if (self->tp->tag)
	return PyString_FromString(self->tp->tag);

    Py_RETURN_NONE;
}

static int
Task_set_tag(Task *self, PyObject *value, void *closure)
{
    if (!PyString_Check(value)) {
	PyErr_Format(PyExc_Exception, "tag must be a string");
	return -1;
    }

    work_queue_task_specify_tag(self->tp, PyString_AsString(value));

    return 0;
}

static PyObject *
Task_get_output(Task *self, void *closure)
{
    if (self->tp->output) 
	return PyString_FromString(self->tp->output);

    Py_RETURN_NONE;
}

static PyObject *
Task_get_preferred_host(Task *self, void *closure)
{
    if (self->tp->preferred_host)
	return PyString_FromString(self->tp->preferred_host);

    Py_RETURN_NONE;
}

static int
Task_set_preferred_host(Task *self, PyObject *value, void *closure)
{
    if (!PyString_Check(value)) {
	PyErr_Format(PyExc_Exception, "preferred_host must be a string");
	return -1;
    }

    if (self->tp->preferred_host)
	free(self->tp->preferred_host);

    self->tp->preferred_host = strdup(PyString_AsString(value));
    return 0;
}

static PyObject *
Task_get_taskid(Task *self, void *closure)
{
    return PyInt_FromLong(self->tp->taskid);
}

static PyObject *
Task_get_status(Task *self, void *closure)
{
    return PyInt_FromLong(self->tp->status);
}

static PyObject *
Task_get_return_status(Task *self, void *closure)
{
    return PyInt_FromLong(self->tp->return_status);
}

static PyObject *
Task_get_result(Task *self, void *closure)
{
    return PyInt_FromLong(self->tp->result);
}

static PyObject *
Task_get_host(Task *self, void *closure)
{
    if (self->tp->host)
	return PyString_FromString(self->tp->host);

    Py_RETURN_NONE;
}

static PyObject *
Task_get_submit_time(Task *self, void *closure)
{
    return PyLong_FromLong(self->tp->submit_time);
}

static PyObject *
Task_get_start_time(Task *self, void *closure)
{
    return PyLong_FromLong(self->tp->start_time);
}

static PyObject *
Task_get_finish_time(Task *self, void *closure)
{
    return PyLong_FromLong(self->tp->finish_time);
}

static PyObject *
Task_get_transfer_start_time(Task *self, void *closure)
{
    return PyLong_FromLong(self->tp->transfer_start_time);
}

static PyObject *
Task_get_computation_time(Task *self, void *closure)
{
    return PyLong_FromLong(self->tp->computation_time);
}

static PyObject *
Task_get_total_bytes_transferred(Task *self, void *closure)
{
    return PyLong_FromLong(self->tp->total_bytes_transferred);
}

static PyObject *
Task_get_total_transfer_time(Task *self, void *closure)
{
    return PyLong_FromLong(self->tp->total_transfer_time);
}

static PyMemberDef TaskMembers[] = {
    { NULL }
};

static PyMethodDef TaskMethods[] = {
    TASK_METHOD(specify_algorithm),
    TASK_METHOD(specify_input_buffer),
    TASK_METHOD(specify_input_file),
    TASK_METHOD(specify_output_file),
    TASK_METHOD(specify_tag),
    { NULL }
};

static PyGetSetDef TaskGetSetters[] = {
    TASK_GETSETTER(algorithm),
    TASK_GETSETTER(command),
    TASK_GETSETTER(tag),
    TASK_GETTER(output),
    TASK_GETSETTER(preferred_host),
    TASK_GETTER(taskid),
    TASK_GETTER(status),
    TASK_GETTER(return_status),
    TASK_GETTER(result),
    TASK_GETTER(host),
    TASK_GETTER(submit_time),
    TASK_GETTER(start_time),
    TASK_GETTER(finish_time),
    TASK_GETTER(transfer_start_time),
    TASK_GETTER(computation_time),
    TASK_GETTER(total_bytes_transferred),
    TASK_GETTER(total_transfer_time),
    {NULL}
};

static PyTypeObject TaskType = {
    PyObject_HEAD_INIT(NULL)
    0,						/* ob_size           */
    "Task",					/* tp_name           */
    sizeof(Task),				/* tp_basicsize      */
    0,						/* tp_itemsize       */
    (destructor)Task_dealloc,			/* tp_dealloc        */
    0,						/* tp_print          */
    0,						/* tp_getattr        */
    0,						/* tp_setattr        */
    0,						/* tp_compare        */
    0,						/* tp_repr           */
    0,						/* tp_as_number      */
    0,						/* tp_as_sequence    */
    0,						/* tp_as_mapping     */
    0,						/* tp_hash           */
    0,						/* tp_call           */
    0,						/* tp_str            */
    0,						/* tp_getattro       */
    0,						/* tp_setattro       */
    0,						/* tp_as_buffer      */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,	/* tp_flags          */
    "Task object",				/* tp_doc            */
    0,						/* tp_traverse       */
    0,						/* tp_clear          */
    0,						/* tp_richcompare    */
    0,						/* tp_weaklistoffset */
    0,						/* tp_iter           */
    0,						/* tp_iternext       */
    TaskMethods,				/* tp_methods        */
    TaskMembers,				/* tp_members        */
    TaskGetSetters,				/* tp_getset         */
    0,						/* tp_base           */
    0,						/* tp_dict           */
    0,						/* tp_descr_get      */
    0,						/* tp_descr_set      */
    0,						/* tp_dictoffset     */
    (initproc)Task_init,			/* tp_init           */
    0,						/* tp_alloc	     */
    0,						/* tp_new	     */
};

/* Stats ----------------------------------------------------------------- */

typedef struct {
    PyObject_HEAD
    struct work_queue_stats stats;
} Stats;

static int
Stats_init(Stats *self, PyObject *args, PyObject *kwds)
{
    return 0;
}

static int
Stats_dealloc(Stats *self, PyObject *args, PyObject *kwds)
{
#ifndef	NDEBUG
    PySys_WriteStderr("[d] Stats dealloc\n");
#endif

    Py_TYPE(self)->tp_free((PyObject *)self);
    return 0;
}

#define STATS_GET_FUNCTION(f) \
    static PyObject * Stats_get_##f(Stats *self, void * closure) { return PyInt_FromLong(self->stats.f); }

STATS_GET_FUNCTION(workers_init);
STATS_GET_FUNCTION(workers_ready);
STATS_GET_FUNCTION(workers_busy);
STATS_GET_FUNCTION(tasks_running);
STATS_GET_FUNCTION(tasks_waiting);
STATS_GET_FUNCTION(tasks_complete);
STATS_GET_FUNCTION(total_tasks_dispatched);
STATS_GET_FUNCTION(total_tasks_complete);
STATS_GET_FUNCTION(total_workers_joined);
STATS_GET_FUNCTION(total_workers_removed);

static PyMemberDef StatsMembers[] = {
    { NULL }
};

static PyMethodDef StatsMethods[] = {
    { NULL }
};

static PyGetSetDef StatsGetSetters[] = {
    STATS_GETTER(workers_init),
    STATS_GETTER(workers_ready),
    STATS_GETTER(workers_busy),
    STATS_GETTER(tasks_running),
    STATS_GETTER(tasks_waiting),
    STATS_GETTER(tasks_complete),
    STATS_GETTER(total_tasks_dispatched),
    STATS_GETTER(total_tasks_complete),
    STATS_GETTER(total_workers_joined),
    STATS_GETTER(total_workers_removed),
    {NULL}
};

static PyTypeObject StatsType = {
    PyObject_HEAD_INIT(NULL)
    0,						/* ob_size           */
    "Stats",					/* tp_name           */
    sizeof(Stats),				/* tp_basicsize      */
    0,						/* tp_itemsize       */
    (destructor)Stats_dealloc,			/* tp_dealloc        */
    0,						/* tp_print          */
    0,						/* tp_getattr        */
    0,						/* tp_setattr        */
    0,						/* tp_compare        */
    0,						/* tp_repr           */
    0,						/* tp_as_number      */
    0,						/* tp_as_sequence    */
    0,						/* tp_as_mapping     */
    0,						/* tp_hash           */
    0,						/* tp_call           */
    0,						/* tp_str            */
    0,						/* tp_getattro       */
    0,						/* tp_setattro       */
    0,						/* tp_as_buffer      */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,	/* tp_flags          */
    "Stats object",				/* tp_doc            */
    0,						/* tp_traverse       */
    0,						/* tp_clear          */
    0,						/* tp_richcompare    */
    0,						/* tp_weaklistoffset */
    0,						/* tp_iter           */
    0,						/* tp_iternext       */
    StatsMethods,				/* tp_methods        */
    StatsMembers,				/* tp_members        */
    StatsGetSetters,				/* tp_getset         */
    0,						/* tp_base           */
    0,						/* tp_dict           */
    0,						/* tp_descr_get      */
    0,						/* tp_descr_set      */
    0,						/* tp_dictoffset     */
    (initproc)Stats_init,			/* tp_init           */
    0,						/* tp_alloc	     */
    0,						/* tp_new	     */
};

/* WorkQueue ------------------------------------------------------------- */

typedef struct {
    PyObject_HEAD
    PyObject *map;
    struct work_queue *wqp;
    Stats *stats;
} WorkQueue;

static int
WorkQueue_init(WorkQueue *self, PyObject *args, PyObject *kwds)
{
    char *kwlist[]  = { "port", "name", "catalog", "exclusive", NULL };
    PyObject *name  = NULL;
    int   port      = WORK_QUEUE_DEFAULT_PORT;
    int   catalog   = TRUE;
    int   exclusive = TRUE;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|iO!ii", kwlist, &port, &PyString_Type, &name, &catalog, &exclusive))
	return -1;

    self->wqp = work_queue_create(port);
    if (!self->wqp) {
	PyErr_Format(PyExc_ValueError, "could not create workqueue on port %d: %s", port, strerror(errno));
	return -1;
    }

    self->map = PyDict_New();
    if (!self->map) {
	PyErr_Format(PyExc_Exception, "could not create mapping dictionary");
	return -1;
    }
    
    self->stats = (Stats *)PyObject_New(Stats, &StatsType);
    if (!self->stats) {
	PyErr_Format(PyExc_Exception, "could not create stats member");
	return -1;
    }

    if (name) {
    	work_queue_specify_name(self->wqp, PyString_AsString(name));
    }

    work_queue_specify_master_mode(self->wqp, catalog);
    work_queue_specify_worker_mode(self->wqp, exclusive);

    return 0;
}

static int
WorkQueue_dealloc(WorkQueue *self, PyObject *args, PyObject *kwds)
{
#ifndef	NDEBUG
    PySys_WriteStderr("[d] WorkQueue dealloc\n");
#endif

    if (self->wqp)
	work_queue_delete(self->wqp);

    if (self->map) {
	Py_DECREF(self->map);
    }
    
    if (self->stats) {
	Py_DECREF(self->stats);
    }

    Py_TYPE(self)->tp_free((PyObject *)self);
    return 0;
}

static PyObject *
WorkQueue_activate_fast_abort(WorkQueue *self, PyObject *args, PyObject *kwds)
{
    double multiplier;

    if (!PyArg_ParseTuple(args, "d", &multiplier))
	return NULL;

    work_queue_activate_fast_abort(self->wqp, multiplier);

    Py_RETURN_NONE;
}

static PyObject *
WorkQueue_empty(WorkQueue *self, PyObject *args, PyObject *kwds)
{
    return PyBool_FromLong(work_queue_empty(self->wqp));
}

static PyObject *
WorkQueue_hungry(WorkQueue *self, PyObject *args, PyObject *kwds)
{
    return PyBool_FromLong(work_queue_hungry(self->wqp));
}

static PyObject *
WorkQueue_shutdown_workers(WorkQueue *self, PyObject *args, PyObject *kwds)
{
    char *kwlist[] = { "n", NULL };
    int   n = 0;
    
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|i", kwlist, &n))
	return NULL;

    return PyInt_FromLong(work_queue_shut_down_workers(self->wqp, n));
}

static PyObject *
WorkQueue_specify_algorithm(WorkQueue *self, PyObject *args, PyObject *kwds)
{
    int alg;

    if (!PyArg_ParseTuple(args, "i", &alg))
	return NULL;

    work_queue_specify_algorithm(self->wqp, alg);

    Py_RETURN_NONE;
}

static PyObject *
WorkQueue_specify_name(WorkQueue *self, PyObject *args, PyObject *kwds)
{
    const char *name;

    if (!PyArg_ParseTuple(args, "s", &name))
	return NULL;

    work_queue_specify_name(self->wqp, name);

    Py_RETURN_NONE;
}

static PyObject *
WorkQueue_specify_master_mode(WorkQueue *self, PyObject *args, PyObject *kwds)
{
    int master_mode;

    if (!PyArg_ParseTuple(args, "i", &master_mode))
	return NULL;

    work_queue_specify_master_mode(self->wqp, master_mode);

    Py_RETURN_NONE;
}

static PyObject *
WorkQueue_specify_worker_mode(WorkQueue *self, PyObject *args, PyObject *kwds)
{
    int worker_mode;

    if (!PyArg_ParseTuple(args, "i", &worker_mode))
	return NULL;

    work_queue_specify_worker_mode(self->wqp, worker_mode);

    Py_RETURN_NONE;
}

static PyObject *
WorkQueue_submit(WorkQueue *self, PyObject *args, PyObject *kwds)
{
    Task *t;
    PyObject *key = NULL;

    if (!PyArg_ParseTuple(args, "O!", &TaskType, &t))
	return NULL;

    work_queue_submit(self->wqp, t->tp);

    key = PyInt_FromLong((long)t->tp);
    PyDict_SetItem(self->map, key, (PyObject *)t); 

    Py_XDECREF(key);
    Py_RETURN_NONE;
}

static PyObject *
WorkQueue_wait(WorkQueue *self, PyObject *args, PyObject *kwds)
{
    Task *t;
    PyObject *key = NULL;
    struct work_queue_task *tp;
    int timeout;

    if (!PyArg_ParseTuple(args, "i", &timeout))
	return NULL;

    tp = work_queue_wait(self->wqp, timeout);
    if (!tp) {
	Py_RETURN_NONE;
    }

    key = PyInt_FromLong((long)tp);
    t   = (Task *)PyDict_GetItem(self->map, key);
    if (!t) {
	Py_XDECREF(key);
	return PyErr_Format(PyExc_Exception, "unmapped task");
    }
    Py_INCREF(t);

    if (PyDict_DelItem(self->map, key) < 0) {
	Py_XDECREF(key);
	return PyErr_Format(PyExc_Exception, "unable to delete task from map");
    }

    Py_XDECREF(key);
    return (PyObject *)t;
}

static PyObject *
WorkQueue_get_port(WorkQueue *self, void *closure)
{
    return PyInt_FromLong(work_queue_port(self->wqp));
}

static PyObject *
WorkQueue_get_stats(WorkQueue *self, void *closure)
{
    work_queue_get_stats(self->wqp, &(self->stats->stats));
    Py_INCREF(self->stats);
    
    return (PyObject *)self->stats;
}

static PyMemberDef WorkQueueMembers[] = {
    { NULL }
};

static PyMethodDef WorkQueueMethods[] = {
    WORKQUEUE_METHOD(activate_fast_abort),
    WORKQUEUE_METHOD(empty),
    WORKQUEUE_METHOD(hungry),
    WORKQUEUE_METHOD(shutdown_workers),
    WORKQUEUE_METHOD(specify_algorithm),
    WORKQUEUE_METHOD(specify_name),
    WORKQUEUE_METHOD(specify_master_mode),
    WORKQUEUE_METHOD(specify_worker_mode),
    WORKQUEUE_METHOD(submit),
    WORKQUEUE_METHOD(wait),
    { NULL }
};


static PyGetSetDef WorkQueueGetSetters[] = {
    WORKQUEUE_GETTER(port),
    WORKQUEUE_GETTER(stats),
    {NULL}
};

static PyTypeObject WorkQueueType = {
    PyObject_HEAD_INIT(NULL)
    0,						/* ob_size           */
    "WorkQueue",				/* tp_name           */
    sizeof(WorkQueue),				/* tp_basicsize      */
    0,						/* tp_itemsize       */
    (destructor)WorkQueue_dealloc,		/* tp_dealloc        */
    0,						/* tp_print          */
    0,						/* tp_getattr        */
    0,						/* tp_setattr        */
    0,						/* tp_compare        */
    0,						/* tp_repr           */
    0,						/* tp_as_number      */
    0,						/* tp_as_sequence    */
    0,						/* tp_as_mapping     */
    0,						/* tp_hash           */
    0,						/* tp_call           */
    0,						/* tp_str            */
    0,						/* tp_getattro       */
    0,						/* tp_setattro       */
    0,						/* tp_as_buffer      */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,	/* tp_flags          */
    "WorkQueue object",				/* tp_doc            */
    0,						/* tp_traverse       */
    0,						/* tp_clear          */
    0,						/* tp_richcompare    */
    0,						/* tp_weaklistoffset */
    0,						/* tp_iter           */
    0,						/* tp_iternext       */
    WorkQueueMethods,				/* tp_methods        */
    WorkQueueMembers,				/* tp_members        */
    WorkQueueGetSetters,			/* tp_getset         */
    0,						/* tp_base           */
    0,						/* tp_dict           */
    0,						/* tp_descr_get      */
    0,						/* tp_descr_set      */
    0,						/* tp_dictoffset     */
    (initproc)WorkQueue_init,			/* tp_init           */
    0,						/* tp_alloc	     */
    0,						/* tp_new	     */
};

/* Debugging ------------------------------------------------------------- */

static PyObject *
workqueue_set_debug_flag(PyObject *self, PyObject *args)
{
    const char *flag;

    if (!PyArg_ParseTuple(args, "s", &flag))
	return NULL;

    return PyBool_FromLong(debug_flags_set(flag));
}

/* Module Methods Table -------------------------------------------------- */

static PyMethodDef
ModuleMethods[] = {
    {"set_debug_flag", workqueue_set_debug_flag, METH_VARARGS, "Set debug flag."},
    {NULL, NULL, 0, NULL}
};

/* Module Initializer ---------------------------------------------------- */

void
initworkqueue(void)
{
    PyObject *m;

    m = Py_InitModule(MODULE_NAME, ModuleMethods);

    WorkQueueType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&WorkQueueType) < 0) return;
    
    TaskType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&TaskType) < 0) return;
    
    StatsType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&StatsType) < 0) return;

    Py_INCREF(&WorkQueueType);
    Py_INCREF(&TaskType);
    Py_INCREF(&StatsType);

    PyModule_AddObject(m, "WorkQueue", (PyObject*)&WorkQueueType);
    PyModule_AddObject(m, "Task", (PyObject*)&TaskType);
    PyModule_AddObject(m, "Stats", (PyObject*)&StatsType);

    PyModule_AddIntConstant(m, "WORK_QUEUE_DEFAULT_PORT",     WORK_QUEUE_DEFAULT_PORT);
    PyModule_AddIntConstant(m, "WORK_QUEUE_RANDOM_PORT",      -1);

    PyModule_AddIntConstant(m, "WORK_QUEUE_SCHEDULE_UNSET",   WORK_QUEUE_SCHEDULE_UNSET);
    PyModule_AddIntConstant(m, "WORK_QUEUE_SCHEDULE_FCFS",    WORK_QUEUE_SCHEDULE_FCFS);
    PyModule_AddIntConstant(m, "WORK_QUEUE_SCHEDULE_FILES",   WORK_QUEUE_SCHEDULE_FILES);
    PyModule_AddIntConstant(m, "WORK_QUEUE_SCHEDULE_TIME",    WORK_QUEUE_SCHEDULE_TIME);
    PyModule_AddIntConstant(m, "WORK_QUEUE_SCHEDULE_DEFAULT", WORK_QUEUE_SCHEDULE_DEFAULT);
    PyModule_AddIntConstant(m, "WORK_QUEUE_SCHEDULE_DEFAULT", WORK_QUEUE_SCHEDULE_DEFAULT);
    
    PyModule_AddIntConstant(m, "WORK_QUEUE_RESULT_UNSET",	   WORK_QUEUE_RESULT_UNSET);
    PyModule_AddIntConstant(m, "WORK_QUEUE_RESULT_INPUT_FAIL",	   WORK_QUEUE_RESULT_INPUT_FAIL);
    PyModule_AddIntConstant(m, "WORK_QUEUE_RESULT_INPUT_MISSING",  WORK_QUEUE_RESULT_INPUT_MISSING);
    PyModule_AddIntConstant(m, "WORK_QUEUE_RESULT_FUNCTION_FAIL",  WORK_QUEUE_RESULT_FUNCTION_FAIL);
    PyModule_AddIntConstant(m, "WORK_QUEUE_RESULT_OUTPUT_FAIL",	   WORK_QUEUE_RESULT_OUTPUT_FAIL);
    PyModule_AddIntConstant(m, "WORK_QUEUE_RESULT_OUTPUT_MISSING", WORK_QUEUE_RESULT_OUTPUT_MISSING);
    PyModule_AddIntConstant(m, "WORK_QUEUE_RESULT_LINK_FAIL",	   WORK_QUEUE_RESULT_LINK_FAIL);

    PyModule_AddIntConstant(m, "WORK_QUEUE_INPUT",   WORK_QUEUE_INPUT);
    PyModule_AddIntConstant(m, "WORK_QUEUE_OUTPUT",  WORK_QUEUE_OUTPUT);
    PyModule_AddIntConstant(m, "WORK_QUEUE_NOCACHE", WORK_QUEUE_NOCACHE);
    PyModule_AddIntConstant(m, "WORK_QUEUE_CACHE",   WORK_QUEUE_CACHE);
    
    PyModule_AddIntConstant(m, "WORK_QUEUE_MASTER_MODE_STANDALONE", WORK_QUEUE_MASTER_MODE_STANDALONE);
    PyModule_AddIntConstant(m, "WORK_QUEUE_MASTER_MODE_CATALOG",    WORK_QUEUE_MASTER_MODE_CATALOG);

    PyModule_AddIntConstant(m, "WORK_QUEUE_WORKER_MODE_SHARED",     WORK_QUEUE_WORKER_MODE_SHARED);
    PyModule_AddIntConstant(m, "WORK_QUEUE_WORKER_MODE_EXCLUSIVE",  WORK_QUEUE_WORKER_MODE_EXCLUSIVE);

    /* hackity hack hack */
    debug_config("python-workqueue");

    if (PyErr_Occurred())
	Py_FatalError("can't initialize module " MODULE_NAME);
}

/* 
vim: sw=4 sts=4 ts=8 ft=c
*/
