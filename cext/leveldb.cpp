#include <Python.h>
#include <assert.h>
#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "leveldb/cache.h"
#include "leveldb/comparator.h"

using namespace std;

class LongComparator : public leveldb::Comparator {
public:
  int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
    assert(a.size() >= sizeof(long));
    assert(b.size() >= sizeof(long));
    long la = *((long *)a.data());
    long lb = *((long *)b.data());
#if 0
    if (a.size() != sizeof(long)) {
      cout << la << ":size " << a.size() << endl;
    }
    if (b.size() != sizeof(long)) {
      cout << lb << ":size " << b.size() << endl;
    }
#endif
    if (la < lb) return -1;
    if (la > lb) return 1;
    return 0;
  }
  const char* Name() const { return "LongComparator"; }
  void FindShortestSeparator(string*, const leveldb::Slice&) const { }
  void FindShortSuccessor(string*) const { }
};

typedef struct PyLevelDBIntHash : public PyObject {
  //PyObject_HEAD
  //
  leveldb::DB *db;

  static int init(PyObject *self, PyObject *args, PyObject *kwds);
  static void dealloc(PyObject *self);
  static PyObject *put(PyObject *self, PyObject *args);
  static PyObject *get(PyObject *self, PyObject *args);
} PyLevelDBIntHash;

LongComparator comparator;

int
PyLevelDBIntHash::init(PyObject *self, PyObject *args, PyObject *kwds) {
  const char *kwlist[] = { "dir", "block_cache_size", "block_size",
		     "write_buffer_size",
		     "max_open_files",
		     "block_restart_interval",
		     NULL };
  char *dbdir;
  size_t cache_size = 0;
  leveldb::Options options;
  if (!PyArg_ParseTupleAndKeywords(args, kwds, "s|nnnii", (char **)kwlist,
				   &dbdir,
				   &cache_size, &options.block_size,
				   &options.write_buffer_size,
				   &options.max_open_files,
				   &options.block_restart_interval))
    return -1;
  PyLevelDBIntHash *THIS = (PyLevelDBIntHash *)self;
  options.create_if_missing = true;
  if (cache_size > 0)
    options.block_cache = leveldb::NewLRUCache(cache_size);
  options.comparator = &comparator;

  leveldb::Status status;
  Py_BEGIN_ALLOW_THREADS
  status = leveldb::DB::Open(options, dbdir, &THIS->db);
  Py_END_ALLOW_THREADS
  if (!status.ok()) {
    return -1;
  }
  return 0;
}

void
PyLevelDBIntHash::dealloc(PyObject *self) {
  PyLevelDBIntHash *THIS = (PyLevelDBIntHash *)self;
  if (THIS->db != NULL) {
    delete THIS->db;
    THIS->db = NULL;
  }
  self->ob_type->tp_free(self);
}

PyObject *
PyLevelDBIntHash::put(PyObject *self, PyObject *args) {
  PyLevelDBIntHash *THIS = (PyLevelDBIntHash *)self;
  PY_LONG_LONG key;
  char *value;
  int value_len;
  if (!PyArg_ParseTuple(args, "Lt#:put", &key, &value, &value_len))
    return NULL;
  leveldb::Slice keyslice((char *)&key, sizeof(PY_LONG_LONG));
  leveldb::Slice valueslice(value, value_len);
  leveldb::Status sw;
  Py_BEGIN_ALLOW_THREADS
  sw = THIS->db->Put(leveldb::WriteOptions(), keyslice, valueslice);
  Py_END_ALLOW_THREADS
  // TODO: throw exception upon error
  PyObject *rv = sw.ok() ? Py_True : Py_False;
  Py_INCREF(rv);
  return rv;
}

PyObject *
PyLevelDBIntHash::get(PyObject *self, PyObject *args) {
  PyLevelDBIntHash *THIS = (PyLevelDBIntHash *)self;
  PY_LONG_LONG key;
  if (!PyArg_ParseTuple(args, "L:get", &key))
    return NULL;
  leveldb::Slice keyslice((char *)&key, sizeof(PY_LONG_LONG));
  string value;
  leveldb::Status sr;
  Py_BEGIN_ALLOW_THREADS
  sr = THIS->db->Get(leveldb::ReadOptions(), keyslice, &value);
  Py_END_ALLOW_THREADS
  // TODO: throw exception upon error
  if (sr.IsNotFound()) {
    Py_RETURN_NONE;
  } else {
    return PyString_FromStringAndSize(value.data(), value.length());
  }
}

static PyMethodDef PyLevelDBIntHash_methods[] = {
  {"put", (PyCFunction)PyLevelDBIntHash::put, METH_VARARGS,
   "updates value for the key"},
  {"get", (PyCFunction)PyLevelDBIntHash::get, METH_VARARGS,
   "gets value for the key. None if key does not exist"},
  {NULL}
};

static PyTypeObject PyLevelDBIntHashType {
  PyObject_HEAD_INIT(NULL)
  0, // ob_size
  "leveldb.IntHash", // tp_name
  sizeof(PyLevelDBIntHash), // tp_basicsize
  0, /* tp_itemsize */
  (destructor)PyLevelDBIntHash::dealloc, /* tp_dealloc */
  0, /* tp_print */
  0, /* tp_getattr */
  0, /* tp_setattr */
  0, /* tp_compare */
  0, /* tp_repr */
  0, /* tp_as_number */
  0, /* tp_as_sequence */
  0, /* tp_as_mapping */
  0, /* tp_hash */
  0, /* tp_call */
  0, /* tp_str */
  0, /* tp_getattro */
  0, /* tp_setattro */
  0, /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
  "int-key hash table on top of LevelDB", /* tp_doc */
  0, /* tp_traverse */
  0, /* tp_clear */
  0, /* tp_richcompare */
  0, /* tp_weaklistoffset */
  0, /* tp_iter */
  0, /* tp_iternext */
  PyLevelDBIntHash_methods, /* tp_methods */
  0, /* tp_members */
  0, /* tp_getset */
  0, /* tp_base */
  0, /* tp_dict */
  0, /* tp_desc_get */
  0, /* tp_desc_set */
  0, /* tp_dictoffset */
  (initproc)PyLevelDBIntHash::init, /* tp_init */
  0, /* tp_alloc */
  0, /* tp_new */
};

static PyMethodDef module_methods[] = {
  {NULL},
};

extern "C"
PyMODINIT_FUNC
initleveldb(void) {
  PyLevelDBIntHashType.tp_new = PyType_GenericNew;
  if (PyType_Ready(&PyLevelDBIntHashType) < 0)
    return;

  PyObject *m = Py_InitModule3("leveldb", module_methods,
			       "Python bridge for LevelDB");
  if (m == NULL) return;

  Py_INCREF(&PyLevelDBIntHashType);
  PyModule_AddObject(m, "IntHash", (PyObject*)&PyLevelDBIntHashType);
}

    
