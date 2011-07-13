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

typedef struct PyLevelDB : public PyObject {
  //PyObject_HEAD
  //
  leveldb::DB *db;

  // these functions have no corresponding C API
  static PyObject *close(PyObject *self, PyObject *args);
  static PyObject *closed(PyObject *self, PyObject *args);
} PyLevelDB;

typedef struct PyLevelDBIntHash : public PyLevelDB {
  static int init(PyObject *self, PyObject *args, PyObject *kwds);
  static void dealloc(PyObject *self);
  static PyObject *put(PyObject *self, PyObject *args);
  static PyObject *get(PyObject *self, PyObject *args);
  static PyObject *Delete(PyObject *self, PyObject *args);
  static PyObject *new_iterator(PyObject *self, PyObject *args,
				PyObject *kwds);

  static PyObject *repair_db(PyObject *cls, PyObject *args, PyObject *kwds);
} PyLevelDBIntHash;

typedef struct PyLevelDBIterator : public PyObject {
  leveldb::Iterator *it;

  static void dealloc(PyObject *self);
  static PyObject *valid(PyObject *self, PyObject *args);
  static PyObject *seek_to_first(PyObject *self, PyObject *args);
  static PyObject *seek_to_last(PyObject *self, PyObject *args);
  static PyObject *seek(PyObject *self, PyObject *args);
  static PyObject *next(PyObject *self, PyObject *args);
  static PyObject *prev(PyObject *self, PyObject *args);
  static PyObject *key(PyObject *self, PyObject *args);
  static PyObject *value(PyObject *self, PyObject *args);
  static PyObject *status(PyObject *self, PyObject *args);

  //static void register_cleanup(PyObject *self, PyObject *args);
} PyLevelDBIterator;

// general exception object representing non-ok status from LevelDB APIs.
static PyObject *leveldbError;

PyObject *
PyLevelDB::close(PyObject *self, PyObject *args) {
  PyLevelDB* THIS = (PyLevelDB*)self;
  if (THIS->db == NULL) {
    PyErr_SetString(leveldbError, "closed database");
    return NULL;
  }
  delete THIS->db;
  THIS->db = NULL;
  Py_RETURN_NONE;
}

PyObject *
PyLevelDB::closed(PyObject *self, PyObject *args) {
  PyLevelDB* THIS = (PyLevelDB*)self;
  if (THIS->db == NULL)
    Py_RETURN_TRUE;
  else
    Py_RETURN_FALSE;
}

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
    PyErr_SetString(leveldbError, status.ToString().c_str());
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
  if (THIS->db == NULL) {
    PyErr_SetString(leveldbError, "closed database");
    return NULL;
  }
  leveldb::Slice keyslice((char *)&key, sizeof(PY_LONG_LONG));
  leveldb::Slice valueslice(value, value_len);
  leveldb::Status sw;
  Py_BEGIN_ALLOW_THREADS
  sw = THIS->db->Put(leveldb::WriteOptions(), keyslice, valueslice);
  Py_END_ALLOW_THREADS
  if (!sw.ok()) {
    PyErr_SetString(leveldbError, sw.ToString().c_str());
    return NULL;
  }
  // TODO: should return more useful values?
  PyObject *rv = Py_True;
  Py_INCREF(rv);
  return rv;
}

PyObject *
PyLevelDBIntHash::get(PyObject *self, PyObject *args) {
  PyLevelDBIntHash *THIS = (PyLevelDBIntHash *)self;
  PY_LONG_LONG key;
  if (!PyArg_ParseTuple(args, "L:get", &key))
    return NULL;
  if (THIS->db == NULL) {
    PyErr_SetString(leveldbError, "closed database");
    return NULL;
  }
  leveldb::Slice keyslice((char *)&key, sizeof(PY_LONG_LONG));
  string value;
  leveldb::Status sr;
  Py_BEGIN_ALLOW_THREADS
  sr = THIS->db->Get(leveldb::ReadOptions(), keyslice, &value);
  Py_END_ALLOW_THREADS
  if (sr.IsNotFound()) {
    Py_RETURN_NONE;
  }
  if (!sr.ok()) {
    PyErr_SetString(leveldbError, sr.ToString().c_str());
    return NULL;
  }
  return PyString_FromStringAndSize(value.data(), value.length());
}

PyObject *
PyLevelDBIntHash::Delete(PyObject *self, PyObject *args) {
  PyLevelDBIntHash *THIS = (PyLevelDBIntHash *)self;
  // TODO: support "sync" WriteOption
  PY_LONG_LONG key;
  if (!PyArg_ParseTuple(args, "L:delete", &key))
    return NULL;
  if (THIS->db == NULL) {
    PyErr_SetString(leveldbError, "closed database");
    return NULL;
  }
  leveldb::Slice keyslice((char *)&key, sizeof(PY_LONG_LONG));
  leveldb::Status status;
  Py_BEGIN_ALLOW_THREADS
  status = THIS->db->Delete(leveldb::WriteOptions(), keyslice);
  Py_END_ALLOW_THREADS
  if (!status.ok()) {
    PyErr_SetString(leveldbError, status.ToString().c_str());
    return NULL;
  }
  Py_RETURN_NONE;
}

PyObject *
PyLevelDBIntHash::repair_db(PyObject *cls, PyObject *args, PyObject *kwds) {
  static const char *kwlist[] = { "dbname",
				  "block_cache_size", "block_size",
				  "write_buffer_size", "max_open_files",
				  "block_restart_interval",
				  "create_if_missing", "error_if_exists",
				  NULL };
  char *dbname = NULL;
  size_t cache_size = 0;
  leveldb::Options options;
  if (!PyArg_ParseTupleAndKeywords(args, kwds,
				   "s|nnniibb", (char**)kwlist,
				   &dbname,
				   &cache_size, &options.block_size,
				   &options.write_buffer_size,
				   &options.max_open_files,
				   &options.block_restart_interval,
				   &options.create_if_missing,
				   &options.error_if_exists))
    return NULL;
  if (cache_size > 0)
    options.block_cache = leveldb::NewLRUCache(cache_size);
  options.comparator = &comparator;
  leveldb::Status status;
  Py_BEGIN_ALLOW_THREADS
  status = RepairDB(string(dbname), options);
  Py_END_ALLOW_THREADS
  if (!status.ok()) {
    PyErr_SetString(leveldbError, status.ToString().c_str());
    return NULL;
  }
  Py_RETURN_NONE;
}

static PyMethodDef PyLevelDBIntHash_methods[] = {
  {"put", (PyCFunction)PyLevelDBIntHash::put, METH_VARARGS,
   "updates value for the key"},
  {"get", (PyCFunction)PyLevelDBIntHash::get, METH_VARARGS,
   "gets value for the key. None if key does not exist"},
  {"delete", (PyCFunction)PyLevelDBIntHash::Delete, METH_VARARGS,
   "deletes entry for key. it is not an error to delete nonexistent key"},
  {"new_iterator", (PyCFunction)PyLevelDBIntHash::new_iterator,
   METH_KEYWORDS, "creates new iterator"},
  {"close", (PyCFunction)PyLevelDBIntHash::close, METH_NOARGS,
   "closes database"},
  {"closed", (PyCFunction)PyLevelDBIntHash::closed, METH_NOARGS,
   "returns True if database has been closed"},
  {"repair_db", (PyCFunction)PyLevelDBIntHash::repair_db,
   METH_KEYWORDS|METH_CLASS, "repairs a database. takes database directory and WriteOption as keywords"},
  {NULL}
};

static PyTypeObject PyLevelDBIntHashType = {
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

void
PyLevelDBIterator::dealloc(PyObject *self) {
  PyLevelDBIterator *THIS = (PyLevelDBIterator*)self;
  if (THIS->it != NULL) {
    delete THIS->it;
    THIS->it = NULL;
  }
  self->ob_type->tp_free(self);
}

PyObject *
PyLevelDBIterator::valid(PyObject *self, PyObject *args) {
  PyLevelDBIterator *THIS = (PyLevelDBIterator*)self;
  PyObject *r = THIS->it->Valid() ? Py_True : Py_False;
  Py_INCREF(r);
  return r;
}

PyObject *
PyLevelDBIterator::seek_to_first(PyObject *self, PyObject *args) {
  PyLevelDBIterator *THIS = (PyLevelDBIterator*)self;
  THIS->it->SeekToFirst();
  Py_RETURN_NONE;
}

PyObject *
PyLevelDBIterator::seek_to_last(PyObject *self, PyObject *args) {
  PyLevelDBIterator *THIS = (PyLevelDBIterator*)self;
  THIS->it->SeekToLast();
  Py_RETURN_NONE;
}

PyObject *
PyLevelDBIterator::seek(PyObject *self, PyObject *args) {
  //PyLevelDBIterator *THIS = (PyLevelDBIterator*)self;
  // TODO
  return NULL;
}

PyObject *
PyLevelDBIterator::next(PyObject *self, PyObject *args) {
  PyLevelDBIterator *THIS = (PyLevelDBIterator*)self;
  THIS->it->Next();
  // TODO error check?
  Py_RETURN_NONE;
}

PyObject *
PyLevelDBIterator::prev(PyObject *self, PyObject *args) {
  PyLevelDBIterator *THIS = (PyLevelDBIterator*)self;
  THIS->it->Prev();
  // TODO error check?
  Py_RETURN_NONE;
}

PyObject *
PyLevelDBIterator::key(PyObject *self, PyObject *args) {
  PyLevelDBIterator *THIS = (PyLevelDBIterator*)self;
  // TODO this falls short for IntHash... we want to return int, not
  // string.
  leveldb::Slice k = THIS->it->key();
  return PyString_FromStringAndSize(k.data(), k.size());
}

PyObject *
PyLevelDBIterator::value(PyObject *self, PyObject *args) {
  PyLevelDBIterator *THIS = (PyLevelDBIterator*)self;
  leveldb::Slice v = THIS->it->value();
  return PyString_FromStringAndSize(v.data(), v.size());
}

PyObject *
PyLevelDBIterator::status(PyObject *self, PyObject *args) {
  //PyLevelDBIterator *THIS = (PyLevelDBIterator*)self;
  // TODO
  return NULL;
}

static PyMethodDef PyLevelDBIterator_methods[] = {
  {"valid", (PyCFunction)PyLevelDBIterator::valid, METH_NOARGS,
   ""},
  {"seek_to_first", (PyCFunction)PyLevelDBIterator::seek_to_first,
   METH_NOARGS, ""},
  //  {"seek_to_last", (PyCFunction)PyLevelDBIterator::seek_to_last,
  //   METH_NOARGS, ""},
  //  {"seek", (PyCFunction)PyLevelDBIterator::seek, METH_VARARGS,
  //   ""},
  {"next", (PyCFunction)PyLevelDBIterator::next, METH_NOARGS,
   ""},
  //  {"prev", (PyCFunction)PyLevelDBIterator::prev, METH_NOARGS,
  //   ""},
  {"key", (PyCFunction)PyLevelDBIterator::key, METH_NOARGS,
   ""},
  {"value", (PyCFunction)PyLevelDBIterator::value, METH_NOARGS,
   ""},
  //  {"status", (PyCFunction)PyLevelDBIterator::status, METH_NOARGS,
  //   ""},
  {NULL}
};

static PyTypeObject PyLevelDBIteratorType = {
  PyObject_HEAD_INIT(NULL)
  0, // ob_size
  "leveldb.Iterator", // tp_name
  sizeof(PyLevelDBIterator), // tp_basicsize
  0, /* tp_itemsize */
  (destructor)PyLevelDBIterator::dealloc, /* tp_dealloc */
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
  "LevelDB Iterator", /* tp_doc */
  0, /* tp_traverse */
  0, /* tp_clear */
  0, /* tp_richcompare */
  0, /* tp_weaklistoffset */
  0, /* tp_iter */
  0, /* tp_iternext */
  PyLevelDBIterator_methods, /* tp_methods */
  0, /* tp_members */
  0, /* tp_getset */
  0, /* tp_base */
  0, /* tp_dict */
  0, /* tp_desc_get */
  0, /* tp_desc_set */
  0, /* tp_dictoffset */
  0,  //(initproc)PyLevelDBIterator::init, /* tp_init */
  0, /* tp_alloc */
  0, /* tp_new */
};

PyObject *
PyLevelDBIntHash::new_iterator(PyObject *self, PyObject *args,
			       PyObject *kwds) {
  PyLevelDBIntHash *THIS = (PyLevelDBIntHash *)self;
  // TODO Snapshot not supported yet.
  const char *kwlist[] = { "verify_checksms", "fill_cache", NULL };
  leveldb::ReadOptions options;
  if (!PyArg_ParseTupleAndKeywords(args, kwds, "|bb", (char**)kwlist,
				   &options.verify_checksums,
				   &options.fill_cache))
    return NULL;
  if (THIS->db == NULL) {
    PyErr_SetString(leveldbError, "closed database");
    return NULL;
  }
  leveldb::Iterator *iterator = NULL;
  leveldb::Status status;
  Py_BEGIN_ALLOW_THREADS
  iterator = THIS->db->NewIterator(options);
  Py_END_ALLOW_THREADS
  if (iterator == NULL)
    Py_RETURN_NONE;
  PyLevelDBIterator* obj = PyObject_New(PyLevelDBIterator,
					&PyLevelDBIteratorType);
  obj->it = iterator;
  return obj;
}

static PyMethodDef module_methods[] = {
  {NULL},
};

extern "C"
PyMODINIT_FUNC
initleveldb(void) {
  PyLevelDBIntHashType.tp_new = PyType_GenericNew;
  if (PyType_Ready(&PyLevelDBIntHashType) < 0)
    return;
  PyLevelDBIteratorType.tp_new = PyType_GenericNew;
  if (PyType_Ready(&PyLevelDBIteratorType) < 0)
    return;

  PyObject *m = Py_InitModule3("leveldb", module_methods,
			       "Python bridge for LevelDB");
  if (m == NULL) return;

  Py_INCREF(&PyLevelDBIntHashType);
  PyModule_AddObject(m, "IntHash", (PyObject*)&PyLevelDBIntHashType);

  Py_INCREF(&PyLevelDBIteratorType);
  PyModule_AddObject(m, "Iterator", (PyObject*)&PyLevelDBIteratorType);

  leveldbError = PyErr_NewException((char*)"leveldb.error", NULL, NULL);
  Py_INCREF(leveldbError);
  PyModule_AddObject(m, "error", leveldbError);
}
