#include <new>
#include <Python.h>
#include <structmember.h>
#include "fpgenerator.h"

typedef struct {
  PyObject_HEAD
  FPGenerator cobj;

  static int init(PyObject *self, PyObject *args, PyObject *kwds);
  static PyObject *fp(PyObject *self, PyObject *args);
  static PyObject *extend(PyObject *self, PyObject *args);
  static PyObject *extend_byte(PyObject *self, PyObject *args);
  static PyObject *reduce(PyObject *self, PyObject *args);

#if 0
  static PyMemberDef members[];
#endif
} cFPGenerator;

int
cFPGenerator::init(PyObject *self, PyObject *args, PyObject *kwds) {
  ulong polynomial;
  int degree;
  static const char *kwlist[] = {"polynomial", "degree", NULL};
  if (!PyArg_ParseTupleAndKeywords(args, kwds, "Ki", (char**)kwlist,
				   &polynomial, &degree))
    return -1;
  if (degree > 64 || degree < 1) {
    PyErr_SetString(PyExc_ValueError, "degree must be 1 to 64");
    return -1;
  }
  new(&((cFPGenerator*)self)->cobj) FPGenerator(polynomial, degree);
  return 0;
}

PyObject *
cFPGenerator::fp(PyObject *self, PyObject *args) {
  const char *s;
  Py_ssize_t n;
  if (!PyArg_ParseTuple(args, "s#", &s, &n))
    return NULL;
  ulong f = ((cFPGenerator*)self)->cobj.fp(s, 0, n);
  return PyLong_FromUnsignedLongLong(f);
}

PyObject *
cFPGenerator::extend(PyObject *self, PyObject *args) {
  ulong f;
  const char *s;
  Py_ssize_t sn;
  int start = 0, n = -1;
  if (!PyArg_ParseTuple(args, "Ks#|ii", &f, &s, &sn, &start, &n))
    return NULL;
  if (start > sn) start = sn;
  if (n < 0) n = sn - start;
  if (n == 0)
    return args;
  ulong ff = ((cFPGenerator*)self)->cobj.extend(f, s, start, n);
  return PyLong_FromUnsignedLongLong(ff);
}

PyObject *
cFPGenerator::extend_byte(PyObject *self, PyObject *args) {
  ulong f;
  int v;
  if (!PyArg_ParseTuple(args, "Ki", &f, &v))
    return NULL;
  ulong ff = ((cFPGenerator*)self)->cobj.extend_byte(f, (byte)(v & 0xff));
  return PyLong_FromUnsignedLongLong(ff);
}

PyObject *
cFPGenerator::reduce(PyObject *self, PyObject *args) {
  ulong fp;
  if (!PyArg_ParseTuple(args, "K", &fp))
    return NULL;
  ulong ff = ((cFPGenerator*)self)->cobj.reduce(fp);
  return PyLong_FromUnsignedLongLong(ff);
}

#if 0
PyMemberDef cFPGenerator::members[] = {
  {"degree", T_INT, offsetof(cFPGenerator, cobj.degree), 0, "degree"},
  {"polynomial", T_ULONGLONG, offsetof(cFPGenerator, cobj.polynomial), 0,
   "polynomial"},
  {"empty", T_ULONGLONG, offsetof(cFPGenerator, cobj.empty), 0, "empty"},
  {NULL},
};
#endif
#if 0
static PyMemberDef cFPGenerator_members[] = {
  {NULL}
};
#endif
static PyMethodDef cFPGenerator_methods[] = {
  {"fp", (PyCFunction)cFPGenerator::fp, METH_VARARGS,
   "computes fingerprint value for string/unicode/buffer"},
  {"extend", (PyCFunction)cFPGenerator::extend, METH_VARARGS,
   "extends fingerprint f by adding (all bits of) v"},
  {"extend_byte", (PyCFunction)cFPGenerator::extend_byte, METH_VARARGS,
   "extends f with lower eight bits of v without full reduction. "
   "In other words, returns a polynomial that is equal (mod polynomial) "
   "to the desired fingerprint but may be of higher degree than the "
   "desired fingerprint."},
  {"reduce", (PyCFunction)cFPGenerator::reduce, METH_VARARGS,
   "returns a value equal (mod polynomial) to fp and of degree less than "
   "degree."},
  {NULL}
};

static PyTypeObject cFPGeneratorType = {
  PyObject_HEAD_INIT(NULL)
  0, /* ob_size; unused, must be 0 for binary compatibility with older versions */
  "cfpgenerator.FPGenerator", /* tp_name */
  sizeof(cFPGenerator), /* tp_basicsize */
  0, /* tp_itemsize */
  0, /* tp_dealloc */
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
  "FPGenerator in C", /* tp_doc */
  0, /* tp_traverse */
  0, /* tp_clear */
  0, /* tp_richcompare */
  0, /* tp_weaklistoffset */
  0, /* tp_iter */
  0, /* tp_iternext */
  cFPGenerator_methods, /* tp_methods */
  0, //cFPGenerator_members, /* tp_members */
  0, /* tp_getset */
  0, /* tp_base */
  0, /* tp_dict */
  0, /* tp_desc_get */
  0, /* tp_desc_set */
  0, /* tp_dictoffset */
  (initproc)cFPGenerator::init, /* tp_init */
  0, /* tp_alloc */
  0, /* tp_new */
};

static PyMethodDef module_methods[] = {
  {NULL}
};

extern "C"
PyMODINIT_FUNC
initcfpgenerator(void) {
  cFPGeneratorType.tp_new = PyType_GenericNew;
  if (PyType_Ready(&cFPGeneratorType) < 0)
    return;

  PyObject *m = Py_InitModule3("cfpgenerator", module_methods,
			       "FPGenerator written in C");
  if (m == NULL) return;
  
  Py_INCREF(&cFPGeneratorType);
  PyModule_AddObject(m, "FPGenerator", (PyObject*)&cFPGeneratorType);
}
