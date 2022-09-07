/* Generated by Cython 0.29.26 */

#ifndef __PYX_HAVE__recordclass___linkedlist
#define __PYX_HAVE__recordclass___linkedlist

#include "Python.h"
struct LinkedItem;
struct LinkedList;
struct DLinkedItem;
struct DLinkedList;

/* "recordclass/_linkedlist.pyx":36
 * @cython.no_gc
 * @cython.final
 * cdef public class linkeditem[object LinkedItem, type LinkedItemType]:             # <<<<<<<<<<<<<<
 *     cdef object val
 *     cdef linkeditem next
 */
struct LinkedItem {
  PyObject_HEAD
  PyObject *val;
  struct LinkedItem *next;
};

/* "recordclass/_linkedlist.pyx":41
 * 
 * @cython.final
 * cdef public class linkedlist[object LinkedList, type LinkedListType]:             # <<<<<<<<<<<<<<
 *     cdef public linkeditem start
 *     cdef public linkeditem end
 */
struct LinkedList {
  PyObject_HEAD
  struct __pyx_vtabstruct_11recordclass_11_linkedlist_linkedlist *__pyx_vtab;
  struct LinkedItem *start;
  struct LinkedItem *end;
};

/* "recordclass/_linkedlist.pyx":115
 * @cython.no_gc
 * @cython.final
 * cdef public class dlinkeditem[object DLinkedItem, type DLinkedItemType]:             # <<<<<<<<<<<<<<
 *     cdef object val
 *     cdef linkeditem next
 */
struct DLinkedItem {
  PyObject_HEAD
  PyObject *val;
  struct LinkedItem *next;
  struct LinkedItem *prev;
};

/* "recordclass/_linkedlist.pyx":121
 * 
 * @cython.final
 * cdef public class dlinkedlist[object DLinkedList, type DLinkedListType]:             # <<<<<<<<<<<<<<
 *     cdef public dlinkeditem start
 *     cdef public dlinkeditem end
 */
struct DLinkedList {
  PyObject_HEAD
  struct __pyx_vtabstruct_11recordclass_11_linkedlist_dlinkedlist *__pyx_vtab;
  struct DLinkedItem *start;
  struct DLinkedItem *end;
};

#ifndef __PYX_HAVE_API__recordclass___linkedlist

#ifndef __PYX_EXTERN_C
  #ifdef __cplusplus
    #define __PYX_EXTERN_C extern "C"
  #else
    #define __PYX_EXTERN_C extern
  #endif
#endif

#ifndef DL_IMPORT
  #define DL_IMPORT(_T) _T
#endif

__PYX_EXTERN_C DL_IMPORT(PyTypeObject) LinkedItemType;
__PYX_EXTERN_C DL_IMPORT(PyTypeObject) LinkedListType;
__PYX_EXTERN_C DL_IMPORT(PyTypeObject) DLinkedItemType;
__PYX_EXTERN_C DL_IMPORT(PyTypeObject) DLinkedListType;

#endif /* !__PYX_HAVE_API__recordclass___linkedlist */

/* WARNING: the interface of the module init function changed in CPython 3.5. */
/* It now returns a PyModuleDef instance instead of a PyModule instance. */

#if PY_MAJOR_VERSION < 3
PyMODINIT_FUNC init_linkedlist(void);
#else
PyMODINIT_FUNC PyInit__linkedlist(void);
#endif

#endif /* !__PYX_HAVE__recordclass___linkedlist */
