/* $Id: univdiag.h 196273 2016-08-12 15:15:06Z twu $ */
#ifndef UNIVDIAG_INCLUDED
#define UNIVDIAG_INCLUDED

#include "bool.h"
#include "list.h"
#include "genomicpos.h"
#include "types.h"

#define T Univdiag_T
typedef struct T *T;


extern T
Univdiag_new (int querystart, int queryend, Univcoord_T univdiagonal);
extern T
Univdiag_new_fillin (int querystart, int queryend, int indexsize, Univcoord_T univdiagonal);
extern void
Univdiag_free (T *old);
extern void
Univdiag_gc (List_T *list);

extern int
Univdiag_ascending_cmp (const void *a, const void *b);
extern int
Univdiag_descending_cmp (const void *a, const void *b);
extern int
Univdiag_diagonal_cmp (const void *a, const void *b);

#undef T
#endif


