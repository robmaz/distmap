/* $Id: oligoindex_hr.h 180701 2015-12-10 19:54:31Z twu $ */
#ifndef OLIGOINDEX_HR_INCLUDED
#define OLIGOINDEX_HR_INCLUDED

#include "bool.h"
#include "types.h"
#include "mode.h"
#include "genomicpos.h"
#include "list.h"
#include "diagpool.h"


#define OVERABUNDANCE_CHECK 50
#define OVERABUNDANCE_PCT 0.97
#define OVERABUNDANCE_MIN 200


#ifdef HAVE_AVX2
/* Attempted to use int, so we don't need to check for count > 255.  However, SIMD is much faster on bytes than on ints */
typedef int Count_T;
typedef unsigned int Inquery_T;
#define INQUERY_FALSE 0x00000000
#define INQUERY_TRUE  0xFFFFFFFF
#define SIMD_NELTS 8		/* 8 ints in 256 bits */

/* #define CHECK_FOR_OVERFLOW 1 -- Optional if we use int for Count_T */
#define CHECK_FOR_OVERFLOW 1

#ifdef CHECK_FOR_OVERFLOW
#define MAXCOUNT 255
#define INCR_COUNT(counts,inquery) if (++counts > MAXCOUNT) inquery = INQUERY_FALSE;
#else
#define INCR_COUNT(counts,inquery) counts += 1;
#endif


#elif defined(HAVE_SSE2)
typedef char Count_T;
typedef unsigned char Inquery_T;
#define INQUERY_FALSE 0x00
#define INQUERY_TRUE  0xFF
#define SIMD_NELTS 16		/* 16 bytes in 128 bits */

#define CHECK_FOR_OVERFLOW 1	/* Required, since a char can hold only 127 positive counts */
#ifdef CHECK_FOR_OVERFLOW
#define INCR_COUNT(counts,inquery) if (++counts < 0) inquery = INQUERY_FALSE;
#else
#define INCR_COUNT(counts,inquery) counts += 1;
#endif

#else
typedef char Count_T;
typedef bool Inquery_T;
#define INQUERY_FALSE false
#define INQUERY_TRUE true

#define CHECK_FOR_OVERFLOW 1	/* Required, since a char can hold only 127 positive counts */
#ifdef CHECK_FOR_OVERFLOW
#define INCR_COUNT(counts,inquery) if (++counts < 0) inquery = false;
#else
#define INCR_COUNT(counts,inquery) counts += 1;
#endif

#endif


#define T Oligoindex_T
typedef struct T *T;

typedef struct Oligoindex_array_T *Oligoindex_array_T;

extern void
Oligoindex_hr_setup (Genomecomp_T *ref_blocks_in, Mode_T mode_in);

extern int
Oligoindex_indexsize (T this);

extern int
Oligoindex_array_length (Oligoindex_array_T oligoindices);
extern T
Oligoindex_array_elt (Oligoindex_array_T oligoindices, int source);

extern Oligoindex_array_T
Oligoindex_array_new_major (int max_querylength, int max_genomiclength);

extern Oligoindex_array_T
Oligoindex_array_new_minor (int max_querylength, int max_genomiclength);

extern double
Oligoindex_set_inquery (int *badoligos, int *repoligos, int *trimoligos, int *trim_start, int *trim_end,
			T this, char *queryuc_ptr, int querystart, int queryend, bool trimp);
extern void
Oligoindex_hr_tally (T this, Univcoord_T mappingstart, Univcoord_T mappingend, bool plusp,
		     char *queryuc_ptr, int querystart, int queryend, Chrpos_T chrpos, int genestrand);
extern void
Oligoindex_untally (T this, char *queryuc_ptr, int querylength);
extern void
Oligoindex_clear_inquery (T this, char *queryuc_ptr, int querystart, int queryend);
extern void
Oligoindex_array_free(Oligoindex_array_T *old);

extern List_T
Oligoindex_get_mappings (List_T diagonals, bool *coveredp, Chrpos_T **mappings, int *npositions,
			 int *totalpositions, bool *oned_matrix_p, int *maxnconsecutive, 
			 Oligoindex_array_T array, T this, char *queryuc_ptr,
			 int querystart, int queryend, int querylength,
			 Chrpos_T chrstart, Chrpos_T chrend,
			 Univcoord_T chroffset, Univcoord_T chrhigh, bool plusp,
			 Diagpool_T diagpool);

#undef T
#endif

