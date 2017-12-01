/* $Id: sarray-read.h 184464 2016-02-18 00:09:13Z twu $ */
#ifndef SARRAY_READ_INCLUDED
#define SARRAY_READ_INCLUDED
#include "access.h"
#include "bool.h"
#include "mode.h"
#include "genome.h"
#include "compress.h"
#include "genomicpos.h"
#include "splicetrie.h"
#include "iit-read-univ.h"


#define T Sarray_T
typedef struct T *T;

/* For benchmarking */
Univcoord_T
Sarray_size (Sarray_T this);

extern void
Sarray_setup (T sarray_fwd_in, T sarray_rev_in, Genome_T genome_in, Mode_T mode,
	      Univ_IIT_T chromosome_iit_in, int circular_typeint_in, bool *circularp_in,
	      Chrpos_T shortsplicedist_in, int splicing_penalty_in,
	      int max_deletionlength, int max_end_deletions,
	      int max_middle_insertions_in, int max_end_insertions,
	      Univcoord_T *splicesites_in, Splicetype_T *splicetypes_in,
	      Chrpos_T *splicedists_in, int nsplicesites_in);

#if 0
extern void
Sarray_shmem_remove (char *dir, char *fileroot, char *snps_root, Mode_T mode, bool fwdp);
#endif

extern T
Sarray_new (char *dir, char *fileroot, Access_mode_T sarray_access, Access_mode_T lcp_access,
	    Access_mode_T guideexc_access, Access_mode_T indexij_access, bool sharedp, Mode_T mode, bool fwdp);
extern void
Sarray_free (T *old);

extern List_T
Sarray_search_greedy (int *found_score, char *queryuc_ptr, char *queryrc, int querylength,
		      Compress_T query_compress_fwd, Compress_T query_compress_rev,
		      int nmisses_allowed, int genestrand);

#undef T
#endif


