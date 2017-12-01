/* $Id: mapq.h 184376 2016-02-16 23:39:30Z twu $ */
#ifndef MAPQ_INCLUDED
#define MAPQ_INCLUDED

#include "types.h"
#include "compress.h"
#include "genomicpos.h"


#define MAX_QUALITY_SCORE_INPUT 96	/* Was 40 */
#define MAX_QUALITY_SCORE 40

extern void
MAPQ_init (int quality_score_adj_in);
extern int
MAPQ_max_quality_score (char *quality_string, int querylength);
extern float
MAPQ_loglik (Compress_T query_compress, Univcoord_T left, int querystart, int queryend,
	     int querylength, char *quality_string, bool plusp, int genestrand);

#endif

