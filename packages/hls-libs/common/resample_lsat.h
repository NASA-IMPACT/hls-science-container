#ifndef RESAMPLEL8_H
#define RESAMPLEL8_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "lsat.h"
#include "cubic_conv.h"

/* Return 0 for success, non-zero for failure (out of bound) */
int resample_lsat(double utmx,
		double utmy,
		lsat_t *l8,
		lsatpix_t *l8pix);

#endif 
