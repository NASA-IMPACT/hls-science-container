#include "resample_lsat.h"

extern char DEBUG;   /* debug flag */

/* Return 0 for success, non-zero for failure (out of bound, or reflectance for
 * any of the 16 neighbors is fill or less than fill) 
 *
 * Oct 4, 2017: reflectance less than fill value will cause failure (confusion?)
 *   and as a result, any reflectance value smaller fill vaule is treated as
 *   fill value. Nov 16, 2017
 */
int resample_lsat(double utmx,
		double utmy,
		lsat_t *lsat,
		lsatpix_t *lsatpix)
{
	int ib;
	int irow, icol;
	double subrow, subcol;	/* subpix location */
	int frow, fcol;		/* floor of the subpix location */
	double val[4][4];	/* to hold pixel values of any type */
	double xdis[4], ydis[4];	/* distance in the horizontal and vertical direction */
	double xw[4], yw[4];		/* weight */
	double conv;
	int bcol, ecol;
	int brow, erow;
	double tmp;
	char neighbor_has_fillval;	/* Any of the 16 neighbors has fill value*/

	/* subpixel location in the source image */
	subcol = (utmx - lsat->ulx)/LS_PIXSZ;
	subrow = (lsat->uly - utmy)/LS_PIXSZ;


	/* Look for the ranges of col/row for the 4x4 kernel */
	/* Floor(subcol) and floor(subrow)) give the nearest neighbor, but to 
	 * find the kernel, need know the the location of the target pixel in 
	 * relation to the nearest neighbor.
	 * Be careful here.  Nov 18, 2015.
	 */

	bcol = floor(subcol+0.5)-2;
	brow = floor(subrow+0.5)-2;

	ecol = bcol+3;
	erow = brow+3;

	if (bcol < 0 || ecol > lsat->ncol-1 || brow < 0 || erow > lsat->nrow-1)
		return -1;

	for (icol = bcol; icol <= ecol; icol++) 
		xdis[icol-bcol] = fabs(subcol - (icol+0.5));
	if (cc_weight(xdis, xw) != 0)
		return -1;

	for (irow = brow; irow <= erow; irow++) 
		ydis[irow-brow] = fabs(subrow - (irow+0.5));
	if (cc_weight(ydis, yw) != 0)
		return -1;

	/******************** Reflectance */
	for (ib = 0; ib < L8NRB; ib++) {
		neighbor_has_fillval = 0;
		for (irow = brow; irow <= erow; irow++) {
			for (icol = bcol; icol <= ecol; icol++) {
				 /* Apr 8, 2017: 
				  * If any neighbor is fill or smaller than fill, the result will be fill.
				  */
				tmp = lsat->ref[ib][irow*lsat->ncol+icol]; 
				if (tmp <= HLS_REFL_FILLVAL) 
					neighbor_has_fillval = 1;
				else 
					val[irow-brow][icol-bcol] = tmp;
			}
		}

		if (neighbor_has_fillval) 
			// Aug 29, 2019
			//lsatpix->ref[ib] = HLS_REFL_FILLVAL; 
			return -1;
		else {
			conv = cubic_conv(val, xw, yw);
			lsatpix->ref[ib] = conv;
		}
	}

	/******************** Thermal */
	for (ib = 0; ib < L8NTB; ib++) {
		neighbor_has_fillval = 0;
		for (irow = brow; irow <= erow; irow++) {
			for (icol = bcol; icol <= ecol; icol++) {
				tmp = lsat->thm[ib][irow*lsat->ncol+icol]; 
				if (tmp <= HLS_THM_FILLVAL)
					neighbor_has_fillval = 1;
				else
					val[irow-brow][icol-bcol] = tmp;
			}
		}

		if (neighbor_has_fillval) 
			// Aug 29, 2019
			// lsatpix->thm[ib] = HLS_THM_FILLVAL;
			return -1;
		else {
			conv = cubic_conv(val, xw, yw);
			lsatpix->thm[ib] = conv;
		}
	}

	/******************** ACmask and Fmask */
	/* A 4x4 kernel is too big for QA resampling??? */
	/* Try the inner 2x2 for the time being. A presence rule: if state 1 is present
	 * in any of the 2x2 pixel, set ouptut to 1.
	 * Nov 16, 2017: This is the rule used for Sentinel as well. This strict rule 
	 *  is general better than the majority rule, except for the water. It is ok to
	 *  use the water bit to discard data, but not to pick water for use.
	 *
 	 * Nov 16, 2017: Maybe it is okay to use an aggressive 3x3 window for QA.
	 *   try it later.
   	 */ 

	int N = 4; 	/* maximum 4 possilbe states, for the 2-bit group bits 6 and 7. */
	int freq[N];	
	int bit;
	int i,k;
	unsigned char mask, state;


	/* May 4, 2017 */
	// Aug 29, 2019. A useless test?  It would have returned before this point if condition is true.
	if (lsatpix->ref[0] == HLS_REFL_FILLVAL)	/* All other bands should be fill too*/
		//return 0; 	//Aug 29, 2019. 
		return -1;

	/*** ACmask ***/
	mask = 0;
	/* Bits 0-5 are individual bits */
	for (bit = 0; bit <= 5; bit++) {
		freq[1] = freq[0] = 0;
		/* The innermost 2 x 2 neighbors in the 4x4 cubic convolution kernel*/
		for (irow = brow+1; irow <= erow-1; irow++) {
			for (icol = bcol+1; icol <= ecol-1; icol++) {
				k = irow*lsat->ncol+icol;
				state = ((lsat->acmask[k] >> bit) & 01);
				freq[state]++;
			}
		}
		//state = (freq[1] >= freq[0]);   /* This is a majority rule. Oct 4, 2017 */
		if (freq[1] > 0)		  /* But I decided to use a presence rule */
			mask = (mask | (01 << bit));
	} 

	/* Bits 6-7 are a group on aerosol level, 4 states*/
	freq[3] = freq[2] = freq[1] = freq[0] = 0;
	/* Innermost 2 x 2 neighbors in the 4x4 cubic convolution kernel*/
	for (irow = brow+1; irow <= erow-1; irow++) {
		for (icol = bcol+1; icol <= ecol-1; icol++) {
			k = irow*lsat->ncol+icol;
			state = ((lsat->acmask[k] >> 6) & 03);
			freq[state]++;
		}
	}
	for (i = 0; i <= 3; i++) {
		if (freq[i] > 0) 	/* The presence of higher values of aerosol level override lower ones */
			state = i;
	}
	mask = (mask | (state << 6));

	/* Finally, all bits have been considered */
	lsatpix->acmask = mask;

	/*** Fmask ***/
	mask = 0;
	/* Bits 0-5 are individual bits */
	for (bit = 0; bit <= 5; bit++) {
		freq[1] = freq[0] = 0;
		/* 2 x 2 neighbors in the 4x4 cubic convolution kernel*/
		for (irow = brow+1; irow <= erow-1; irow++) {
			for (icol = bcol+1; icol <= ecol-1; icol++) {
				k = irow*lsat->ncol+icol;
				state = ((lsat->fmask[k] >> bit) & 01);
				freq[state]++;
			}
		}
		if (freq[1] > 0)		  
			mask = (mask | (01 << bit));
	} 

	/* Apr 12, 2021. Fmask also has the 2 bits of aerosol level, borrowed from USGS aerosol QA */ 
	/* Bit 6-7 are a group on aerosol, 4 states*/
	freq[3] = freq[2] = freq[1] = freq[0] = 0;
	/* The innermost 2 x 2 neighbors in the 4x4 cubic convolution kernel*/
	for (irow = brow+1; irow <= erow-1; irow++) {
		for (icol = bcol+1; icol <= ecol-1; icol++) {
			k = irow*lsat->ncol+icol;
			state = ((lsat->fmask[k] >> 6) & 03);
			freq[state]++;
		}
	}
	for (i = 0; i <= 3; i++) {
		if (freq[i] > 0) 	/* The presence of higher values of aerosol level override lower ones */
			state = i;
	}
	mask = (mask | (state << 6));

	/* Finally, all bits have been considered */
	lsatpix->fmask = mask;


	return 0;
}
