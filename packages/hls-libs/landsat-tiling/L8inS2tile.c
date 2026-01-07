/* Grid a Landsat scene into an ovelapping Sentinel-2 tile. Both systems use
 * UTM, but the UTM zone numbers may be different. 
 *
 * If the output file does not exist yet, create it; otherwise, update it. 
 * An output file can pre-exist if L8 data from an adjacent row on the same
 * day has already been rendered into this tile. 
 * 
 * AROP fitting is available to register Landsat to Sentinel-2.
 *
 * Note that input Landsat data Y coordinate does not use a false northing for 
 * the southern hemisphere as the S2 does, but the output does.
 *
 * Sep 8, 2017
 * Aug 20, 2020
 * Dec 3, 2020
 * Exit code:
 *  0: Always return 0 if there is no error. A Landsat scene is attempted  on a given
 *     MGRS tile and empty file is created initially. If the gridding process finds
 *     out that there is no overlap at all, the empty file will be deleted in the end,
 *     and in this case the code still return 0.
 *     Before 20 Aug 2020 the code returned 1 for the non-overlapping case and it caused 
 *     some confusion for the IMPACT team; changed to 0 and they will look out for
 *     the message of empty file deletion.
 *  non-zero: other errors. 
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "lsat.h"
#include "hls_commondef.h"
#include "hls_projection.h"
#include "resample_lsat.h"
#include "read_arop_output.h"
#include "error.h"
#include "lsatmeta.h"
#include "util.h"

char DEBUG = 0;

/* At this pixel, not all bands are fill value */
int ispixvalid(lsat_t *lsat, int irow, int icol);

int main(int argc, char *argv[])
{
	char s2tileid[20];		/* The output S2 tile id. */
	double s2ulx;		
	double s2uly;			/* The uly of S2 uses an false northing for s. hemisphere */ 
	char fname_s2ref[LINELEN];	/* S2 reference used in AROP, an attribute to be written to hdf */
	char fname_aropstdout[LINELEN];	/* AROP output to stdout, captured in a file*/
	char fname_in[LINELEN];		/* Input Landsat in path/row */
	char fname_out[LINELEN];	/* Output L8 in S2 tiles */

	char s2zonehem[10]; 	/* UTM zone number and the hemisphere spec. e.g. 13S,  13N*/
	int l8zone, s2zone;

	lsat_t lsatin, lsatout;
	lsatpix_t lsatpix;			/* One L8 pixel */

	double coeff[2][3]; /* AROP fitted 1st order polynomial coeff */
	int ncp;	    /* Number of control point in AROP */
	double rmse; 	   
	double avexshift, aveyshift; /* The average amounts to add to warp image coordinates based on AROP */
	double lsatcol, lsatrow;
	char arop_valid;	/* Whether valid AROP fitting is available */

	int irow, icol;
	double lsatx, lsaty;
	double s2x, s2y;
	double warpulx, warpuly;
	double s2ulyGCTP;   	/* In GCTP convention */
	int ib;
	int k;
	int ret;
	char *pos;
	char outputfile_is_new;	/* A new output file to be created,  or an existing one to be updated. */
	char disjoint; 		/* Whether the data portion of the scene overlaps with the tile at all */

	
	lsatmeta_t metascene; 

	intn access_mode;

	char message[MSGLEN];

	if (argc != 8) {
		fprintf(stderr, "%s s2tileid s2ulx s2uly s2ref aropstdout lsat_in lsat_out\n", argv[0]);
		exit(10);
	}

	strcpy(s2tileid,         argv[1]);
	s2ulx  =            atof(argv[2]);
	s2uly  =            atof(argv[3]);
	strcpy(fname_s2ref,      argv[4]);  /* S2 reference image */
	strcpy(fname_aropstdout, argv[5]);
	strcpy(fname_in,         argv[6]);
	strcpy(fname_out,        argv[7]);
	
	/* Read the input */
	strcpy(lsatin.fname, fname_in);
	strcpy(lsatin.inpathrow, INWRSPATHROW); 
	ret = open_lsat(&lsatin, DFACC_READ);
	if (ret != 0) {
		Error("Error in open_lsat()");
		exit(10);
	}
	fprintf(stderr, "\nzonehem, ulx, uly = %s %.0lf %.0lf\n", lsatin.zonehem, lsatin.ulx, lsatin.uly);

	ret = get_input_metadata(&lsatin, &metascene);
	if (ret != 0) {
		sprintf(message, "Error in get_input_metadata for %s", lsatin.fname);
		Error(message);
		exit(10);
	}

	/* Create output */
	s2zone = atoi(s2tileid);
	sprintf(s2zonehem, "%02d%s",  s2zone, s2tileid[2] >= 'N'?  "N" : "S"); 
	strcpy(lsatout.fname, fname_out);
	strcpy(lsatout.zonehem, s2zonehem);
	lsatout.ulx = s2ulx;
	if (strstr(s2zonehem, "S") && s2uly > 0) 	/* Oct 3, 2018: No false northing; GCTP convention */ 	
		lsatout.uly = s2uly - pow(10,7);
	else
		lsatout.uly = s2uly;

	lsatout.nrow = S2TILESZ/HLS_PIXSZ;	/* S2 tile of 30-m pixels */
	lsatout.ncol = lsatout.nrow;
	outputfile_is_new = 1;
	if (! file_exist(fname_out)) {
		ret = open_lsat(&lsatout, DFACC_CREATE);
		if (ret != 0) {
			Error("Error in open_lsat()");
			exit(10);
		}
		ret = copy_input_metadata_to_tile(&lsatout, &metascene);   /* ULX, ULY are updated here. Oct 4, 2018*/
		if (ret != 0) {
			Error("Error in copy_input_metadata_to_tile");
			exit(20);
		}

		/* Set S2 tileid as an attribute */
		SDsetattr(lsatout.sd_id, L_S2TILEID, DFNT_CHAR8, strlen(s2tileid), (VOIDP)s2tileid);
	}
	else {
		outputfile_is_new = 0;
		ret = open_lsat(&lsatout, DFACC_WRITE);
		if (ret != 0) {
			Error("Error in open_lsat()");
			exit(10);
		}
		ret = update_input_metadata_for_tile(&lsatout, &metascene);
		if (ret != 0) {
			Error("Error in update_input_metadata_for_tile");
			exit(20);
		}
	}

	
	/* Oct 3, 2018: Use GCTP convention. s2ulyGCTP has been updated earlier*/
//	if (strstr(s2zonehem, "S"))
//		s2ulyGCTP = s2uly - 10000000;	/* Accommodate GCTP. But the output header uses the standard */
//	else
//		s2ulyGCTP = s2uly;
	s2ulyGCTP = lsatout.uly;

	l8zone = atoi(lsatin.zonehem);
	if (l8zone == s2zone) {
		//fprintf(stderr, "zone, ulx, uly = %s %.0lf %.0lf\n", lsatout.zonehem, lsatout.ulx, lsatout.uly);
		double ydiff;
		ydiff = (s2ulyGCTP-lsatin.uly);
		fprintf(stderr, "ulx.diff, uly.diff = %lf, %lf\n", (lsatout.ulx-lsatin.ulx)/30, ydiff/30);
	}
	else {
		//fprintf(stderr, "Zone different. %d, %d.  Reproject.\n", l8zone, s2zone);
		fprintf(stderr, "Zone different. Resample from the input zone \n");
	}

	/* Open the AROP return */
	arop_valid = 1;
	if (strcmp(fname_aropstdout, "NONE") == 0) { /* No valid AROP fitting*/
		warpulx = lsatin.ulx;
		warpuly = lsatin.uly;
		arop_valid = 0;
		ncp = 0;
		rmse = 0;
		avexshift = 0;
		aveyshift = 0;
	}
	else {
		double newwarpulx, newwarpuly;

		/* The AROP uses the GCTP convention regarding southern hemisphere */
		ret = read_aropstdout(fname_aropstdout, 
					&warpulx, 
					&warpuly,
					&newwarpulx,
				     	&newwarpuly,
					&ncp,
					&rmse,
					coeff);
		// Sep 18, 2017: revert to the original. If only a few GCP are available, they are unreliable.
		if (ret != 0 || ncp < MINNCP) {
			arop_valid = 0;

			ncp = 0;
			rmse = 0;
			avexshift = 0;
			aveyshift = 0;
		}
		else {
			avexshift = warpulx - newwarpulx;	/* only used for HDF attributes, not for calculation */
			aveyshift = warpuly - newwarpuly;
		}
	}

	if (outputfile_is_new) 
		lsatout.tile_has_data = 0;	
	else
		lsatout.tile_has_data = 1;

	disjoint = 1;    /* Assuming no overlap between the scene and tile*/ 
	double xinzone, yinzone; /* x, y in the input zone, which is different from S2 zone */ 

	/* AROP derives the polynomial (1) based on aggregated, 30m Sentinel pixel, (2) for
	 * the UTM that the reference image is in, which can be different from the input 
	 * Landsat UTM zone. If the Landsat image is not in the Sentinel-2 zone, AROP reproject
	 * the image into that zone first, and the derived the relationship is
	 * for the reference image and the reprojected Landsat; location in the reprojected
	 * image will be found in the input image which is in a different UTM zone.  
	 * Nov 16, 2017.
	 */	
	/* irow, icol refer to the pixels in the reference image space, i.e., the S2 tile */
	for (irow = 0; irow < lsatout.nrow; irow++) {
		s2y = s2ulyGCTP  - (irow+0.5) * HLS_PIXSZ;
		for (icol = 0; icol < lsatout.ncol; icol++) {
			/* If there are already data this output pixel, no need to continue to look; 
			 * the data have been retrieved from the adjacent WRS-2 row of the same day,
			 * which are identical for the two adjacent rows.
			 */
			if (ispixvalid(&lsatout, irow, icol))
			 	/* Aug 20, 2020: But there is chance that variable disjoint won't be
				 * reset to 0, if "continue" here. But anyway, "disjoint" value is 
				 * only used by my script and not needed for global production. */
				continue;

			s2x = s2ulx + (icol+0.5) * HLS_PIXSZ;
			if (arop_valid) {
				/* Predicted col, row for the warp image (landsat) */
				lsatcol = coeff[0][0] + coeff[0][1] * (icol+0.5) + coeff[0][2] * (irow+0.5);
				lsatrow = coeff[1][0] + coeff[1][1] * (icol+0.5) + coeff[1][2] * (irow+0.5);

				/* Before AROP fits the model, if L8 is not in the UTM zone of S2, it
				 * is reprojected into that zone, and warpulx/warpuly refer to the UL of the
				 * reprojected image. If same zone, warpulx/warpuly are the UL of input.
				 */
				lsatx = warpulx + lsatcol * LS_PIXSZ;  
				lsaty = warpuly - lsatrow * LS_PIXSZ;  
			}
			else {
				lsatx = s2x;
				lsaty = s2y;
			}


			if (l8zone != s2zone) {
				/* Get the corresponding Landsat x/y in the input Landsat utm zone */
				utm2utm(s2zone, lsatx, lsaty, l8zone, &xinzone, &yinzone);
				lsatx = xinzone;
				lsaty = yinzone;
			}

			/* Return 0 if there are data for this pixel location */
			if (resample_lsat(lsatx, lsaty, &lsatin, &lsatpix) != 0) 
				continue;

			/* Copy the pixel values into the output tile */ 
			k = irow * lsatout.ncol + icol;
			for (ib = 0; ib < L8NRB; ib++)
				lsatout.ref[ib][k] = lsatpix.ref[ib];
			for (ib = 0; ib < L8NTB; ib++)
				lsatout.thm[ib][k] = lsatpix.thm[ib];
			lsatout.acmask[k] = lsatpix.acmask;
			lsatout.fmask[k] =  lsatpix.fmask;
			
			lsatout.tile_has_data = 1;
			disjoint = 0;
		}
	}
	close_lsat(&lsatin);

	/* Write AROP attributes */
	if (outputfile_is_new) 
		set_arop_metadata(&lsatout, fname_s2ref, ncp, rmse, avexshift, aveyshift); 
	else {
		update_arop_metadata(&lsatout, fname_s2ref, ncp, rmse, avexshift, aveyshift); 
	}

	close_lsat(&lsatout);

	return(0);
}

/* At this pixel, not all bands have fill value */
int ispixvalid(lsat_t *lsat, int irow, int icol)
{
	int ib, k;
	int valid;
	k = irow * lsat->ncol + icol;
	
	valid = 0;
	/* Apr 12, 2017: 
	 * The output from Eric code for cirrus band does not use a fill value; so just random
	 * stuff outside the footprint. Do not check on cirrus for now (indix is L8NRB-1)
	 *   for (ib = 0; ib < L8NRB; ib++) {
	 */
	for (ib = 0; ib < L8NRB-1; ib++) {
		if (lsat->ref[ib][k] > HLS_REFL_FILLVAL) {
			valid = 1;
			break;
		}
	}

	return(valid);
}
