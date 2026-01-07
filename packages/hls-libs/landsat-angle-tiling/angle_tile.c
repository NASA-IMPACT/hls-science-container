/* Grid the solar and view angle of a Landsat scene into an ovelapping 
 * Sentinel-2 tile. Both systems use UTM, but the zone numbers may be different. 
 * 
 * A nearest-neighbor resampling is sufficient. The sub-pixel geolocation error of 
 * L8 does not matter; even for reflectance, AROP is no longer needed on Collection-2 
 * data.
 * 
 * Note that Landsat data Y coordinate does not use a false northing for 
 * the southern hemisphere as the S2 does.
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "l8ang.h"
#include "hls_hdfeos.h"
#include "hls_commondef.h"
#include "hls_projection.h"
#include "error.h"
#include "util.h"

int main(int argc, char *argv[])
{
	char s2tileid[20];		/* The output S2 tile id. */
	double s2ulx;			
	double s2uly;	/* The uly of S2 observing the false northing standard for southern hemisphere */ 
	char fname_sz[LINELEN];		/* solar zenith file in scene */
	char fname_out[LINELEN];	/* four angle SDS in tile */

	char s2zonehem[10]; 	/* UTM zone number and the hemisphere spec. e.g. 13S,  13N*/
	int l8zone, s2zone;

	l8ang_t angin; 	/* Angle in scene */ 
	l8ang_t angout;	/* Angle in tile */

	int irow, icol;
	double lsatx, lsaty;
	double s2x, s2y;
	/* For southern hemisphere, use GCTP convention in resampling input; output is still in S2 standard */
	double s2ulyGCTP;   	
	int ib;
	int k;
	int ret;
	char *pos;

	char update_outfile;    /* Indicator whether update or create new */
	char l1tsceneid[50];
	char message[MSGLEN];

	if (argc != 6) {
		fprintf(stderr, "Usage: %s s2tileid s2ulx s2uly SZangScene angTile \n", argv[0]);
		exit(1);
	}

	strcpy(s2tileid,         argv[1]);
	s2ulx  =            atof(argv[2]);
	s2uly  =            atof(argv[3]);
	strcpy(fname_sz,         argv[4]);
	strcpy(fname_out,        argv[5]);
	
	/* Read the scene-based input */
	ret = read_l8ang_inpathrow(&angin, fname_sz);
	if (ret != 0) {
		Error("Error in open_l8ang()");
		exit(1);
	}

	/* Create tile-based output */
	s2zone = atoi(s2tileid);
	sprintf(s2zonehem, "%02d%s",  s2zone, s2tileid[2] >= 'N'?  "N" : "S"); 
	strcpy(angout.fname, fname_out);
	strcpy(angout.zonehem, s2zonehem);
	angout.ulx = s2ulx;
	angout.uly = s2uly;
	angout.nrow = S2TILESZ/HLS_PIXSZ;	/* S2 tile of 30-m pixels */
	angout.ncol = angout.nrow;
	if (file_exist(fname_out)) {
		update_outfile = 1;
		ret = open_l8ang(&angout, DFACC_WRITE);
		if (ret != 0) {
			Error("Error in open_l8ang()");
			exit(1);
		}
	}
	else {
		update_outfile = 0;
		ret = open_l8ang(&angout, DFACC_CREATE);
		if (ret != 0) {
			Error("Error in open_l8ang()");
			exit(1);
		}
	}
	
	if (strstr(s2zonehem, "S"))
		s2ulyGCTP = s2uly - 1E7;	/* Just accommodate GCTP. The output header uses the standard */
	else
		s2ulyGCTP = s2uly;

	l8zone = atoi(angin.zonehem);

	if (update_outfile) 
		angout.tile_has_data = 1;
	else
		angout.tile_has_data = 0;
	int lsatrow, lsatcol;
	int kin, kout;
	double xinzone, yinzone; /* x, y in the input zone, which may be different from S2 zone */ 
	for (irow = 0; irow < angout.nrow; irow++) {
		s2y = s2ulyGCTP  - (irow+0.5) * HLS_PIXSZ;
		for (icol = 0; icol < angout.ncol; icol++) {
			s2x = s2ulx + (icol+0.5) * HLS_PIXSZ;

			if (l8zone != s2zone) {
				utm2utm(s2zone, s2x, s2y, l8zone, &xinzone, &yinzone);
				lsatx = xinzone;
				lsaty = yinzone;
			}
			else {
				lsatx = s2x;
				lsaty = s2y;
			}

			lsatrow = floor((angin.uly - lsaty) / HLS_PIXSZ); 
			lsatcol = floor((lsatx - angin.ulx) / HLS_PIXSZ); 
			if (lsatrow < 0 || lsatrow > angin.nrow-1 || lsatcol < 0 || lsatcol > angin.ncol-1)
				continue;
	
			kin = lsatrow * angin.ncol + lsatcol;
			kout = irow * angout.ncol + icol;
			
			if (angin.sz[kin] != ANGFILL ) { 
				angout.sz[kout] = angin.sz[kin];
				angout.sa[kout] = angin.sa[kin];

				angout.vz[kout] = angin.vz[kin];
				angout.va[kout] = angin.va[kin];
				
				angout.tile_has_data = 1;
			}
		}
	}

	/* Set L1T scene ID as metadata. 
	 * If the output file already exists, append the current scene ID to the previous one 
	 */

	pos = strrchr(fname_sz, '/');
	if (pos == NULL)
		pos = fname_sz;
	else 
		pos++;
	strcpy(l1tsceneid, pos);
	l8ang_add_l1tsceneid(&angout, l1tsceneid);

	/* Close. If the output file is empty, it will be deleted during closing */
	close_l8ang(&angout);


	/* Make angle HDF-EOS only if it is opened for the first time, and 
	 * if it is non-empty and therefore hasn't been deleted */
	if (update_outfile == 0 && file_exist(angout.fname)) {
		int NSDS = 4;	 	/* Number of angle SDS */
		sds_info_t all_sds[NSDS];
        	set_L8ang_sds_info(all_sds, NSDS, &angout);
        	ret = angle_PutSpaceDefHDF(angout.fname, all_sds, NSDS);
        	if (ret != 0) {
                	Error("Error in angle_PutSpaceDefHDF");
                	exit(1);
		}
        }

	return 0;
}
