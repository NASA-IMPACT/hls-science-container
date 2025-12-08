TGT = sentinel-add-fmask-sds
OBJ = addFmaskSDS.o \
	hls_projection.o \
	s2r.o \
	util.o \
	hdfutility.o \
	hls_hdfeos.o \
	dilation.o \

$(TGT): $(OBJ)
	$(CC) $(CFLAGS) -o $(TGT) $(OBJ) -L$(GCTPLIB) -L$(HDFLIB) -L$(ZLIBLIB) -L$(SZIPLIB) -L$(JPEGLIB) -L$(PROJLIB) $(GCTPLINK) $(HDFLINK)

addFmaskSDS.o: addFmaskSDS.c
	$(CC) $(CFLAGS) -c addFmaskSDS.c -I$(HDFINC) -I$(GCTPINC) -I$(COMMON_DIR)

hls_projection.o: ${COMMON_DIR}/hls_projection.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/hls_projection.c -I$(HDFINC) -I$(GCTPINC) -I$(COMMON_DIR)

s2r.o: ${COMMON_DIR}/s2r.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/s2r.c -I$(HDFINC) -I$(GCTPINC) -I$(COMMON_DIR)

util.o: ${COMMON_DIR}/util.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/util.c -I$(HDFINC) -I$(COMMON_DIR)

hdfutility.o: ${COMMON_DIR}/hdfutility.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/hdfutility.c -I$(HDFINC) -I$(COMMON_DIR)

hls_hdfeos.o: ${COMMON_DIR}/hls_hdfeos.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/hls_hdfeos.c -I$(HDFINC) -I$(COMMON_DIR)

dilation.o: dilation.c
	$(CC) $(CFLAGS) -c  dilation.c -I$(HDFINC) -I$(GCTPINC) -I$(COMMON_DIR)

install:
	install -d ${PREFIX}/bin
	install -m 755 $(TGT) ${PREFIX}/bin

clean:
	rm -f *.o $(TGT)