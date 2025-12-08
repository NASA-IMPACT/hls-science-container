TGT = landsat-add-fmask-sds
OBJ = addFmaskSDS.o \
		  dilation.o \
		  lsat.o \
		  lsatmeta.o \
		  util.o \
		  hdfutility.o

$(TGT): $(OBJ)
	$(CC) $(CFLAGS) -o $(TGT) $(OBJ) -L$(GCTPLIB) -L$(HDFLIB) -L$(ZLIB) -L$(SZIPLIB) -L$(JPEGLIB) -L$(PROJLIB) $(GCTPLINK) $(HDFLINK)

addFmaskSDS.o: addFmaskSDS.c
	$(CC) $(CFLAGS) -c addFmaskSDS.c -I$(HDFINC) -I$(GCTPINC) -I$(COMMON_DIR)

dilation.o: ${COMMON_DIR}/dilation.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/dilation.c -I$(HDFINC) -I$(COMMON_DIR)

lsat.o: ${COMMON_DIR}/lsat.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/lsat.c -I$(HDFINC) -I$(COMMON_DIR)

lsatmeta.o: ${COMMON_DIR}/lsatmeta.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/lsatmeta.c -I$(HDFINC) -I$(COMMON_DIR)

util.o: ${COMMON_DIR}/util.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/util.c -I$(HDFINC) -I$(COMMON_DIR)

hdfutility.o: ${COMMON_DIR}/hdfutility.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/hdfutility.c -I$(HDFINC) -I$(COMMON_DIR)

install:
	install -d ${PREFIX}/bin
	install -m 755 $(TGT) ${PREFIX}/bin

clean:
	rm -f *.o $(TGT)