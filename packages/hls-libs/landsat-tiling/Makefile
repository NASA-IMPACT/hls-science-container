TGT = landsat-tile
OBJ = L8inS2tile.o \
	hls_projection.o \
	lsat.o \
	lsatmeta.o \
	util.o \
	hdfutility.o \
	read_arop_output.o \
	resample_lsat.o \
	cubic_conv.o

$(TGT): $(OBJ)
	$(CC) $(CFLAGS) -o $(TGT) $(OBJ) -L$(GCTPLIB) -L$(HDFLIB) -L$(ZLIB) -L$(SZLIB) -L$(JPGLIB) -L$(PROJLIB)  $(GCTPLINK) $(HDFLINK)

L8inS2tile.o: L8inS2tile.c
	$(CC) $(CFLAGS) -c L8inS2tile.c -I$(HDFINC) -I$(GCTPINC) -I$(COMMON_DIR)

hls_projection.o: ${COMMON_DIR}/hls_projection.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/hls_projection.c -I$(GCTPINC) -I$(HDFINC) -I$(COMMON_DIR)

lsat.o: ${COMMON_DIR}/lsat.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/lsat.c -I$(HDFINC)  -I$(COMMON_DIR)

lsatmeta.o: ${COMMON_DIR}/lsatmeta.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/lsatmeta.c -I$(HDFINC) -I$(COMMON_DIR)

util.o: ${COMMON_DIR}/util.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/util.c -I$(HDFINC) -I$(COMMON_DIR)

hdfutility.o: ${COMMON_DIR}/hdfutility.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/hdfutility.c -I$(HDFINC) -I$(COMMON_DIR)

read_arop_output.o: ${COMMON_DIR}/read_arop_output.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/read_arop_output.c -I$(HDFINC) -I$(COMMON_DIR)

resample_lsat.o: ${COMMON_DIR}/resample_lsat.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/resample_lsat.c -I$(HDFINC) -I$(COMMON_DIR)

cubic_conv.o: ${COMMON_DIR}/cubic_conv.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/cubic_conv.c -I$(HDFINC) -I$(COMMON_DIR)

install:
	install -d ${PREFIX}/bin
	install -m 755 $(TGT) ${PREFIX}/bin

clean:
	rm -f *.o
