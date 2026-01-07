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
	$(CC) $(CFLAGS) -c L8inS2tile.c -I$(HDFINC) -I$(GCTPINC) -I$(SRC_DIR)

hls_projection.o: ${SRC_DIR}/hls_projection.c
	$(CC) $(CFLAGS) -c  ${SRC_DIR}/hls_projection.c -I$(GCTPINC) -I$(HDFINC) -I$(SRC_DIR)

lsat.o: ${SRC_DIR}/lsat.c
	$(CC) $(CFLAGS) -c  ${SRC_DIR}/lsat.c -I$(HDFINC)  -I$(SRC_DIR)

lsatmeta.o: ${SRC_DIR}/lsatmeta.c
	$(CC) $(CFLAGS) -c  ${SRC_DIR}/lsatmeta.c -I$(HDFINC) -I$(SRC_DIR)

util.o: ${SRC_DIR}/util.c
	$(CC) $(CFLAGS) -c  ${SRC_DIR}/util.c -I$(HDFINC) -I$(SRC_DIR)

hdfutility.o: ${SRC_DIR}/hdfutility.c
	$(CC) $(CFLAGS) -c  ${SRC_DIR}/hdfutility.c -I$(HDFINC) -I$(SRC_DIR)

read_arop_output.o: ${SRC_DIR}/read_arop_output.c
	$(CC) $(CFLAGS) -c ${SRC_DIR}/read_arop_output.c -I$(HDFINC) -I$(SRC_DIR)

resample_lsat.o: ${SRC_DIR}/resample_lsat.c
	$(CC) $(CFLAGS) -c ${SRC_DIR}/resample_lsat.c -I$(HDFINC) -I$(SRC_DIR)

cubic_conv.o: ${SRC_DIR}/cubic_conv.c
	$(CC) $(CFLAGS) -c ${SRC_DIR}/cubic_conv.c -I$(HDFINC) -I$(SRC_DIR)

install:
	install -d ${PREFIX}/bin
	install -m 755 $(TGT) ${PREFIX}/bin

clean:
	rm -f *.o