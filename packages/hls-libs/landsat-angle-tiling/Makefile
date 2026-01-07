TGT = landsat-angle-tile
OBJ = angle_tile.o \
	l8ang.o \
	hls_hdfeos.o \
	s2r.o \
	util.o \
	hls_projection.o \
	hdfutility.o \
	cubic_conv.o

$(TGT): $(OBJ)
	$(CC) $(CFLAGS) -o $(TGT) $(OBJ) -L$(GCTPLIB) -L$(HDFLIB) -L$(ZLIB) -L$(SZLIB) -L$(JPGLIB) -L$(PROJLIB)  $(GCTPLINK) $(HDFLINK)

angle_tile.o: angle_tile.c
	$(CC) $(CFLAGS) -c angle_tile.c -I$(HDFINC) -I$(GCTPINC) -I$(SRC_DIR)

l8ang.o: ${SRC_DIR}/l8ang.c
	$(CC) $(CFLAGS) -c  ${SRC_DIR}/l8ang.c -I$(HDFINC) -I$(SRC_DIR)

hls_hdfeos.o: ${SRC_DIR}/hls_hdfeos.c
	$(CC) $(CFLAGS) -c  ${SRC_DIR}/hls_hdfeos.c -I$(HDFINC) -I$(SRC_DIR)

s2r.o: ${SRC_DIR}/s2r.c
	$(CC) $(CFLAGS) -c  ${SRC_DIR}/s2r.c -I$(HDFINC) -I$(GCTPINC) -I$(SRC_DIR)

util.o: ${SRC_DIR}/util.c
	$(CC) $(CFLAGS) -c  ${SRC_DIR}/util.c -I$(HDFINC) -I$(SRC_DIR)

hls_projection.o: ${SRC_DIR}/hls_projection.c
	$(CC) $(CFLAGS) -c ${SRC_DIR}/hls_projection.c -I$(GCTPINC)  -I$(HDFINC) -I$(SRC_DIR)

hdfutility.o: ${SRC_DIR}/hdfutility.c
	$(CC) $(CFLAGS) -c  ${SRC_DIR}/hdfutility.c -I$(HDFINC) -I$(SRC_DIR)

cubic_conv.o: ${SRC_DIR}/cubic_conv.c
	$(CC) $(CFLAGS) -c ${SRC_DIR}/cubic_conv.c -I$(HDFINC) -I$(SRC_DIR)

install:
	install -d ${PREFIX}/bin
	install -m 755 $(TGT) ${PREFIX}/bin

clean:
	rm -f *.o