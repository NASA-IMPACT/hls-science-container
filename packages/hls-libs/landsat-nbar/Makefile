TGT = landsat-derive-l8nbar
OBJ = derive_l8nbar.o \
	lsat.o \
	l8ang.o \
	hls_projection.o \
	mean_solarzen.o \
	local_solar.o \
	rtls.o \
	hdfutility.o \
	util.o \
	cubic_conv.o \
	cfactor.o \
	hls_hdfeos.o \
	s2r.o \
  lsatmeta.o

$(TGT): $(OBJ)
	$(CC) $(CFLAGS) -o $(TGT) $(OBJ) -L$(GCTPLIB) -L$(HDFLIB) -L$(ZLIB) -L$(SZLIB) -L$(JPGLIB) -L$(PROJLIB) $(GCTPLINK) $(HDFLINK)

derive_l8nbar.o: derive_l8nbar.c
	$(CC) $(CFLAGS) -c derive_l8nbar.c -I$(HDFINC) -I$(GCTPINC) -I$(SRC_DIR)

lsat.o: ${SRC_DIR}/lsat.c
	$(CC) $(CFLAGS) -c ${SRC_DIR}/lsat.c -I$(GCTPINC) -I$(HDFINC) -I$(SRC_DIR)

l8ang.o: ${SRC_DIR}/l8ang.c
	$(CC) $(CFLAGS) -c  ${SRC_DIR}/l8ang.c -I$(HDFINC) -I$(SRC_DIR)

hls_projection.o: ${SRC_DIR}/hls_projection.c
	$(CC) $(CFLAGS) -c ${SRC_DIR}/hls_projection.c -I$(GCTPINC) -I$(HDFINC) -I$(SRC_DIR)

mean_solarzen.o: ${SRC_DIR}/mean_solarzen.c
	$(CC) $(CFLAGS) -c ${SRC_DIR}/mean_solarzen.c -I$(GCTPINC) -I$(HDFINC) -I$(SRC_DIR)

local_solar.o: ${SRC_DIR}/local_solar.c
	$(CC) $(CFLAGS) -c ${SRC_DIR}/local_solar.c -I$(GCTPINC) -I$(HDFINC) -I$(SRC_DIR)

rtls.o: ${SRC_DIR}/rtls.c
	$(CC) $(CFLAGS) -c ${SRC_DIR}/rtls.c -I$(SRC_DIR)

hdfutility.o: ${SRC_DIR}/hdfutility.c
	$(CC) $(CFLAGS) -c  ${SRC_DIR}/hdfutility.c -I$(HDFINC) -I$(SRC_DIR)

util.o: ${SRC_DIR}/util.c
	$(CC) $(CFLAGS) -c  ${SRC_DIR}/util.c  -I$(HDFINC) -I$(SRC_DIR)

cubic_conv.o: ${SRC_DIR}/cubic_conv.c
	$(CC) $(CFLAGS) -c  ${SRC_DIR}/cubic_conv.c -I$(HDFINC) -I$(SRC_DIR)

cfactor.o: ${SRC_DIR}/cfactor.c
	$(CC) $(CFLAGS) -c  ${SRC_DIR}/cfactor.c -I$(HDFINC) -I$(GCTPINC) -I$(SRC_DIR)

hls_hdfeos.o: ${SRC_DIR}/hls_hdfeos.c
	$(CC) $(CFLAGS) -c  ${SRC_DIR}/hls_hdfeos.c -I$(HDFINC) -I$(SRC_DIR)

s2r.o: ${SRC_DIR}/s2r.c
	$(CC) $(CFLAGS) -c ${SRC_DIR}/s2r.c -I$(GCTPINC) -I$(HDFINC) -I$(SRC_DIR)

lsatmeta.o: ${SRC_DIR}/lsatmeta.c
	$(CC) $(CFLAGS) -c ${SRC_DIR}/lsatmeta.c -I$(GCTPINC) -I$(HDFINC) -I$(SRC_DIR)

install:
	install -d ${PREFIX}/bin
	install -m 755 $(TGT) ${PREFIX}/bin

clean:
	rm -f *.o