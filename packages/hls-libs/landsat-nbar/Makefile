TGT = landsat-nbar
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
	$(CC) $(CFLAGS) -c derive_l8nbar.c -I$(HDFINC) -I$(GCTPINC) -I$(COMMON_DIR)

lsat.o: ${COMMON_DIR}/lsat.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/lsat.c -I$(GCTPINC) -I$(HDFINC) -I$(COMMON_DIR)

l8ang.o: ${COMMON_DIR}/l8ang.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/l8ang.c -I$(HDFINC) -I$(COMMON_DIR)

hls_projection.o: ${COMMON_DIR}/hls_projection.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/hls_projection.c -I$(GCTPINC) -I$(HDFINC) -I$(COMMON_DIR)

mean_solarzen.o: ${COMMON_DIR}/mean_solarzen.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/mean_solarzen.c -I$(GCTPINC) -I$(HDFINC) -I$(COMMON_DIR)

local_solar.o: ${COMMON_DIR}/local_solar.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/local_solar.c -I$(GCTPINC) -I$(HDFINC) -I$(COMMON_DIR)

rtls.o: ${COMMON_DIR}/rtls.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/rtls.c -I$(COMMON_DIR)

hdfutility.o: ${COMMON_DIR}/hdfutility.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/hdfutility.c -I$(HDFINC) -I$(COMMON_DIR)

util.o: ${COMMON_DIR}/util.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/util.c  -I$(HDFINC) -I$(COMMON_DIR)

cubic_conv.o: ${COMMON_DIR}/cubic_conv.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/cubic_conv.c -I$(HDFINC) -I$(COMMON_DIR)

cfactor.o: ${COMMON_DIR}/cfactor.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/cfactor.c -I$(HDFINC) -I$(GCTPINC) -I$(COMMON_DIR)

hls_hdfeos.o: ${COMMON_DIR}/hls_hdfeos.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/hls_hdfeos.c -I$(HDFINC) -I$(COMMON_DIR)

s2r.o: ${COMMON_DIR}/s2r.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/s2r.c -I$(GCTPINC) -I$(HDFINC) -I$(COMMON_DIR)

lsatmeta.o: ${COMMON_DIR}/lsatmeta.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/lsatmeta.c -I$(GCTPINC) -I$(HDFINC) -I$(COMMON_DIR)

install:
	install -d ${PREFIX}/bin
	install -m 755 $(TGT) ${PREFIX}/bin

clean:
	rm -f *.o
