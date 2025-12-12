### The file dependency of my code is crazy:
### cubic_conv is not needed in this code, but just because
### a header file is included which requires cubic_conv definition.

TGT = sentinel-derive-nbar
OBJ = derive_s2nbar.o \
	s2at30m.o \
	s2ang.o \
	hls_projection.o \
	mean_solarzen.o \
	local_solar.o \
	rtls.o \
	hdfutility.o \
	util.o \
	cubic_conv.o \
	cfactor.o

$(TGT): $(OBJ)
	$(CC) $(CFLAGS) -o $(TGT) $(OBJ) -L$(GCTPLIB)  -L$(HDFLIB) -L$(ZLIB) -L$(SZIPLIB) -L$(JPEGLIB) -L$(PROJLIB) $(GCTPLINK)  $(HDFLINK)

derive_s2nbar.o: derive_s2nbar.c
	$(CC) $(CFLAGS) -c derive_s2nbar.c -I$(HDFINC) -I$(GCTPINC) -I$(COMMON_DIR)

s2at30m.o: ${COMMON_DIR}/s2at30m.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/s2at30m.c -I$(GCTPINC) -I$(HDFINC) -I$(COMMON_DIR)

s2ang.o: ${COMMON_DIR}/s2ang.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/s2ang.c -I$(HDFINC) -I$(COMMON_DIR)

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
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/util.c -I$(GCTPINC) -I$(HDFINC) -I$(COMMON_DIR)

cubic_conv.o: ${COMMON_DIR}/cubic_conv.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/cubic_conv.c -I$(HDFINC) -I$(COMMON_DIR)

cfactor.o: ${COMMON_DIR}/cfactor.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/cfactor.c -I$(GCTPINC) -I$(HDFINC) -I$(COMMON_DIR)

install:
	install -d ${PREFIX}/bin
	install -m 755 $(TGT) ${PREFIX}/bin

clean:
	rm -f *.o $(TGT)