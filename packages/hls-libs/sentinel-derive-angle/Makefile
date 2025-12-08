TGT = sentinel-derive-angle
OBJ = derive_s2ang.o \
	s2mapinfo.o \
	s2detfoo.o \
	s2ang.o \
	s2r.o \
	pnpoly.o \
	util.o \
	hdfutility.o \
	hls_hdfeos.o

$(TGT): $(OBJ)
	$(CC) $(CFLAGS) -o $(TGT) $(OBJ) -L$(HDFLIB) -L$(GCTPLIB) -L$(HDFLIB) -L$(ZLIB) -L$(SZIPLIB) -L$(JPEGLIB) -L$(PROJLIB) $(GCTPLINK) $(HDFLINK)

derive_s2ang.o: derive_s2ang.c
	$(CC) $(CFLAGS) -c derive_s2ang.c -I$(HDFINC) -I$(GCTPINC) -I$(COMMON_DIR)

s2mapinfo.o: ${COMMON_DIR}/s2mapinfo.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/s2mapinfo.c -I$(HDFINC) -I$(GCTPINC) -I$(COMMON_DIR)

s2detfoo.o: ${COMMON_DIR}/s2detfoo.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/s2detfoo.c -I$(HDFINC) -I$(GCTPINC) -I$(COMMON_DIR)

pnpoly.o: ${COMMON_DIR}/pnpoly.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/pnpoly.c  -I$(HDFINC) -I$(GCTPINC) -I$(COMMON_DIR)

s2ang.o: ${COMMON_DIR}/s2ang.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/s2ang.c -I$(HDFINC) -I$(GCTPINC) -I$(COMMON_DIR)

s2r.o: ${COMMON_DIR}/s2r.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/s2r.c -I$(HDFINC) -I$(GCTPINC) -I$(COMMON_DIR)

hdfutility.o: ${COMMON_DIR}/hdfutility.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/hdfutility.c -I$(HDFINC) -I$(COMMON_DIR)

util.o: ${COMMON_DIR}/util.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/util.c -I$(HDFINC) -I$(COMMON_DIR)

hls_hdfeos.o: ${COMMON_DIR}/hls_hdfeos.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/hls_hdfeos.c -I$(HDFINC) -I$(COMMON_DIR)

install:
	install -d ${PREFIX}/bin
	install -m 755 $(TGT) ${PREFIX}/bin

clean:
	rm -f *.o $(TGT)