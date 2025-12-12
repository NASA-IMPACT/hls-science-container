TGT = sentinel-create-s2at30m
OBJ = create_s2at30m.o \
	hls_projection.o \
	s2r.o \
	s2at30m.o \
	util.o \
	hdfutility.o

$(TGT): $(OBJ)
	$(CC) $(CFLAGS) -o $(TGT) $(OBJ) -L$(GCTPLIB) -L$(HDFLIB) -L$(ZLIB) -L$(SZIPLIB) -L$(JPEGLIB) -L$(PROJLIB) $(GCTPLINK) $(HDFLINK)

create_s2at30m.o: create_s2at30m.c
	$(CC) $(CFLAGS) -c create_s2at30m.c -I$(HDFINC) -I$(GCTPINC) -I$(COMMON_DIR)

hls_projection.o: ${COMMON_DIR}/hls_projection.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/hls_projection.c -I$(GCTPINC) -I$(HDFINC) -I$(COMMON_DIR)

s2r.o: ${COMMON_DIR}/s2r.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/s2r.c -I$(HDFINC) -I$(GCTPINC) -I$(COMMON_DIR)

s2at30m.o: ${COMMON_DIR}/s2at30m.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/s2at30m.c -I$(HDFINC) -I$(GCTPINC) -I$(COMMON_DIR)

hdfutility.o: ${COMMON_DIR}/hdfutility.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/hdfutility.c -I$(HDFINC) -I$(COMMON_DIR)

util.o: ${COMMON_DIR}/util.c
	$(CC) $(CFLAGS) -c  ${COMMON_DIR}/util.c -I$(HDFINC) -I$(COMMON_DIR)

install:
	install -d ${PREFIX}/bin
	install -m 755 $(TGT) ${PREFIX}/bin

clean:
	rm -f *.o $(TGT)