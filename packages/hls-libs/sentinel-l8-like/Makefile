# Because of the use of hls_hdfeos.h, additional file are included:
# s2r.[hc]
TGT = sentinel-l8-like
OBJ = L8like.o \
			  s2at30m.o \
			  s2r.o \
			  hdfutility.o \
			  util.o \
			  hls_hdfeos.o

$(TGT): $(OBJ)
	$(CC) $(CFLAGS) -o $(TGT) $(OBJ) -L$(GCTPLIB)  -L$(HDFLIB) -L$(ZLIB) -L$(SZIPLIB) -L$(JPEGLIB) -L$(PROJLIB) $(GCTPLINK) $(HDFLINK)

L8like.o: L8like.c
	$(CC) $(CFLAGS) -c L8like.c -I$(HDFINC) -I$(GCTPINC) -I$(COMMON_DIR)

s2at30m.o: ${COMMON_DIR}/s2at30m.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/s2at30m.c -I$(GCTPINC) -I$(HDFINC) -I$(COMMON_DIR)

s2r.o: ${COMMON_DIR}/s2r.c
	$(CC) $(CFLAGS) -c ${COMMON_DIR}/s2r.c -I$(GCTPINC) -I$(HDFINC) -I$(COMMON_DIR)

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