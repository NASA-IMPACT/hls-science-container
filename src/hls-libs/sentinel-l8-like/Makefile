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
	$(CC) $(CFLAGS) -c L8like.c -I$(HDFINC) -I$(GCTPINC) -I$(SRC_DIR)

s2at30m.o: ${SRC_DIR}/s2at30m.c
	$(CC) $(CFLAGS) -c ${SRC_DIR}/s2at30m.c -I$(GCTPINC) -I$(HDFINC) -I$(SRC_DIR)

s2r.o: ${SRC_DIR}/s2r.c
	$(CC) $(CFLAGS) -c ${SRC_DIR}/s2r.c -I$(GCTPINC) -I$(HDFINC) -I$(SRC_DIR)

hdfutility.o: ${SRC_DIR}/hdfutility.c
	$(CC) $(CFLAGS) -c  ${SRC_DIR}/hdfutility.c -I$(HDFINC) -I$(SRC_DIR)

util.o: ${SRC_DIR}/util.c
	$(CC) $(CFLAGS) -c  ${SRC_DIR}/util.c -I$(HDFINC) -I$(SRC_DIR)

hls_hdfeos.o: ${SRC_DIR}/hls_hdfeos.c
	$(CC) $(CFLAGS) -c  ${SRC_DIR}/hls_hdfeos.c -I$(HDFINC) -I$(SRC_DIR)

install:
	install -d ${PREFIX}/bin
	install -m 755 $(TGT) ${PREFIX}/bin

clean:
	rm -f *.o $(TGT)
