PREFIX = ${HLS_LIBS_PREFIX}

SUBDIRS = sentinel-add-fmask-sds \
			  landsat-add-fmask-sds

.PHONY: all $(SUBDIRS) clean

all: $(SUBDIRS)
	@echo "--- All subdirectories built successfully ---"

$(SUBDIRS):
	@echo "Entering directory '$@' to run make..."
	$(MAKE) -C $@

install:
	@echo "--- Installing all subdirectories ---"
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir install; \
	done

clean:
	@echo "--- Cleaning all subdirectories ---"
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir clean; \
	done