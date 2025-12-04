PREFIX = ${HLS_LIBS_PREFIX}

SUBDIRS = landsat-add-fmask-sds \
			  sentinel-add-fmask-sds \
			  sentinel-consolidate \
			  sentinel-consolidate-angle \
			  sentinel-create-s2at30m \
			  sentinel-derive-angle \
			  sentinel-derive-nbar \
			  sentinel-l8-like \
			  sentinel-trim \
			  sentinel-twohdf2one

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
