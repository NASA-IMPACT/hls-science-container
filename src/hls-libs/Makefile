# Define the list of subdirectories that contain Makefiles
#
SUBDIRS = sentinel-add-fmask-sds \
		  landsat-add-fmask-sds

MAKE = make

.PHONY: all $(SUBDIRS) clean

all: $(SUBDIRS)
    @echo "--- All subdirectories built successfully ---"

$(SUBDIRS):
    @echo "Entering directory '$@' to run make..."
    $(MAKE) -C $@

clean:
    @echo "--- Cleaning all subdirectories ---"
    for dir in $(SUBDIRS); do \
        $(MAKE) -C $$dir clean; \
    done
