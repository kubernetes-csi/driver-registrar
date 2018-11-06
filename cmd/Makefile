SUBDIRS := driver-kube-registrar \
	driver-node-registrar

CLEANDIRS = $(SUBDIRS:%=clean-%)
CONTAINERDIRS = $(SUBDIRS:%=container-%)
PUSHDIRS = $(SUBDIRS:%=push-%)

all: $(SUBDIRS)

clean: $(CLEANDIRS)

container: $(CONTAINERDIRS)

push: $(PUSHDIRS)

$(CLEANDIRS):
	$(MAKE) -C $(@:clean-%=%) clean

$(CONTAINERDIRS):
	$(MAKE) -C $(@:container-%=%) container

$(PUSHDIRS):
	$(MAKE) -C $(@:push-%=%) push

$(SUBDIRS):
	$(MAKE) -C $@


.PHONY: all \
	push \
	container \
	clean \
	$(SUBDIRS) \
	$(CONTAINERDIRS) \
	$(PUSHDIRS) \
	$(CLEANDIRS)
