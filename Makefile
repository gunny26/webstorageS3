export PLATFORM ?= linux/arm64/v8
export IMAGENAME ?= $(shell pwd | rev | cut -d/ -f 1 | rev)
export DATESTRING ?= $(shell date -I)
export TAG ?= $(shell git describe --always)
export REGISTRY ?= registry.messner.click/gunny26
export IMAGE_NAME ?= $(REGISTRY)/$(IMAGENAME):$(DATESTRING)-$(TAG)
export IMAGE_NAME_LATEST ?= $(REGISTRY)/$(IMAGENAME):latest
export IMAGE_NAME_STABLE ?= $(REGISTRY)/$(IMAGENAME):stable

latest:
	git commit -a -m "automatic pre latest built commit"; echo 0
	python3 setup.py install --user
	git push origin latest

stable:
	git checkout master
	git pull latest
	git commit -a -m "automatic pre stable commit"; echo 0
	python3 setup.py install --user
	python3 setup.py bdist_wheel
	git push origin master
	git checkout latest

lint:
	black webstorageS3/*.py
	black bin/*.py
	isort webstorageS3/*.py
	isort bin/*.py
	pylint webstorageS3/*.py
	pylint bin/*.py

latest-snap:
	git commit -a -m "automatic pre snap build commit"
	snapcraft clean webstorages3
	snapcraft

stable-snap:
	git commit -a -m "automatic pre snap build commit"
	snapcraft clean webstorages3
	snapcraft
