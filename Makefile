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

stable:
	echo $(IMAGENAME) > stable.tmp
	git add * | tee -a stable.tmp
	git commit -a -m "automatic pre deployment commit" | tee -a stable.tmp
	echo "using $(DATESTRING)-$(TAG)" | tee -a stable.tmp
	docker build --no-cache --platform $(PLATFORM) -t $(IMAGE_NAME) . | tee -a stable.tmp
	docker tag $(IMAGE_NAME) $(IMAGE_NAME_LATEST) | tee -a stable.tmp
	docker tag $(IMAGE_NAME) $(IMAGE_NAME_STABLE) | tee -a stable.tmp
	docker push $(IMAGE_NAME) | tee -a stable.tmp
	docker push $(IMAGE_NAME_STABLE) | tee -a stable.tmp
	mv stable.tmp stable.log
	git add stable.log
	git push origin master

lint:
	black webstorageS3/*.py
	black bin/*.py
	isort webstorageS3/*.py
	black bin/*.py
	pylint webstorageS3/*.py
	pylint bin/*.py

clean:
	if [ -f stable.log ]; then rm stable.log; fi
	if [ -f stable ]; then rm stable; fi
	if [ -f latest.log ]; then rm latest.log; fi
	if [ -f latest ]; then rm latest; fi

latest-snap:
	git commit -a -m "automatic pre snap build commit"
	snapcraft clean webstorages3
	snapcraft

stable-snap:
	git commit -a -m "automatic pre snap build commit"
	snapcraft clean webstorages3
	snapcraft
