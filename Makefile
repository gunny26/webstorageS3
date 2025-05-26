export PLATFORM ?= linux/arm64/v8
export REPOSITORY_NAME ?= $(shell pwd | rev | cut -d/ -f 1 | rev)
export DATESTRING ?= $(shell date -I)
export TAG ?= $(shell git describe --always)

latest:
	git commit -a -m "automatic latest built commit"; echo 0
	python3 setup.py install --user
	git push origin latest

stable:
	git checkout master
	git merge latest
	git commit -a -m "automatic stable commit"; echo 0
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
