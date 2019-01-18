docker-build.operator:
	cd operator && $(MAKE) test
	docker build -f operator/Dockerfile .