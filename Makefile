# Build the NiFi NAR inside a Docker container: no local Java or Maven required.
#
#   make          build the NAR (with tests) and copy it to ./dist/
#   make test     run tests only
#   make clean    remove build output (target/, dist/)
#   make distclean  also remove the Maven cache (.cache/)

JDK_IMAGE  ?= eclipse-temurin:21-jdk
MVN_ARGS   ?=
DIST_DIR   := dist
CACHE_DIR  := .cache/m2
NAR_GLOB   := nifi-latest-dedup-nar/target/nifi-latest-dedup-nar-*.nar

# Run as the current user so build output is not owned by root (matters on Linux);
# Maven repository and wrapper distribution are cached in ./.cache between runs.
DOCKER_RUN = docker run --rm \
	--user $$(id -u):$$(id -g) \
	-v "$(CURDIR)":/workspace \
	-w /workspace \
	-e HOME=/tmp \
	-e MAVEN_USER_HOME=/workspace/$(CACHE_DIR) \
	$(JDK_IMAGE) \
	./mvnw -B -Dmaven.repo.local=/workspace/$(CACHE_DIR)/repository

.PHONY: nar test clean distclean

nar:
	@mkdir -p $(CACHE_DIR) $(DIST_DIR)
	$(DOCKER_RUN) clean install $(MVN_ARGS)
	@rm -f $(DIST_DIR)/*.nar
	cp $(NAR_GLOB) $(DIST_DIR)/
	@echo "NAR: $$(ls $(DIST_DIR)/*.nar)"

test:
	@mkdir -p $(CACHE_DIR)
	$(DOCKER_RUN) test -pl nifi-latest-dedup-processors $(MVN_ARGS)

clean:
	rm -rf $(DIST_DIR) nifi-latest-dedup-processors/target nifi-latest-dedup-nar/target target

distclean: clean
	rm -rf .cache
