PACKAGE ?= pvc
VERSION ?= $(shell git describe --tags)
BASE_DIR ?= $(shell pwd)
ERLANG_BIN ?= $(shell dirname $(shell which erl))
REBAR ?= $(BASE_DIR)/rebar3
MAKE = make

.PHONY: compile clean packageclean check lint shell

all: compile

compile:
	$(REBAR) compile

debug_bin:
	$(REBAR) as debug_bin compile

clean: packageclean
	$(REBAR) clean

packageclean:
	rm -fr *.deb
	rm -fr *.tar.gz

check: xref dialyzer lint

lint:
	$(REBAR) as lint lint

shell:
	$(REBAR) shell --apps pvc

include tools.mk
