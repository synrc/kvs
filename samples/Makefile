RELEASE := kvs_shell
COOKIE  := node_runner
APPS    := mnesia kernel stdlib sasl kvs active
VER     := 1.0.0
PLT_NAME := ~/.kvs_dialyzer.plt
VM      := rels/files/vm.args
SYS     := rels/files/sys.config
ERL_ARGS     := -args_file $(VM) -config $(SYS)
RUN_DIR      ?= ./rels/devbox
LOG_DIR      ?= ./rels/devbox/logs

default: compile
static-link:

include otp.mk
