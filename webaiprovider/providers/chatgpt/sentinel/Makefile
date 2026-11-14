CC ?= cc
AR ?= ar
BUILD_DIR ?= build
CPPFLAGS ?= -I.
CFLAGS ?= -std=c99 -O3 -Wall -Wextra -Werror -fPIC -fvisibility=hidden
LDFLAGS ?=

SRCS := $(filter-out _native.c,$(wildcard *.c))
HDRS := sentinel.h internal.h
STATIC_OBJS := $(patsubst %.c,$(BUILD_DIR)/obj/static/%.o,$(SRCS))
SHARED_OBJS := $(patsubst %.c,$(BUILD_DIR)/obj/shared/%.o,$(SRCS))

STATIC_LIB := $(BUILD_DIR)/libsentinel.a
SHARED_LIB := $(BUILD_DIR)/libsentinel.so
TEST_BIN := $(BUILD_DIR)/test_sentinel

all: $(STATIC_LIB) $(SHARED_LIB) $(TEST_BIN)

$(BUILD_DIR)/obj/static/%.o: %.c $(HDRS) | $(BUILD_DIR)/obj/static
	$(CC) $(CPPFLAGS) -DSENTINEL_STATIC $(CFLAGS) -c $< -o $@

$(BUILD_DIR)/obj/shared/%.o: %.c $(HDRS) | $(BUILD_DIR)/obj/shared
	$(CC) $(CPPFLAGS) -DSENTINEL_EXPORTS $(CFLAGS) -c $< -o $@

$(STATIC_LIB): $(STATIC_OBJS) | $(BUILD_DIR)
	$(AR) rcs $@ $^

$(SHARED_LIB): $(SHARED_OBJS) | $(BUILD_DIR)
	$(CC) -shared $(LDFLAGS) -o $@ $^

$(TEST_BIN): ../../../../tests/test_sentinel.c $(STATIC_LIB) $(HDRS) | $(BUILD_DIR)
	$(CC) $(CPPFLAGS) -DSENTINEL_STATIC $(CFLAGS) $(LDFLAGS) ../../../../tests/test_sentinel.c $(STATIC_LIB) -o $@

$(BUILD_DIR) $(BUILD_DIR)/obj/static $(BUILD_DIR)/obj/shared:
	mkdir -p $@

test: $(TEST_BIN)
	$(TEST_BIN)

test-san: $(SRCS) ../../../../tests/test_sentinel.c $(HDRS) | $(BUILD_DIR)
	$(CC) $(CPPFLAGS) -DSENTINEL_STATIC -std=c99 -O1 -g -Wall -Wextra -Werror \
		-fsanitize=address,undefined $(SRCS) ../../../../tests/test_sentinel.c -o $(BUILD_DIR)/test_sentinel_san
	$(BUILD_DIR)/test_sentinel_san

clean:
	rm -rf $(BUILD_DIR)

.PHONY: all test test-san clean
