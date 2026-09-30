// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

#include "compress_shim.h"
#include <zstd.h>

unsigned long long egres_zstd_contentsize_error(void) {
    return ZSTD_CONTENTSIZE_ERROR;
}

unsigned long long egres_zstd_contentsize_unknown(void) {
    return ZSTD_CONTENTSIZE_UNKNOWN;
}

