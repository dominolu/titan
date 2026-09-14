#ifndef TITAN_PLUGIN_ABI_V1_H
#define TITAN_PLUGIN_ABI_V1_H

#include <stddef.h>
#include <stdint.h>

#if defined(_WIN32)
#define TITAN_PLUGIN_EXPORT __declspec(dllexport)
#else
#define TITAN_PLUGIN_EXPORT __attribute__((visibility("default")))
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define TITAN_PLUGIN_MAGIC UINT64_C(0x544954414e504c47)
#define TITAN_HOST_MAGIC UINT64_C(0x544954414e484f53)
#define TITAN_DYNAMIC_ABI_MAJOR UINT16_C(1)
#define TITAN_DYNAMIC_ABI_MINOR UINT16_C(0)
#define TITAN_MANIFEST_SCHEMA_MAJOR UINT16_C(1)
#define TITAN_MANIFEST_SCHEMA_MINOR UINT16_C(0)

typedef int32_t TitanStatus;
typedef uint64_t TitanPluginHandle;

enum {
    TITAN_STATUS_OK = 0,
    TITAN_STATUS_INVALID_ARGUMENT = 1,
    TITAN_STATUS_HOST_ERROR = 2,
    TITAN_STATUS_PANIC = 3,
};

enum {
    TITAN_STOP_SHUTDOWN = 0,
    TITAN_STOP_RESTART = 1,
    TITAN_STOP_FAILURE = 2,
};

typedef struct TitanBuffer {
    uint8_t *data;
    size_t len;
    size_t capacity;
    void (*free)(uint8_t *data, size_t len, size_t capacity);
} TitanBuffer;

typedef struct TitanEventMetadataV1 {
    uint32_t source_id;
    uint32_t flags;
    uint64_t source_sequence;
    int64_t exchange_ts;
    int64_t receive_ts;
    int64_t publish_ts;
    uint64_t routing_key;
    uint64_t trace_id;
    uint64_t causation_id;
} TitanEventMetadataV1;

typedef struct TitanHostApiV1 {
    uint64_t magic;
    uint32_t struct_size;
    uint16_t abi_major;
    uint16_t abi_minor;
    void *context;
    TitanStatus (*publish_event)(
        void *context,
        const uint8_t *event_type,
        size_t event_type_len,
        uint32_t schema_version,
        const uint8_t *payload,
        size_t payload_len,
        TitanEventMetadataV1 metadata);
    TitanStatus (*log)(
        void *context,
        uint32_t level,
        const uint8_t *message,
        size_t message_len);
    int64_t (*now_ns)(void *context);
    TitanStatus (*resolve_secret)(
        void *context,
        const uint8_t *secret_ref,
        size_t secret_ref_len,
        TitanBuffer *output);
} TitanHostApiV1;

typedef struct PluginApiV1 {
    uint64_t magic;
    uint32_t struct_size;
    uint16_t abi_major;
    uint16_t abi_minor;
    uint16_t manifest_schema_major;
    uint16_t manifest_schema_minor;
    uint64_t required_feature_bits;
    uint64_t optional_feature_bits;
    const uint8_t *(*manifest_json)(size_t *length);
    TitanStatus (*create)(
        const uint8_t *config_json,
        size_t config_json_len,
        TitanPluginHandle *output);
    TitanStatus (*destroy)(TitanPluginHandle handle);
    size_t (*last_error)(uint8_t *output, size_t capacity);
    TitanStatus (*validate)(TitanPluginHandle handle);
    TitanStatus (*start)(TitanPluginHandle handle, const TitanHostApiV1 *host);
    TitanStatus (*quiesce)(TitanPluginHandle handle, uint32_t reason);
    TitanStatus (*stop)(TitanPluginHandle handle);
    TitanStatus (*query_interface)(
        TitanPluginHandle handle,
        const uint8_t *interface_name,
        size_t interface_name_len,
        uint16_t requested_major,
        const void **output);
} PluginApiV1;

typedef const PluginApiV1 *(*TitanPluginEntryV1)(void);

TITAN_PLUGIN_EXPORT const PluginApiV1 *titan_plugin_entry_v1(void);

#ifdef __cplusplus
}
#endif

#endif
