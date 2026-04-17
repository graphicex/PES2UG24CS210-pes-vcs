// object.c — Content-addressable object store
#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++)
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    const char *type_str;
    if      (type == OBJ_BLOB)   type_str = "blob";
    else if (type == OBJ_TREE)   type_str = "tree";
    else if (type == OBJ_COMMIT) type_str = "commit";
    else return -1;

    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len);
    size_t full_len = (size_t)header_len + 1 + len;
    uint8_t *full = malloc(full_len);
    if (!full) return -1;
    memcpy(full, header, (size_t)header_len);
    full[header_len] = '\0';
    memcpy(full + header_len + 1, data, len);

    ObjectID id;
    compute_hash(full, full_len, &id);
    if (id_out) *id_out = id;

    if (object_exists(&id)) { free(full); return 0; }

    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(&id, hex);
    char shard_dir[512];
    snprintf(shard_dir, sizeof(shard_dir), "%s/%.2s", OBJECTS_DIR, hex);
    mkdir(shard_dir, 0755);

    char final_path[512], tmp_path[520];
    object_path(&id, final_path, sizeof(final_path));
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", final_path);

    int fd = open(tmp_path, O_CREAT | O_WRONLY | O_TRUNC, 0444);
    if (fd < 0) { free(full); return -1; }
    ssize_t written = write(fd, full, full_len);
    free(full);
    if (written < 0 || (size_t)written != full_len) { close(fd); unlink(tmp_path); return -1; }
    if (fsync(fd) != 0) { close(fd); unlink(tmp_path); return -1; }
    close(fd);
    if (rename(tmp_path, final_path) != 0) { unlink(tmp_path); return -1; }

    int dir_fd = open(shard_dir, O_RDONLY);
    if (dir_fd >= 0) { fsync(dir_fd); close(dir_fd); }
    return 0;
}

int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    char path[512];
    object_path(id, path, sizeof(path));
    FILE *f = fopen(path, "rb");
    if (!f) return -1;
    fseek(f, 0, SEEK_END);
    size_t full_len = (size_t)ftell(f);
    rewind(f);
    uint8_t *full = malloc(full_len);
    if (!full) { fclose(f); return -1; }
    if (fread(full, 1, full_len, f) != full_len) { free(full); fclose(f); return -1; }
    fclose(f);

    ObjectID computed;
    compute_hash(full, full_len, &computed);
    if (memcmp(computed.hash, id->hash, HASH_SIZE) != 0) { free(full); return -1; }

    uint8_t *null_pos = (uint8_t *)memchr(full, '\0', full_len);
    if (!null_pos) { free(full); return -1; }
    if      (strncmp((char *)full, "blob",   4) == 0) *type_out = OBJ_BLOB;
    else if (strncmp((char *)full, "tree",   4) == 0) *type_out = OBJ_TREE;
    else if (strncmp((char *)full, "commit", 6) == 0) *type_out = OBJ_COMMIT;
    else { free(full); return -1; }

    size_t data_offset = (size_t)(null_pos - full) + 1;
    *len_out  = full_len - data_offset;
    *data_out = malloc(*len_out + 1);
    if (!*data_out) { free(full); return -1; }
    memcpy(*data_out, full + data_offset, *len_out);
    ((uint8_t *)*data_out)[*len_out] = '\0';
    free(full);
    return 0;
}
