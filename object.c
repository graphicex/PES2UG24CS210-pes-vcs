// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions:     object_write, object_read

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
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

// Get the filesystem path where an object should be stored.
// Format: .pes/objects/XX/YYYYYYYY...
// The first 2 hex chars form the shard directory; the rest is the filename.
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

// ─── TODO: Implement these ──────────────────────────────────────────────────

// Write an object to the store.
//
// Object format on disk:
//   "<type> <size>\0<data>"
//   where <type> is "blob", "tree", or "commit"
//   and <size> is the decimal string of the data length
//
// Steps:
//   1. Build the full object: header ("blob 16\0") + data
//   2. Compute SHA-256 hash of the FULL object (header + data)
//   3. Check if object already exists (deduplication) — if so, just return success
//   4. Create shard directory (.pes/objects/XX/) if it doesn't exist
//   5. Write to a temporary file in the same shard directory
//   6. fsync() the temporary file to ensure data reaches disk
//   7. rename() the temp file to the final path (atomic on POSIX)
//   8. Open and fsync() the shard directory to persist the rename
//   9. Store the computed hash in *id_out

// HINTS - Useful syscalls and functions for this phase:
//   - sprintf / snprintf : formatting the header string
//   - compute_hash       : hashing the combined header + data
//   - object_exists      : checking for deduplication
//   - mkdir              : creating the shard directory (use mode 0755)
//   - open, write, close : creating and writing to the temp file
//                          (Use O_CREAT | O_WRONLY | O_TRUNC, mode 0644)
//   - fsync              : flushing the file descriptor to disk
//   - rename             : atomically moving the temp file to the final path
//
//implemented
// 1ST STEP Build the full object buffer
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    //   Build header + full object buffer
    const char *type_str;
    if      (type == OBJ_BLOB)   type_str = "blob";
    else if (type == OBJ_TREE)   type_str = "tree";
    else if (type == OBJ_COMMIT) type_str = "commit";
    else return -1;

    char header[64];
    // snprintf
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len);

    // Full object = header bytes +one '\0'separator + raw data
    size_t full_len = (size_t)header_len + 1 + len;
    uint8_t *full   = malloc(full_len);
    if (!full) return -1;

    memcpy(full, header, (size_t)header_len);
    full[header_len] = '\0';
    memcpy(full + header_len + 1, data, len);
 // ── Step 2: Compute SHA-256 of the full object ───────────────────────────
    ObjectID id;
    compute_hash(full, full_len, &id);
    if (id_out) *id_out = id;

    // ── Step 3: Deduplication — already stored? ──────────────────────────────
    if (object_exists(&id)) {
        free(full);
        return 0;  // already stored, nothing to do
    }

    // ── Step 4: Create shard directory .pes/objects/XX/ ─────────────────────
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(&id, hex);

    char shard_dir[512];
    snprintf(shard_dir, sizeof(shard_dir), "%s/%.2s", OBJECTS_DIR, hex);
    mkdir(shard_dir, 0755);  // OK if already exists

    // ── Step 5: Build final and temp paths ───────────────────────────────────
    char final_path[512], tmp_path[520];
    object_path(&id, final_path, sizeof(final_path));
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", final_path);

    // ── Step 6: Write full object to temp file ───────────────────────────────
    int fd = open(tmp_path, O_CREAT | O_WRONLY | O_TRUNC, 0444);
    if (fd < 0) { free(full); return -1; }

    ssize_t written = write(fd, full, full_len);
    free(full);  // done with the buffer regardless of outcome

    if (written < 0 || (size_t)written != full_len) {
        close(fd);
        unlink(tmp_path);
        return -1;
    }

    // ── Step 6b: fsync the temp file — data reaches disk ─────────────────────
    if (fsync(fd) != 0) { close(fd); unlink(tmp_path); return -1; }
    close(fd);

    // ── Step 7: Atomic rename to final path ──────────────────────────────────
    if (rename(tmp_path, final_path) != 0) {
        unlink(tmp_path);
        return -1;
    }

    // ── Step 7b: fsync the shard directory — directory entry is durable ──────
    int dir_fd = open(shard_dir, O_RDONLY);
    if (dir_fd >= 0) { fsync(dir_fd); close(dir_fd); }

    return 0;
}

/*
 * object_read — Retrieve and verify an object from the store.
 *
 * HOW IT WORKS (step by step):
 *
 * Step 1 — Locate and read the file
 *   object_path() converts the binary ObjectID → the .pes/objects/XX/YY...
 *   path. We read the entire file into one malloc'd buffer.
 *
 * Step 2 — Integrity check (verify hash)
 *   Re-hash the file contents and compare to the expected ObjectID.
 *   If they don't match, the file is corrupt (disk error, manual edit, etc).
 *   We return -1 so callers never use corrupt data silently.
 *
 * Step 3 — Parse the header
 *   memchr() safely finds the '\0' separator. Everything before it is the
 *   header ("blob 42"), everything after is the raw data.
 *   strncmp() identifies the type string.
 *
 * Step 4 — Return the data portion
 *   Allocate a new buffer for just the data (after the null separator),
 *   copy it in, and set the output pointers. We add an extra null byte
 *   at the end so callers can safely treat text objects as C strings.
 */
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    // ── Step 1: Get path and read the entire file ────────────────────────────
    char path[512];
    object_path(id, path, sizeof(path));

    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    fseek(f, 0, SEEK_END);
    size_t full_len = (size_t)ftell(f);
    rewind(f);

    uint8_t *full = malloc(full_len);
    if (!full) { fclose(f); return -1; }

    if (fread(full, 1, full_len, f) != full_len) {
        free(full); fclose(f); return -1;
    }
    fclose(f);

    // ── Step 2: Integrity check ───────────────────────────────────────────────
    // Re-hash the stored bytes; they must match the requested ObjectID
    ObjectID computed;
    compute_hash(full, full_len, &computed);
    if (memcmp(computed.hash, id->hash, HASH_SIZE) != 0) {
        fprintf(stderr, "error: corrupt object — hash mismatch\n");
        free(full);
        return -1;
    }

    // ── Step 3: Parse header ─────────────────────────────────────────────────
    // Find the '\0' that separates "blob 42" from the raw data
    uint8_t *null_pos = (uint8_t *)memchr(full, '\0', full_len);
    if (!null_pos) { free(full); return -1; }

    // Identify object type from header prefix
    if      (strncmp((char *)full, "blob",   4) == 0) *type_out = OBJ_BLOB;
    else if (strncmp((char *)full, "tree",   4) == 0) *type_out = OBJ_TREE;
    else if (strncmp((char *)full, "commit", 6) == 0) *type_out = OBJ_COMMIT;
    else { free(full); return -1; }

    // ── Step 4: Extract and return the data portion ──────────────────────────
    size_t data_offset = (size_t)(null_pos - full) + 1; // byte after '\0'
    *len_out  = full_len - data_offset;

    // +1 so callers can safely strlen() text objects (commits, etc.)
    *data_out = malloc(*len_out + 1);
    if (!*data_out) { free(full); return -1; }
    memcpy(*data_out, full + data_offset, *len_out);
    ((uint8_t *)*data_out)[*len_out] = '\0';  // safe null-terminator

    free(full);
    return 0;
}

//
// Returns 0 on success, -1 on error.
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    // TODO: Implement
    (void)type; (void)data; (void)len; (void)id_out;
    return -1;
}

// Read an object from the store.
//
// Steps:
//   1. Build the file path from the hash using object_path()
//   2. Open and read the entire file
//   3. Parse the header to extract the type string and size
//   4. Verify integrity: recompute the SHA-256 of the file contents
//      and compare to the expected hash (from *id). Return -1 if mismatch.
//   5. Set *type_out to the parsed ObjectType
//   6. Allocate a buffer, copy the data portion (after the \0), set *data_out and *len_out
//
// HINTS - Useful syscalls and functions for this phase:
//   - object_path        : getting the target file path
//   - fopen, fread, fseek: reading the file into memory
	//   - memchr             : safely finding the '\0' separating header and data
//   - strncmp            : parsing the type string ("blob", "tree", "commit")
//   - compute_hash       : re-hashing the read data for integrity verification
//   - memcmp             : comparing the computed hash against the requested hash
//   - malloc, memcpy     : allocating and returning the extracted data
//
// The caller is responsible for calling free(*data_out).
// Returns 0 on success, -1 on error (file not found, corrupt, etc.).
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    // TODO: Implement
    (void)id; (void)type_out; (void)data_out; (void)len_out;
    return -1;
}
