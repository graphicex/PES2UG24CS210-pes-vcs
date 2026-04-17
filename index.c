// index.c — Staging area implementation
//
// Text format of .pes/index (one entry per line, sorted by path):
//
//   <mode-octal> <64-char-hex-hash> <mtime-seconds> <size> <path>
//
// Example:
//   100644 a1b2c3d4e5f6...  1699900000 42 README.md
//   100644 f7e8d9c0b1a2...  1699900100 128 src/main.c
//
// This is intentionally a simple text format. No magic numbers, no
// binary parsing. The focus is on the staging area CONCEPT (tracking
// what will go into the next commit) and ATOMIC WRITES (temp+rename).
//
// PROVIDED functions: index_find, index_remove, index_status
// TODO functions:     index_load, index_save, index_add

#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

// Find an index entry by path (linear scan).
IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

// Remove a file from the index.
// Returns 0 on success, -1 if path not in index.
int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0)
                memmove(&index->entries[i], &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }
    fprintf(stderr, "error: '%s' is not in the index\n", path);
    return -1;
}

// Print the status of the working directory.
int index_status(const Index *index) {
    printf("Staged changes:\n");
    int staged_count = 0;
    // Note: A true Git implementation deeply diffs against the HEAD tree here. 
    // For this lab, displaying indexed files represents the staging intent.
    for (int i = 0; i < index->count; i++) {
        printf("  staged:     %s\n", index->entries[i].path);
        staged_count++;
    }
    if (staged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Unstaged changes:\n");
    int unstaged_count = 0;
    for (int i = 0; i < index->count; i++) {
        struct stat st;
        if (stat(index->entries[i].path, &st) != 0) {
            printf("  deleted:    %s\n", index->entries[i].path);
            unstaged_count++;
        } else {
            // Fast diff: check metadata instead of re-hashing file content
            if (st.st_mtime != (time_t)index->entries[i].mtime_sec || st.st_size != (off_t)index->entries[i].size) {
                printf("  modified:   %s\n", index->entries[i].path);
                unstaged_count++;
            }
        }
    }
    if (unstaged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Untracked files:\n");
    int untracked_count = 0;
    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            // Skip hidden directories, parent directories, and build artifacts
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            if (strcmp(ent->d_name, ".pes") == 0) continue;
            if (strcmp(ent->d_name, "pes") == 0) continue; // compiled executable
            if (strstr(ent->d_name, ".o") != NULL) continue; // object files

            // Check if file is tracked in the index
            int is_tracked = 0;
            for (int i = 0; i < index->count; i++) {
                if (strcmp(index->entries[i].path, ent->d_name) == 0) {
                    is_tracked = 1; 
                    break;
                }
            }
            
            if (!is_tracked) {
                struct stat st;
                stat(ent->d_name, &st);
                if (S_ISREG(st.st_mode)) { // Only list regular files for simplicity
                    printf("  untracked:  %s\n", ent->d_name);
                    untracked_count++;
                }
            }
        }
        closedir(dir);
    }
    if (untracked_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    return 0;
}

// ─── IMPLEMENTED: TODOs ──────────────────────────────────────────────────────

// Load the index from .pes/index.
// Returns 0 on success, -1 on error.
int index_load(Index *index) {
    index->count = 0;
    
    FILE *f = fopen(".pes/index", "r");
    if (!f) {
        // If the file doesn't exist, we start with an empty index (not an error)
        return 0;
    }

    char hash_hex[65];
    unsigned int mode;
    long long mtime, size;
    char path[1024];

    // Read the text file line by line
    while (fscanf(f, "%o %64s %lld %lld %[^\n]", &mode, hash_hex, &mtime, &size, path) == 5) {
        IndexEntry *entry = &index->entries[index->count];
        
        entry->mode = mode;
        entry->mtime_sec = (uint32_t)mtime;
        entry->size = (uint32_t)size;
        
        // Remove potential trailing/leading whitespace from path reading
        strcpy(entry->path, path);
        
        // Convert string to ObjectID
        hex_to_hash(hash_hex, &entry->hash);
        
        index->count++;
    }

    fclose(f);
    return 0;
}

// Helper function to compare IndexEntries by path for qsort
static int compare_entries(const void *a, const void *b) {
    return strcmp(((const IndexEntry *)a)->path, ((const IndexEntry *)b)->path);
}

// Save the index to .pes/index atomically.
// Returns 0 on success, -1 on error.
int index_save(const Index *index) {
    // Sort entries array by path 
    // Note: casting away const to sort in-place is typical for this logic pattern
    qsort((void *)index->entries, index->count, sizeof(IndexEntry), compare_entries);

    // Write to a temporary file first
    FILE *f = fopen(".pes/index.tmp", "w");
    if (!f) {
        perror("Failed to open temp index file");
        return -1;
    }

    for (int i = 0; i < index->count; i++) {
        const IndexEntry *entry = &index->entries[i];
        char hash_hex[65];
        
        // Convert ObjectID to text output
        hash_to_hex(&entry->hash, hash_hex);
        
        fprintf(f, "%06o %s %u %u %s\n", 
                entry->mode, 
                hash_hex, 
                entry->mtime_sec, 
                entry->size, 
                entry->path);
    }

    // Flush userspace buffers and sync to disk
    fflush(f);
    fsync(fileno(f));
    fclose(f);

    // Atomically move the temp file over the old index
    if (rename(".pes/index.tmp", ".pes/index") != 0) {
        perror("Failed to rename index file");
        return -1;
    }

    return 0;
}

// Stage a file for the next commit.
// Returns 0 on success, -1 on error.
int index_add(Index *index, const char *path) {
    struct stat st;
    
    // Get file metadata
    if (lstat(path, &st) != 0) {
        perror("Failed to stat file");
        return -1;
    }

    // Open target file
    FILE *f = fopen(path, "rb");
    if (!f) {
        perror("Failed to open file for reading");
        return -1;
    }

    // Read target file's contents
    unsigned char *buffer = NULL;
    if (st.st_size > 0) {
        buffer = malloc(st.st_size);
        if (!buffer) {
            fclose(f);
            return -1;
        }
        if (fread(buffer, 1, st.st_size, f) != st.st_size) {
            free(buffer);
            fclose(f);
            return -1;
        }
    }
    fclose(f);

    ObjectID hash;
    // Save contents as OBJ_BLOB into the object database
    if (object_write(OBJ_BLOB, buffer, st.st_size, &hash) != 0) {
        if (buffer) free(buffer);
        return -1;
    }
    if (buffer) free(buffer);

    // Check if the file is already staged
    IndexEntry *entry = index_find(index, path);
    if (!entry) {
        // Create new entry
        entry = &index->entries[index->count++];
        strncpy(entry->path, path, sizeof(entry->path) - 1);
        entry->path[sizeof(entry->path) - 1] = '\0';
    }

    // Update metadata and hash
    entry->mode = st.st_mode;
    entry->mtime_sec = st.st_mtime;
    entry->size = st.st_size;
    entry->hash = hash;

    // Save index automatically upon adding
    return index_save(index);
}
