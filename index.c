#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

int object_write(int type, const void *data, size_t len, ObjectID *id_out);

IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

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

int index_status(const Index *index) {
    printf("Staged changes:\n");
    int staged_count = 0;
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
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            if (strcmp(ent->d_name, ".pes") == 0) continue;
            if (strcmp(ent->d_name, "pes") == 0) continue;
            if (strstr(ent->d_name, ".o") != NULL) continue;

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
                if (S_ISREG(st.st_mode)) {
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

static int compare_index_entries(const void *a, const void *b) {
    return strcmp(((const IndexEntry *)a)->path, ((const IndexEntry *)b)->path);
}

int index_load(Index *index) {
    index->count = 0;
    FILE *f = fopen(".pes/index", "r");
    if (!f) return 0; 

    char hex[65];
    char temp_path[256];
    unsigned int mode;
    unsigned long mtime;
    size_t size;

    while (fscanf(f, "%o %64s %lu %zu %255s", &mode, hex, &mtime, &size, temp_path) == 5) {
        if (index->count >= MAX_INDEX_ENTRIES) break;

        IndexEntry *e = &index->entries[index->count++];
        e->mode = (uint32_t)mode;
        hex_to_hash(hex, &e->hash);
        e->mtime_sec = (uint64_t)mtime;
        e->size = size;
        
        strncpy(e->path, temp_path, sizeof(e->path) - 1);
        e->path[sizeof(e->path) - 1] = '\0';
    }

    fclose(f);
    return 0;
}

int index_save(const Index *index) {
    // 1. Allocate on the heap to protect the stack!
    Index *sorted_idx = malloc(sizeof(Index));
    if (!sorted_idx) return -1;

    // 2. Copy and sort
    *sorted_idx = *index;
    qsort(sorted_idx->entries, sorted_idx->count, sizeof(IndexEntry), compare_index_entries);

    char temp_file[256];
    snprintf(temp_file, sizeof(temp_file), ".pes/index.tmp");

    FILE *f = fopen(temp_file, "w");
    if (!f) {
        free(sorted_idx); // Don't forget to free on error!
        return -1;
    }

    for (int i = 0; i < sorted_idx->count; i++) {
        const IndexEntry *e = &sorted_idx->entries[i];
        char hex[65];
        hash_to_hex(&e->hash, hex);

        fprintf(f, "%o %s %lu %zu %s\n", 
                (unsigned int)e->mode, hex, (unsigned long)e->mtime_sec, (size_t)e->size, e->path);
    }

    fflush(f);
    fsync(fileno(f));
    fclose(f);
    free(sorted_idx); // Clean up heap memory

    if (rename(temp_file, ".pes/index") < 0) {
        unlink(temp_file);
        return -1;
    }

    return 0;
}
int index_add(Index *index, const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) return -1;
    if (!S_ISREG(st.st_mode)) return -1;

    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    void *buf = malloc(st.st_size);
    if (!buf) {
        fclose(f);
        return -1;
    }

    if (fread(buf, 1, st.st_size, f) != (size_t)st.st_size) {
        free(buf);
        fclose(f);
        return -1;
    }
    fclose(f);

    ObjectID id;
    if (object_write(0, buf, st.st_size, &id) != 0) {
        free(buf);
        return -1;
    }
    free(buf);

    IndexEntry *e = index_find(index, path);
    if (!e) {
        if (index->count >= MAX_INDEX_ENTRIES) return -1;
        e = &index->entries[index->count++];
        
        strncpy(e->path, path, sizeof(e->path) - 1);
        e->path[sizeof(e->path) - 1] = '\0';
    }

    e->mode = (st.st_mode & S_IXUSR) ? 0100755 : 0100644;
    e->hash = id;
    e->mtime_sec = (uint64_t)st.st_mtime;
    e->size = (size_t)st.st_size;

    return index_save(index);
}
