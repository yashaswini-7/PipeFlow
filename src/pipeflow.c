/* Pipeflow - Module-I: bounded queue + 3-stage pipeline (source -> transform -> sink) */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <pthread.h>

#define QCAP 4
#define LINE 256

typedef struct {
    char *items[QCAP];
    int head, tail, count, closed;
    pthread_mutex_t m;
    pthread_cond_t not_full, not_empty;
} queue_t;

static void q_init(queue_t *q) {
    memset(q, 0, sizeof(*q));
    pthread_mutex_init(&q->m, NULL);
    pthread_cond_init(&q->not_full, NULL);
    pthread_cond_init(&q->not_empty, NULL);
}

static void q_push(queue_t *q, char *s) {
    pthread_mutex_lock(&q->m);
    while (q->count == QCAP)                 /* back-pressure: wait if full */
        pthread_cond_wait(&q->not_full, &q->m);
    q->items[q->tail] = s;
    q->tail = (q->tail + 1) % QCAP;
    q->count++;
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->m);
}

static char *q_pop(queue_t *q) {
    pthread_mutex_lock(&q->m);
    while (q->count == 0 && !q->closed)
        pthread_cond_wait(&q->not_empty, &q->m);
    if (q->count == 0) { pthread_mutex_unlock(&q->m); return NULL; }
    char *s = q->items[q->head];
    q->head = (q->head + 1) % QCAP;
    q->count--;
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->m);
    return s;
}

static void q_close(queue_t *q) {
    pthread_mutex_lock(&q->m);
    q->closed = 1;
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->m);
}

static queue_t q1, q2;
static const char *infile;

static void *source(void *arg) {
    FILE *f = fopen(infile, "r");
    if (!f) { perror("open"); q_close(&q1); return NULL; }
    char buf[LINE];
    while (fgets(buf, LINE, f)) {
        buf[strcspn(buf, "\n")] = 0;
        q_push(&q1, strdup(buf));
    }
    fclose(f);
    q_close(&q1);
    return NULL;
}

static void *transform(void *arg) {
    char *s;
    while ((s = q_pop(&q1)) != NULL) {
        for (char *p = s; *p; p++) *p = toupper((unsigned char)*p);
        q_push(&q2, s);
    }
    q_close(&q2);
    return NULL;
}

static void *sink(void *arg) {
    char *s; int n = 0;
    while ((s = q_pop(&q2)) != NULL) {
        printf("[sink] %2d: %s\n", ++n, s);
        free(s);
    }
    printf("[sink] total records = %d\n", n);
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc < 2) { printf("Usage: %s <input file>\n", argv[0]); return 1; }
    infile = argv[1];
    q_init(&q1); q_init(&q2);
    pthread_t t[3];
    pthread_create(&t[0], NULL, source, NULL);
    pthread_create(&t[1], NULL, transform, NULL);
    pthread_create(&t[2], NULL, sink, NULL);
    for (int i = 0; i < 3; i++) pthread_join(t[i], NULL);
    return 0;
}
