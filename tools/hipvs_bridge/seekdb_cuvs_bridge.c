/* seekdb <-> hipVS bridge: plain-C CAGRA kNN; all cuVS/ROCm deps live here.
 *
 * Contest optimisation notes (AMD eco track, seekdb x hipVS/cuVS):
 *  1. CAGRA build is tuned through the cuVS *C* parameters object.  The setters
 *     are resolved with dlsym() at run time, so this file still compiles and
 *     links against hipVS releases whose C headers do not export them; when a
 *     symbol is missing the cuVS default is kept (i.e. the frozen baseline
 *     behaviour).  Environment knobs:
 *        OB_CUVS_BUILD_ALGO          auto (default, NN-descent) | ivfpq | default
 *        OB_CUVS_GRAPH_DEGREE        int, unset -> cuVS default (64)
 *        OB_CUVS_INTERMEDIATE_DEGREE int, unset -> cuVS default (128)
 *        OB_CUVS_NN_ITER             int, unset -> cuVS default (20)
 *     NN-descent avoids the IVF-PQ training pass that the cuVS default build
 *     algorithm performs before graph pruning, which shortens CAGRA build for
 *     the L2/flat FP32 datasets used by the contest while keeping (usually
 *     improving) recall at the same final graph degree.
 *  2. Per-search device scratch buffers are pooled per index and reused instead
 *     of being allocated/freed on every knn_search call; the CAGRA
 *     search-parameters object is created once per index.  Concurrent searches
 *     take their own slot, so multi-client QPS does not serialise.
 *  3. Build/search wall times are printed to stderr so the benchmark harness can
 *     attribute time to the GPU stage (OB_CUVS_LOG=0 silences it).
 */
/*
 * clock_gettime()/CLOCK_MONOTONIC (stage timing), dlsym()/RTLD_DEFAULT (optional
 * build-parameter tuning) and pthread_* need a POSIX feature set when the
 * compiler runs in strict ISO mode (-std=c11), so request it before any header.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE 1
#endif
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include "seekdb_cuvs_bridge.h"
#include <cuvs/core/c_api.h>
#include <cuvs/neighbors/cagra.h>
#include <dlpack/dlpack.h>
#ifdef __HIP_PLATFORM_AMD__
#include <cuvs/cuda_runtime.h>
#else
#include <cuda_runtime.h>
#endif
#include <dlfcn.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Number of reusable search scratch slots per index (one per concurrent query). */
#define SK_SLOT_COUNT 8

typedef int (*sk_params_set_int_fn)(cuvsCagraIndexParams_t, int);

typedef struct {
  float *q;      /* device: nq x dim */
  unsigned *n;   /* device: nq x topk */
  float *d;      /* device: nq x topk */
  size_t q_cap;  /* capacities, in elements */
  size_t n_cap;
  size_t d_cap;
  int busy;
} sk_slot;

typedef struct {
  cuvsResources_t res;
  cuvsCagraIndexParams_t ip;
  cuvsCagraIndex_t idx;
  cuvsCagraSearchParams_t sp; /* created once, read-only during search */
  long n, dim;
  pthread_mutex_t mu; /* guards slots[] */
  sk_slot slots[SK_SLOT_COUNT];
} sk_cuvs_index;

static long sk_now_ms(void)
{
  struct timespec ts;
  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) { return 0; }
  return (long)ts.tv_sec * 1000L + (long)(ts.tv_nsec / 1000000L);
}

static int sk_log_enabled(void)
{
  const char *v = getenv("OB_CUVS_LOG");
  return (v == NULL || v[0] != '0');
}

/* ------------------------------------------------------------------------- */
/* Optional build-parameter tuning (never fatal: missing symbol -> default).  */
/* ------------------------------------------------------------------------- */

static void *sk_find_symbol(const char *const *names, int count)
{
  int i = 0;
  for (i = 0; i < count; ++i) {
    void *sym = dlsym(RTLD_DEFAULT, names[i]);
    if (sym != NULL) { return sym; }
  }
  return NULL;
}

static int sk_env_int(const char *name, int def_val, int lo, int hi)
{
  const char *raw = getenv(name);
  char *end = NULL;
  long v = 0;
  if (raw == NULL || raw[0] == '\0') { return def_val; }
  v = strtol(raw, &end, 10);
  if (end == raw) { return def_val; }
  if (v < lo) { v = lo; }
  if (v > hi) { v = hi; }
  return (int)v;
}

/* cuVS enum values: CUVS_CAGRA_GRAPH_BUILD_ALGO_IVF_PQ = 0,
 * CUVS_CAGRA_GRAPH_BUILD_ALGO_NN_DESCENT = 1.  Some hipVS drops ship them as
 * macros; prefer those when they exist. */
#if defined(CUVS_CAGRA_GRAPH_BUILD_ALGO_NN_DESCENT)
#define SK_ALGO_NN_DESCENT ((int)CUVS_CAGRA_GRAPH_BUILD_ALGO_NN_DESCENT)
#else
#define SK_ALGO_NN_DESCENT 1
#endif
#if defined(CUVS_CAGRA_GRAPH_BUILD_ALGO_IVF_PQ)
#define SK_ALGO_IVF_PQ ((int)CUVS_CAGRA_GRAPH_BUILD_ALGO_IVF_PQ)
#else
#define SK_ALGO_IVF_PQ 0
#endif

static void sk_tune_build_params(cuvsCagraIndexParams_t ip)
{
  static const char *algo_syms[] = {
      "cuvsCagraIndexParamsSetBuildAlgo",
      "cuvsCagraIndexParamsSetBuildAlgoType",
      "cuvsCagraIndexParamsSetGraphBuildAlgo"};
  static const char *degree_syms[] = {
      "cuvsCagraIndexParamsSetGraphDegree",
      "cuvsCagraIndexParamsSetDegree"};
  static const char *idegree_syms[] = {
      "cuvsCagraIndexParamsSetIntermediateGraphDegree",
      "cuvsCagraIndexParamsSetIntermediateDegree"};
  static const char *niter_syms[] = {
      "cuvsCagraIndexParamsSetNNDescentNiter",
      "cuvsCagraIndexParamsSetNNDescentIterations",
      "cuvsCagraIndexParamsSetNNIter"};
  const char *algo_env = getenv("OB_CUVS_BUILD_ALGO");
  int want_algo = SK_ALGO_NN_DESCENT;
  int graph_degree = sk_env_int("OB_CUVS_GRAPH_DEGREE", -1, -1, 512);
  int intermediate_degree = sk_env_int("OB_CUVS_INTERMEDIATE_DEGREE", -1, -1, 1024);
  int nn_iter = sk_env_int("OB_CUVS_NN_ITER", -1, -1, 200);
  int applied_algo = -1;
  int applied_degree = -1;
  int applied_intermediate = -1;
  int applied_niter = -1;
  sk_params_set_int_fn fn = NULL;

  if (ip == NULL) { return; }
  if (algo_env != NULL && algo_env[0] != '\0') {
    if (strcmp(algo_env, "ivfpq") == 0 || strcmp(algo_env, "ivf_pq") == 0 ||
        strcmp(algo_env, "0") == 0) {
      want_algo = SK_ALGO_IVF_PQ;
    } else if (strcmp(algo_env, "default") == 0 || strcmp(algo_env, "-1") == 0) {
      want_algo = -1;
    }
  }
  if (want_algo >= 0) {
    fn = (sk_params_set_int_fn)sk_find_symbol(algo_syms, 3);
    if (fn != NULL && fn(ip, want_algo) == 0) { applied_algo = want_algo; }
  }
  if (graph_degree > 0) {
    fn = (sk_params_set_int_fn)sk_find_symbol(degree_syms, 2);
    if (fn != NULL && fn(ip, graph_degree) == 0) { applied_degree = graph_degree; }
  }
  if (intermediate_degree > 0) {
    fn = (sk_params_set_int_fn)sk_find_symbol(idegree_syms, 2);
    if (fn != NULL && fn(ip, intermediate_degree) == 0) {
      applied_intermediate = intermediate_degree;
    }
  }
  if (nn_iter > 0) {
    fn = (sk_params_set_int_fn)sk_find_symbol(niter_syms, 3);
    if (fn != NULL && fn(ip, nn_iter) == 0) { applied_niter = nn_iter; }
  }
  if (sk_log_enabled()) {
    fprintf(stderr,
            "[seekdb_cuvs] build_params algo=%d degree=%d intermediate=%d niter=%d\n",
            applied_algo, applied_degree, applied_intermediate, applied_niter);
  }
}

/* ------------------------------------------------------------------------- */
/* Device scratch slots                                                      */
/* ------------------------------------------------------------------------- */

static void sk_slot_release_dev(sk_cuvs_index *h, sk_slot *s)
{
  if (s->q != NULL) { cuvsRMMFree(h->res, s->q, sizeof(float) * s->q_cap); }
  if (s->n != NULL) { cuvsRMMFree(h->res, s->n, sizeof(unsigned) * s->n_cap); }
  if (s->d != NULL) { cuvsRMMFree(h->res, s->d, sizeof(float) * s->d_cap); }
  s->q = NULL;
  s->n = NULL;
  s->d = NULL;
  s->q_cap = 0;
  s->n_cap = 0;
  s->d_cap = 0;
}

/* Grow the slot so that it can hold nq*dim floats and nq*topk results. */
static int sk_slot_reserve(sk_cuvs_index *h, sk_slot *s, size_t q_need, size_t k_need)
{
  int rc = 0;
  if (s->q == NULL || s->q_cap < q_need) {
    if (s->q != NULL) {
      cuvsRMMFree(h->res, s->q, sizeof(float) * s->q_cap);
      s->q = NULL;
      s->q_cap = 0;
    }
    if (cuvsRMMAlloc(h->res, (void **)&s->q, sizeof(float) * q_need) != CUVS_SUCCESS) {
      s->q = NULL;
      rc = 10;
    } else {
      s->q_cap = q_need;
    }
  }
  if (rc == 0 && (s->n == NULL || s->n_cap < k_need)) {
    if (s->n != NULL) {
      cuvsRMMFree(h->res, s->n, sizeof(unsigned) * s->n_cap);
      s->n = NULL;
      s->n_cap = 0;
    }
    if (cuvsRMMAlloc(h->res, (void **)&s->n, sizeof(unsigned) * k_need) != CUVS_SUCCESS) {
      s->n = NULL;
      rc = 11;
    } else {
      s->n_cap = k_need;
    }
  }
  if (rc == 0 && (s->d == NULL || s->d_cap < k_need)) {
    if (s->d != NULL) {
      cuvsRMMFree(h->res, s->d, sizeof(float) * s->d_cap);
      s->d = NULL;
      s->d_cap = 0;
    }
    if (cuvsRMMAlloc(h->res, (void **)&s->d, sizeof(float) * k_need) != CUVS_SUCCESS) {
      s->d = NULL;
      rc = 12;
    } else {
      s->d_cap = k_need;
    }
  }
  return rc;
}

static sk_slot *sk_slot_acquire(sk_cuvs_index *h, sk_slot *spare, int *pooled)
{
  int i = 0;
  sk_slot *slot = NULL;
  *pooled = 0;
  pthread_mutex_lock(&h->mu);
  for (i = 0; i < SK_SLOT_COUNT; ++i) {
    if (!h->slots[i].busy) {
      h->slots[i].busy = 1;
      slot = &h->slots[i];
      *pooled = 1;
      break;
    }
  }
  pthread_mutex_unlock(&h->mu);
  if (slot == NULL) {
    /* More concurrent searches than slots: use caller-private scratch instead of
     * serialising the GPU. */
    memset(spare, 0, sizeof(*spare));
    slot = spare;
  }
  return slot;
}

static void sk_slot_give_back(sk_cuvs_index *h, sk_slot *slot, int pooled)
{
  if (pooled) {
    pthread_mutex_lock(&h->mu);
    slot->busy = 0;
    pthread_mutex_unlock(&h->mu);
  } else {
    sk_slot_release_dev(h, slot);
  }
}

/* ------------------------------------------------------------------------- */
/* Public C ABI                                                             */
/* ------------------------------------------------------------------------- */

void *seekdb_cuvs_build(const float *base, long n, long dim)
{
  sk_cuvs_index *h = NULL;
  DLManagedTensor dset;
  long ds[2];
  long t0 = 0;
  int i = 0;

  if (base == NULL || n <= 0 || dim <= 0) { return NULL; }
  h = (sk_cuvs_index *)calloc(1, sizeof(*h));
  if (h == NULL) { return NULL; }
  pthread_mutex_init(&h->mu, NULL);
  for (i = 0; i < SK_SLOT_COUNT; ++i) { h->slots[i].busy = 0; }
  if (cuvsResourcesCreate(&h->res) != CUVS_SUCCESS) { free(h); return NULL; }

  ds[0] = n;
  ds[1] = dim;
  memset(&dset, 0, sizeof(dset));
  dset.dl_tensor.data = (void *)base;
  dset.dl_tensor.device.device_type = kDLCPU;
  dset.dl_tensor.device.device_id = 0;
  dset.dl_tensor.ndim = 2;
  dset.dl_tensor.dtype.code = kDLFloat;
  dset.dl_tensor.dtype.bits = 32;
  dset.dl_tensor.dtype.lanes = 1;
  dset.dl_tensor.shape = ds;
  dset.dl_tensor.strides = 0;
  dset.dl_tensor.byte_offset = 0;

  if (cuvsCagraIndexParamsCreate(&h->ip) != CUVS_SUCCESS) { goto fail_res; }
  sk_tune_build_params(h->ip);
  if (cuvsCagraIndexCreate(&h->idx) != CUVS_SUCCESS) { goto fail_ip; }
  t0 = sk_now_ms();
  if (cuvsCagraBuild(h->res, h->ip, &dset, h->idx) != CUVS_SUCCESS) { goto fail_idx; }
  if (sk_log_enabled()) {
    fprintf(stderr, "[seekdb_cuvs] build n=%ld dim=%ld ms=%ld\n", n, dim,
            sk_now_ms() - t0);
  }
  if (cuvsCagraSearchParamsCreate(&h->sp) != CUVS_SUCCESS) { h->sp = NULL; }
  h->n = n;
  h->dim = dim;
  return h;

fail_idx:
  cuvsCagraIndexDestroy(h->idx);
fail_ip:
  cuvsCagraIndexParamsDestroy(h->ip);
fail_res:
  pthread_mutex_destroy(&h->mu);
  cuvsResourcesDestroy(h->res);
  free(h);
  fprintf(stderr, "[seekdb_cuvs] build failed: %s\n", cuvsGetLastErrorText());
  return NULL;
}

int seekdb_cuvs_search(void *handle, const float *query, long nq, long topk,
                       unsigned int *out_ids, float *out_dist)
{
  sk_cuvs_index *h = (sk_cuvs_index *)handle;
  sk_slot spare;
  sk_slot *slot = NULL;
  cuvsCagraSearchParams_t sp = NULL;
  cuvsCagraSearchParams_t tmp_sp = NULL;
  DLManagedTensor qt, nt, dt;
  cuvsFilter filter;
  long qs[2], nsh[2], dsh[2];
  size_t q_need = 0;
  size_t k_need = 0;
  long t0 = 0;
  int pooled = 0;
  int rc = 0;

  if (h == NULL || h->idx == NULL) { return 1; }
  if (query == NULL || out_ids == NULL || nq <= 0 || topk <= 0 || h->dim <= 0) {
    return 2;
  }
  q_need = (size_t)nq * (size_t)h->dim;
  k_need = (size_t)nq * (size_t)topk;
  slot = sk_slot_acquire(h, &spare, &pooled);
  t0 = sk_now_ms();
  rc = sk_slot_reserve(h, slot, q_need, k_need);
  if (rc != 0) { goto done; }
  if (cudaMemcpy(slot->q, query, sizeof(float) * q_need, cudaMemcpyDefault) !=
      cudaSuccess) {
    rc = 13;
    goto done;
  }

  qs[0] = nq;
  qs[1] = h->dim;
  nsh[0] = nq;
  nsh[1] = topk;
  dsh[0] = nq;
  dsh[1] = topk;
  memset(&qt, 0, sizeof(qt));
  qt.dl_tensor.data = slot->q;
  qt.dl_tensor.device.device_type = kDLCUDA;
  qt.dl_tensor.device.device_id = 0;
  qt.dl_tensor.ndim = 2;
  qt.dl_tensor.dtype.code = kDLFloat;
  qt.dl_tensor.dtype.bits = 32;
  qt.dl_tensor.dtype.lanes = 1;
  qt.dl_tensor.shape = qs;
  qt.dl_tensor.strides = 0;
  qt.dl_tensor.byte_offset = 0;
  nt = qt;
  nt.dl_tensor.data = slot->n;
  nt.dl_tensor.dtype.code = kDLUInt;
  nt.dl_tensor.shape = nsh;
  dt = qt;
  dt.dl_tensor.data = slot->d;
  dt.dl_tensor.dtype.code = kDLFloat;
  dt.dl_tensor.shape = dsh;

  sp = h->sp;
  if (sp == NULL) {
    if (cuvsCagraSearchParamsCreate(&tmp_sp) != CUVS_SUCCESS) {
      rc = 14;
      goto done;
    }
    sp = tmp_sp;
  }
  filter.type = NO_FILTER;
  filter.addr = (uintptr_t)0;
  if (cuvsCagraSearch(h->res, sp, h->idx, &qt, &nt, &dt, filter) != CUVS_SUCCESS) {
    rc = 15;
    goto done;
  }
  if (cudaMemcpy(out_ids, slot->n, sizeof(unsigned) * k_need, cudaMemcpyDefault) !=
      cudaSuccess) {
    rc = 16;
    goto done;
  }
  if (out_dist != NULL &&
      cudaMemcpy(out_dist, slot->d, sizeof(float) * k_need, cudaMemcpyDefault) !=
          cudaSuccess) {
    rc = 17;
  }

done:
  if (tmp_sp != NULL) { cuvsCagraSearchParamsDestroy(tmp_sp); }
  sk_slot_give_back(h, slot, pooled);
  if (rc == 0) {
    if (sk_log_enabled()) {
      fprintf(stderr, "[seekdb_cuvs] search nq=%ld topk=%ld ms=%ld\n", nq, topk,
              sk_now_ms() - t0);
    }
  } else {
    fprintf(stderr, "[seekdb_cuvs] search rc=%d: %s\n", rc, cuvsGetLastErrorText());
  }
  return rc;
}

void seekdb_cuvs_free(void *handle)
{
  sk_cuvs_index *h = (sk_cuvs_index *)handle;
  int i = 0;
  if (h == NULL) { return; }
  for (i = 0; i < SK_SLOT_COUNT; ++i) { sk_slot_release_dev(h, &h->slots[i]); }
  if (h->sp != NULL) { cuvsCagraSearchParamsDestroy(h->sp); }
  if (h->idx != NULL) { cuvsCagraIndexDestroy(h->idx); }
  if (h->ip != NULL) { cuvsCagraIndexParamsDestroy(h->ip); }
  pthread_mutex_destroy(&h->mu);
  if (h->res != NULL) { cuvsResourcesDestroy(h->res); }
  free(h);
}

int seekdb_cuvs_cagra_knn(const float *base, long n, long dim,
                          const float *query, long nq, long topk,
                          unsigned int *out_ids)
{
  void *h = seekdb_cuvs_build(base, n, dim);
  int rc = 10;
  if (h == NULL) { return rc; }
  rc = seekdb_cuvs_search(h, query, nq, topk, out_ids, NULL);
  seekdb_cuvs_free(h);
  return rc;
}
