/* seekdb <-> hipVS bridge: plain-C CAGRA kNN; all cuVS/ROCm deps live here. */
#include "seekdb_cuvs_bridge.h"
#include <cuvs/core/c_api.h>
#include <cuvs/neighbors/cagra.h>
#include <dlpack/dlpack.h>
#ifdef __HIP_PLATFORM_AMD__
#include <cuvs/cuda_runtime.h>
#else
#include <cuda_runtime.h>
#endif
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Reuse device scratch across searches on the same index.  The baseline
 * allocated/freed three buffers per query, which dominated small-query latency.
 * The mutex protects only slot bookkeeping, never the GPU search itself. */
#define SK_SCRATCH_SLOTS 4

typedef struct {
  float* queries;
  unsigned* ids;
  float* dists;
  size_t query_elems;
  size_t result_elems;
  int busy;
} sk_scratch;

typedef struct {
  cuvsResources_t        res;
  cuvsCagraIndexParams_t ip;
  cuvsCagraIndex_t       idx;
  long n, dim;
  pthread_mutex_t scratch_mu;
  sk_scratch scratch[SK_SCRATCH_SLOTS];
} sk_cuvs_index;

static sk_scratch* sk_acquire_scratch(sk_cuvs_index* h, sk_scratch* fallback, int* pooled)
{
  int i = 0;
  sk_scratch* s = NULL;
  *pooled = 0;
  pthread_mutex_lock(&h->scratch_mu);
  for (i = 0; i < SK_SCRATCH_SLOTS; ++i) {
    if (!h->scratch[i].busy) {
      h->scratch[i].busy = 1;
      s = &h->scratch[i];
      *pooled = 1;
      break;
    }
  }
  pthread_mutex_unlock(&h->scratch_mu);
  if (s == NULL) {
    memset(fallback, 0, sizeof(*fallback));
    s = fallback;
  }
  return s;
}

static void sk_release_scratch(sk_cuvs_index* h, sk_scratch* s, int pooled)
{
  if (pooled) {
    pthread_mutex_lock(&h->scratch_mu);
    s->busy = 0;
    pthread_mutex_unlock(&h->scratch_mu);
  } else {
    if (s->queries) cuvsRMMFree(h->res, s->queries, sizeof(float) * s->query_elems);
    if (s->ids) cuvsRMMFree(h->res, s->ids, sizeof(unsigned) * s->result_elems);
    if (s->dists) cuvsRMMFree(h->res, s->dists, sizeof(float) * s->result_elems);
  }
}

static int sk_reserve_scratch(sk_cuvs_index* h, sk_scratch* s, size_t q_elems, size_t k_elems)
{
  if (s->query_elems < q_elems) {
    if (s->queries) cuvsRMMFree(h->res, s->queries, sizeof(float) * s->query_elems);
    s->queries = NULL;
    s->query_elems = 0;
    if (cuvsRMMAlloc(h->res, (void**)&s->queries, sizeof(float) * q_elems) != CUVS_SUCCESS) return 10;
    s->query_elems = q_elems;
  }
  if (s->result_elems < k_elems) {
    if (s->ids) cuvsRMMFree(h->res, s->ids, sizeof(unsigned) * s->result_elems);
    if (s->dists) cuvsRMMFree(h->res, s->dists, sizeof(float) * s->result_elems);
    s->ids = NULL;
    s->dists = NULL;
    s->result_elems = 0;
    if (cuvsRMMAlloc(h->res, (void**)&s->ids, sizeof(unsigned) * k_elems) != CUVS_SUCCESS) return 11;
    s->result_elems = k_elems;
    if (cuvsRMMAlloc(h->res, (void**)&s->dists, sizeof(float) * k_elems) != CUVS_SUCCESS) {
      cuvsRMMFree(h->res, s->ids, sizeof(unsigned) * s->result_elems);
      s->ids = NULL;
      s->result_elems = 0;
      return 12;
    }
  }
  return 0;
}

void* seekdb_cuvs_build(const float* base, long n, long dim)
{
  sk_cuvs_index* h = (sk_cuvs_index*)calloc(1, sizeof(*h));
  if (!h) return NULL;
  if (base == NULL || n <= 0 || dim <= 0) { free(h); return NULL; }
  pthread_mutex_init(&h->scratch_mu, NULL);
  if (cuvsResourcesCreate(&h->res) != CUVS_SUCCESS) { free(h); return NULL; }

  DLManagedTensor dset; long ds[2] = {n, dim};
  dset.dl_tensor.data = (void*)base;
  dset.dl_tensor.device.device_type = kDLCPU; dset.dl_tensor.device.device_id = 0;
  dset.dl_tensor.ndim = 2;
  dset.dl_tensor.dtype.code = kDLFloat; dset.dl_tensor.dtype.bits = 32; dset.dl_tensor.dtype.lanes = 1;
  dset.dl_tensor.shape = ds; dset.dl_tensor.strides = 0; dset.dl_tensor.byte_offset = 0;

  if (cuvsCagraIndexParamsCreate(&h->ip) != CUVS_SUCCESS) goto fail_res;
  if (cuvsCagraIndexCreate(&h->idx)      != CUVS_SUCCESS) goto fail_ip;
  if (cuvsCagraBuild(h->res, h->ip, &dset, h->idx) != CUVS_SUCCESS) goto fail_idx;
  h->n = n; h->dim = dim;
  return h;

fail_idx: cuvsCagraIndexDestroy(h->idx);
fail_ip:  cuvsCagraIndexParamsDestroy(h->ip);
fail_res: pthread_mutex_destroy(&h->scratch_mu);
  cuvsResourcesDestroy(h->res); free(h);
  fprintf(stderr, "[seekdb_cuvs] build failed: %s\n", cuvsGetLastErrorText());
  return NULL;
}

int seekdb_cuvs_search(void* handle, const float* query, long nq, long topk,
                       unsigned int* out_ids, float* out_dist)
{
  sk_cuvs_index* h = (sk_cuvs_index*)handle;
  sk_scratch fallback;
  sk_scratch* s = NULL;
  cuvsCagraSearchParams_t sp = 0;
  size_t q_elems = 0, k_elems = 0;
  int rc = 0, pooled = 0;
  if (!h) return 1;
  if (query == NULL || out_ids == NULL || nq <= 0 || topk <= 0 || h->dim <= 0) return 2;

  q_elems = (size_t)nq * (size_t)h->dim;
  k_elems = (size_t)nq * (size_t)topk;
  s = sk_acquire_scratch(h, &fallback, &pooled);
  rc = sk_reserve_scratch(h, s, q_elems, k_elems);
  if (rc != 0) goto done;
  if (cudaMemcpy(s->queries, query, sizeof(float)*q_elems, cudaMemcpyDefault) != cudaSuccess) { rc=13; goto done; }
  {
    DLManagedTensor qt, nt, dt; long qs[2]={nq,h->dim}, nsh[2]={nq,topk}, dsh[2]={nq,topk};
    memset(&qt, 0, sizeof(qt));
    qt.dl_tensor.data=s->queries; qt.dl_tensor.device.device_type=kDLCUDA; qt.dl_tensor.device.device_id=0;
    qt.dl_tensor.ndim=2; qt.dl_tensor.dtype.code=kDLFloat; qt.dl_tensor.dtype.bits=32; qt.dl_tensor.dtype.lanes=1;
    qt.dl_tensor.shape=qs; qt.dl_tensor.strides=0; qt.dl_tensor.byte_offset=0;
    nt=qt; nt.dl_tensor.data=s->ids; nt.dl_tensor.dtype.code=kDLUInt; nt.dl_tensor.shape=nsh;
    dt=qt; dt.dl_tensor.data=s->dists; dt.dl_tensor.dtype.code=kDLFloat; dt.dl_tensor.shape=dsh;
    if (cuvsCagraSearchParamsCreate(&sp) != CUVS_SUCCESS) { rc=14; goto done; }
    cuvsFilter f; f.type=NO_FILTER; f.addr=(uintptr_t)0;
    if (cuvsCagraSearch(h->res, sp, h->idx, &qt, &nt, &dt, f) != CUVS_SUCCESS) { rc=15; goto done; }
    if (cudaMemcpy(out_ids, s->ids, sizeof(unsigned)*k_elems, cudaMemcpyDefault) != cudaSuccess) { rc=16; goto done; }
    if (out_dist) cudaMemcpy(out_dist, s->dists, sizeof(float)*k_elems, cudaMemcpyDefault);
  }
done:
  if (sp) cuvsCagraSearchParamsDestroy(sp);
  sk_release_scratch(h, s, pooled);
  if (rc) fprintf(stderr, "[seekdb_cuvs] search rc=%d: %s\n", rc, cuvsGetLastErrorText());
  return rc;
}

void seekdb_cuvs_free(void* handle)
{
  sk_cuvs_index* h = (sk_cuvs_index*)handle;
  int i = 0;
  if (!h) return;
  for (i = 0; i < SK_SCRATCH_SLOTS; ++i) {
    if (h->scratch[i].queries) cuvsRMMFree(h->res, h->scratch[i].queries, sizeof(float) * h->scratch[i].query_elems);
    if (h->scratch[i].ids) cuvsRMMFree(h->res, h->scratch[i].ids, sizeof(unsigned) * h->scratch[i].result_elems);
    if (h->scratch[i].dists) cuvsRMMFree(h->res, h->scratch[i].dists, sizeof(float) * h->scratch[i].result_elems);
  }
  cuvsCagraIndexDestroy(h->idx);
  cuvsCagraIndexParamsDestroy(h->ip);
  pthread_mutex_destroy(&h->scratch_mu);
  cuvsResourcesDestroy(h->res);
  free(h);
}

int seekdb_cuvs_cagra_knn(const float* base, long n, long dim,
                          const float* query, long nq, long topk,
                          unsigned int* out_ids)
{
  void* h = seekdb_cuvs_build(base, n, dim);
  if (!h) return 10;
  int rc = seekdb_cuvs_search(h, query, nq, topk, out_ids, NULL);
  seekdb_cuvs_free(h);
  return rc;
}
