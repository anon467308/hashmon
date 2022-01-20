#define CAML_NAME_SPACE
#include <caml/alloc.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <caml/memory.h>
#include <caml/mlvalues.h>
#include <immintrin.h>
#include "clhash.h"

// CLHash wrapper

static void *keyptr_val(value v)
{
  return *((void **) Data_custom_val(v));
}

static void finalize_key(value v)
{
  free(keyptr_val(v));
}

static struct custom_operations key_ops = {
  "ch.ethz.infsec.monpoly.clhash.key",
  finalize_key,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default,
  custom_compare_ext_default,
  custom_fixed_length_default
};

CAMLprim value clhash_random_key(value seed1, value seed2)
{
  CAMLparam2(seed1, seed2);
  CAMLlocal1(wrapper);
  void *key = get_random_key_for_clhash(Int64_val(seed1), Int64_val(seed2));
  if (!key) {
    caml_failwith("[clhash_random_key] could not generate random key");
  }
  wrapper = caml_alloc_custom_mem(&key_ops, sizeof(key), RANDOM_BYTES_NEEDED_FOR_CLHASH);
  *((void **) Data_custom_val(wrapper)) = key;
  CAMLreturn(wrapper);
}

CAMLprim value clhash_int(value key, value v)
{
  CAMLparam2(key, v);
  intnat x = Long_val(v);
  uint64_t hash = clhash(keyptr_val(key), (char *)(&x), sizeof(x));
  CAMLreturn(Val_long(hash));
}

CAMLprim value clhash_float(value key, value v)
{
  CAMLparam2(key, v);
  double x = Double_val(v);
  uint64_t hash = clhash(keyptr_val(key), (char *)(&x), sizeof(x));
  CAMLreturn(Val_long(hash));
}

CAMLprim value clhash_string(value key, value v)
{
  CAMLparam2(key, v);
  uint64_t hash = clhash(keyptr_val(key), String_val(v), caml_string_length(v));
  CAMLreturn(Val_long(hash));
}

CAMLprim value clhash_collision_prob(value v)
{
  CAMLparam1(v);
  intnat max_bits = 8 * sizeof(value) - 1;
  intnat bits = Long_val(v);
  intnat loss = (0 < bits && bits <= max_bits) ? max_bits - bits + 1 : 1;
  // Lemma 1 + Lemma 9 from the CLHASH paper
  double prob = 2.004 * (0x1p-64 * (double)(1ull << loss));
  CAMLreturn(caml_copy_double(prob));
}

// Efficient multiplication in GF(2^n) for 1 <= n <= 63
// Requires SSE2 and the PCLMUL instruction.

// Table of irreducible polynomials for each n.
// Source: Joerg Arndt, https://www.jjj.de/mathdata/minweight-irredpoly.txt
static const uint64_t irred_polys[] = {
  0x0000000000000003ULL,
  0x0000000000000007ULL,
  0x000000000000000bULL,
  0x0000000000000013ULL,
  0x0000000000000025ULL,
  0x0000000000000043ULL,
  0x0000000000000083ULL,
  0x000000000000011bULL,
  0x0000000000000203ULL,
  0x0000000000000409ULL,
  0x0000000000000805ULL,
  0x0000000000001009ULL,
  0x000000000000201bULL,
  0x0000000000004021ULL,
  0x0000000000008003ULL,
  0x000000000001002bULL,
  0x0000000000020009ULL,
  0x0000000000040009ULL,
  0x0000000000080027ULL,
  0x0000000000100009ULL,
  0x0000000000200005ULL,
  0x0000000000400003ULL,
  0x0000000000800021ULL,
  0x000000000100001bULL,
  0x0000000002000009ULL,
  0x000000000400001bULL,
  0x0000000008000027ULL,
  0x0000000010000003ULL,
  0x0000000020000005ULL,
  0x0000000040000003ULL,
  0x0000000080000009ULL,
  0x000000010000008dULL,
  0x0000000200000401ULL,
  0x0000000400000081ULL,
  0x0000000800000005ULL,
  0x0000001000000201ULL,
  0x0000002000000053ULL,
  0x0000004000000063ULL,
  0x0000008000000011ULL,
  0x0000010000000039ULL,
  0x0000020000000009ULL,
  0x0000040000000081ULL,
  0x0000080000000059ULL,
  0x0000100000000021ULL,
  0x000020000000001bULL,
  0x0000400000000003ULL,
  0x0000800000000021ULL,
  0x000100000000002dULL,
  0x0002000000000201ULL,
  0x000400000000001dULL,
  0x000800000000004bULL,
  0x0010000000000009ULL,
  0x0020000000000047ULL,
  0x0040000000000201ULL,
  0x0080000000000081ULL,
  0x0100000000000095ULL,
  0x0200000000000011ULL,
  0x0400000000080001ULL,
  0x0800000000000095ULL,
  0x1000000000000003ULL,
  0x2000000000000027ULL,
  0x4000000020000001ULL,
  0x8000000000000003ULL,
};

// The following function is taken from the article
// Daniel Lemire, Owen Kaser: Strongly Universal String Hashing is Fast. Comput. J. 57(11): 1624-1638 (2014)
// (Appendix 2)
static inline uint64_t barrett_reduction(__m128i x, int n)
{
  uint64_t p = irred_polys[n-1];
  __m128i c = _mm_set_epi64x(0, p);
  __m128i q1 = _mm_srli_epi64(x, n);
  __m128i q2 = _mm_clmulepi64_si128(q1, c, 0x00);
  __m128i q3 = _mm_srli_epi64(q2, n);
  __m128i r = _mm_xor_si128(x, _mm_clmulepi64_si128(q3, c, 0x00));
  return _mm_cvtsi128_si64(r) & ((1ULL << n) - 1);
}

CAMLprim value clhash_gf_mul(value nval, value a, value b)
{
  CAMLparam3(nval, a, b);
  intnat n = Long_val(nval);
  if (n < 1 || n > 63) {
    caml_invalid_argument("[clhash_gf_mul] degree must be between 1 and 63");
  }
  __m128i temp = _mm_set_epi64x(Long_val(a), Long_val(b));
  __m128i prod = _mm_clmulepi64_si128(temp, temp, 0x10);
  CAMLreturn(Val_long(barrett_reduction(prod, n)));
}
