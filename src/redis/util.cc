//
// Created by zhanghao on 2018/6/17.
//

#include "util.h"

#if AVOID_ERRNO
# define SET_ERRNO(n)
#else
# include <errno.h>
# define SET_ERRNO(n) errno = (n)
#endif

#if USE_REP_MOVSB /* small win on amd, big loss on intel */
#if (__i386 || __amd64) && __GNUC__ >= 3
# define lzf_movsb(dst, src, len)                \
   asm ("rep movsb"                              \
        : "=D" (dst), "=S" (src), "=c" (len)     \
        :  "0" (dst),  "1" (src),  "2" (len));
#endif
#endif

unsigned int lzfDecompress(const void *const in_data, unsigned int in_len,
	void *out_data, unsigned int out_len)
{
	u8 const *ip = (const u8 *)in_data;
	u8       *op = (u8 *)out_data;
	u8 const *const in_end = ip + in_len;
	u8       *const out_end = op + out_len;

	do
	{
		unsigned int ctrl = *ip++;

		if (ctrl < (1 << 5)) /* literal run */
		{
			ctrl++;

			if (op + ctrl > out_end)
			{
				SET_ERRNO(E2BIG);
				return 0;
			}

#if CHECK_INPUT
			if (ip + ctrl > in_end)
			{
				SET_ERRNO(EINVAL);
				return 0;
			}
#endif

#ifdef lzf_movsb
			lzf_movsb(op, ip, ctrl);
#else
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wimplicit-fallthrough"
			switch (ctrl)
			{
			case 32: *op++ = *ip++; case 31: *op++ = *ip++; case 30: *op++ = *ip++; case 29: *op++ = *ip++;
			case 28: *op++ = *ip++; case 27: *op++ = *ip++; case 26: *op++ = *ip++; case 25: *op++ = *ip++;
			case 24: *op++ = *ip++; case 23: *op++ = *ip++; case 22: *op++ = *ip++; case 21: *op++ = *ip++;
			case 20: *op++ = *ip++; case 19: *op++ = *ip++; case 18: *op++ = *ip++; case 17: *op++ = *ip++;
			case 16: *op++ = *ip++; case 15: *op++ = *ip++; case 14: *op++ = *ip++; case 13: *op++ = *ip++;
			case 12: *op++ = *ip++; case 11: *op++ = *ip++; case 10: *op++ = *ip++; case  9: *op++ = *ip++;
			case  8: *op++ = *ip++; case  7: *op++ = *ip++; case  6: *op++ = *ip++; case  5: *op++ = *ip++;
			case  4: *op++ = *ip++; case  3: *op++ = *ip++; case  2: *op++ = *ip++; case  1: *op++ = *ip++;
			}
#pragma GCC diagnostic pop
#endif
		}
		else /* back reference */
		{
			unsigned int len = ctrl >> 5;

			u8 *ref = op - ((ctrl & 0x1f) << 8) - 1;

#if CHECK_INPUT
			if (ip >= in_end)
			{
				SET_ERRNO(EINVAL);
				return 0;
			}
#endif
			if (len == 7)
			{
				len += *ip++;
#if CHECK_INPUT
				if (ip >= in_end)
				{
					SET_ERRNO(EINVAL);
					return 0;
				}
#endif
			}

			ref -= *ip++;

			if (op + len + 2 > out_end)
			{
				SET_ERRNO(E2BIG);
				return 0;
			}

			if (ref < (u8 *)out_data)
			{
				SET_ERRNO(EINVAL);
				return 0;
			}

#ifdef lzf_movsb
			len += 2;
			lzf_movsb(op, ref, len);
#else
			switch (len)
			{
			default:
				len += 2;

				if (op >= ref + len)
				{
					/* disjunct areas */
					memcpy(op, ref, len);
					op += len;
				}
				else
				{
					/* overlapping, use octte by octte copying */
					do
						*op++ = *ref++;
					while (--len);
				}

				break;

			case 9: *op++ = *ref++; /* fall-thru */
			case 8: *op++ = *ref++; /* fall-thru */
			case 7: *op++ = *ref++; /* fall-thru */
			case 6: *op++ = *ref++; /* fall-thru */
			case 5: *op++ = *ref++; /* fall-thru */
			case 4: *op++ = *ref++; /* fall-thru */
			case 3: *op++ = *ref++; /* fall-thru */
			case 2: *op++ = *ref++; /* fall-thru */
			case 1: *op++ = *ref++; /* fall-thru */
			case 0: *op++ = *ref++; /* two octets more */
				*op++ = *ref++; /* fall-thru */
			}
#endif
		}
	} while (ip < in_end);

	return op - (u8 *)out_data;
}

#define HSIZE (1 << (HLOG))

/*
 * don't play with this unless you benchmark!
 * the data format is not dependent on the hash function.
 * the hash function might seem strange, just believe me,
 * it works ;)
 */
#ifndef FRST
# define FRST(p) (((p[0]) << 8) | p[1])
# define NEXT(v,p) (((v) << 8) | p[2])
# if ULTRA_FAST
#  define IDX(h) ((( h             >> (3*8 - HLOG)) - h  ) & (HSIZE - 1))
# elif VERY_FAST
#  define IDX(h) ((( h             >> (3*8 - HLOG)) - h*5) & (HSIZE - 1))
# else
#  define IDX(h) ((((h ^ (h << 5)) >> (3*8 - HLOG)) - h*5) & (HSIZE - 1))
# endif
#endif
 /*
  * IDX works because it is very similar to a multiplicative hash, e.g.
  * ((h * 57321 >> (3*8 - HLOG)) & (HSIZE - 1))
  * the latter is also quite fast on newer CPUs, and compresses similarly.
  *
  * the next one is also quite good, albeit slow ;)
  * (int)(cos(h & 0xffffff) * 1e6)
  */

#if 0
  /* original lzv-like hash function, much worse and thus slower */
# define FRST(p) (p[0] << 5) ^ p[1]
# define NEXT(v,p) ((v) << 5) ^ p[2]
# define IDX(h) ((h) & (HSIZE - 1))
#endif

#define        MAX_LIT        (1 <<  5)
#define        MAX_OFF        (1 << 13)
#define        MAX_REF        ((1 << 8) + (1 << 3))

#if __GNUC__ >= 3
# define expect(expr,value)         __builtin_expect ((expr),(value))
# define inline                     inline
#else
# define expect(expr,value)         (expr)
# define inline                     static
#endif

#define expect_false(expr) expect ((expr) != 0, 0)
#define expect_true(expr)  expect ((expr) != 0, 1)

/*
 * compressed format
 *
 * 000LLLLL <L+1>    ; literal, L+1=1..33 octets
 * LLLooooo oooooooo ; backref L+1=1..7 octets, o+1=1..4096 offset
 * 111ooooo LLLLLLLL oooooooo ; backref L+8 octets, o+1=1..4096 offset
 *
 */

unsigned int lzfCompress(const void *const in_data, unsigned int in_len,
	void *out_data, unsigned int out_len
#if LZF_STATE_ARG
	, LZF_STATE htab
#endif
)
{
#if !LZF_STATE_ARG
	LZF_STATE htab;
#endif
	const u8 *ip = (const u8 *)in_data;
	u8 *op = (u8 *)out_data;
	const u8 *in_end = ip + in_len;
	u8 *out_end = op + out_len;
	const u8 *ref;

	/* off requires a type wide enough to hold a general pointer difference.
	 * ISO C doesn't have that (size_t might not be enough and ptrdiff_t only
	 * works for differences within a single object). We also assume that no
	 * no bit pattern traps. Since the only platform that is both non-POSIX
	 * and fails to support both assumptions is windows 64 bit, we make a
	 * special workaround for it.
	 */
#if defined (WIN32) && defined (_M_X64)
	unsigned _int64 off; /* workaround for missing POSIX compliance */
#else
	unsigned long off;
#endif
	unsigned int hval;
	int lit;

	if (!in_len || !out_len)
		return 0;

#if INIT_HTAB
	memset(htab, 0, sizeof(htab));
#endif

	lit = 0; op++; /* start run */

	hval = FRST(ip);
	while (ip < in_end - 2)
	{
		LZF_HSLOT *hslot;

		hval = NEXT(hval, ip);
		hslot = htab + IDX(hval);
		ref = *hslot + LZF_HSLOT_BIAS; *hslot = ip - LZF_HSLOT_BIAS;

		if (1
#if INIT_HTAB
			&& ref < ip /* the next test will actually take care of this, but this is faster */
#endif
			&& (off = ip - ref - 1) < MAX_OFF
			&& ref > (u8 *)in_data
			&& ref[2] == ip[2]
#if STRICT_ALIGN
			&& ((ref[1] << 8) | ref[0]) == ((ip[1] << 8) | ip[0])
#else
			&& *(u16 *)ref == *(u16 *)ip
#endif
			)
		{
			/* match found at *ref++ */
			unsigned int len = 2;
			unsigned int maxlen = in_end - ip - len;
			maxlen = maxlen > MAX_REF ? MAX_REF : maxlen;

			if (expect_false(op + 3 + 1 >= out_end)) /* first a faster conservative test */
				if (op - !lit + 3 + 1 >= out_end) /* second the exact but rare test */
					return 0;

			op[-lit - 1] = lit - 1; /* stop run */
			op -= !lit; /* undo run if length is zero */

			for (;;)
			{
				if (expect_true(maxlen > 16))
				{
					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;

					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;

					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;

					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;
				}

				do
					len++;
				while (len < maxlen && ref[len] == ip[len]);

				break;
			}

			len -= 2; /* len is now #octets - 1 */
			ip++;

			if (len < 7)
			{
				*op++ = (off >> 8) + (len << 5);
			}
			else
			{
				*op++ = (off >> 8) + (7 << 5);
				*op++ = len - 7;
			}

			*op++ = off;

			lit = 0; op++; /* start run */

			ip += len + 1;

			if (expect_false(ip >= in_end - 2))
				break;

#if ULTRA_FAST || VERY_FAST
			--ip;
# if VERY_FAST && !ULTRA_FAST
			--ip;
# endif
			hval = FRST(ip);

			hval = NEXT(hval, ip);
			htab[IDX(hval)] = ip - LZF_HSLOT_BIAS;
			ip++;

# if VERY_FAST && !ULTRA_FAST
			hval = NEXT(hval, ip);
			htab[IDX(hval)] = ip - LZF_HSLOT_BIAS;
			ip++;
# endif
#else
			ip -= len + 1;

			do
			{
				hval = NEXT(hval, ip);
				htab[IDX(hval)] = ip - LZF_HSLOT_BIAS;
				ip++;
			} while (len--);
#endif
		}
		else
		{
			/* one more literal byte we must copy */
			if (expect_false(op >= out_end))
				return 0;

			lit++; *op++ = *ip++;

			if (expect_false(lit == MAX_LIT))
			{
				op[-lit - 1] = lit - 1; /* stop run */
				lit = 0; op++; /* start run */
			}
		}
	}

	if (op + 3 > out_end) /* at most 3 bytes can be missing here */
		return 0;

	while (ip < in_end)
	{
		lit++; *op++ = *ip++;

		if (expect_false(lit == MAX_LIT))
		{
			op[-lit - 1] = lit - 1; /* stop run */
			lit = 0; op++; /* start run */
		}
	}

	op[-lit - 1] = lit - 1; /* end run */
	op -= !lit; /* undo run if length is zero */

	return op - (u8 *)out_data;
}


static const uint64_t crc64_tab[256] = {
	UINT64_C(0x0000000000000000), UINT64_C(0x7ad870c830358979),
	UINT64_C(0xf5b0e190606b12f2), UINT64_C(0x8f689158505e9b8b),
	UINT64_C(0xc038e5739841b68f), UINT64_C(0xbae095bba8743ff6),
	UINT64_C(0x358804e3f82aa47d), UINT64_C(0x4f50742bc81f2d04),
	UINT64_C(0xab28ecb46814fe75), UINT64_C(0xd1f09c7c5821770c),
	UINT64_C(0x5e980d24087fec87), UINT64_C(0x24407dec384a65fe),
	UINT64_C(0x6b1009c7f05548fa), UINT64_C(0x11c8790fc060c183),
	UINT64_C(0x9ea0e857903e5a08), UINT64_C(0xe478989fa00bd371),
	UINT64_C(0x7d08ff3b88be6f81), UINT64_C(0x07d08ff3b88be6f8),
	UINT64_C(0x88b81eabe8d57d73), UINT64_C(0xf2606e63d8e0f40a),
	UINT64_C(0xbd301a4810ffd90e), UINT64_C(0xc7e86a8020ca5077),
	UINT64_C(0x4880fbd87094cbfc), UINT64_C(0x32588b1040a14285),
	UINT64_C(0xd620138fe0aa91f4), UINT64_C(0xacf86347d09f188d),
	UINT64_C(0x2390f21f80c18306), UINT64_C(0x594882d7b0f40a7f),
	UINT64_C(0x1618f6fc78eb277b), UINT64_C(0x6cc0863448deae02),
	UINT64_C(0xe3a8176c18803589), UINT64_C(0x997067a428b5bcf0),
	UINT64_C(0xfa11fe77117cdf02), UINT64_C(0x80c98ebf2149567b),
	UINT64_C(0x0fa11fe77117cdf0), UINT64_C(0x75796f2f41224489),
	UINT64_C(0x3a291b04893d698d), UINT64_C(0x40f16bccb908e0f4),
	UINT64_C(0xcf99fa94e9567b7f), UINT64_C(0xb5418a5cd963f206),
	UINT64_C(0x513912c379682177), UINT64_C(0x2be1620b495da80e),
	UINT64_C(0xa489f35319033385), UINT64_C(0xde51839b2936bafc),
	UINT64_C(0x9101f7b0e12997f8), UINT64_C(0xebd98778d11c1e81),
	UINT64_C(0x64b116208142850a), UINT64_C(0x1e6966e8b1770c73),
	UINT64_C(0x8719014c99c2b083), UINT64_C(0xfdc17184a9f739fa),
	UINT64_C(0x72a9e0dcf9a9a271), UINT64_C(0x08719014c99c2b08),
	UINT64_C(0x4721e43f0183060c), UINT64_C(0x3df994f731b68f75),
	UINT64_C(0xb29105af61e814fe), UINT64_C(0xc849756751dd9d87),
	UINT64_C(0x2c31edf8f1d64ef6), UINT64_C(0x56e99d30c1e3c78f),
	UINT64_C(0xd9810c6891bd5c04), UINT64_C(0xa3597ca0a188d57d),
	UINT64_C(0xec09088b6997f879), UINT64_C(0x96d1784359a27100),
	UINT64_C(0x19b9e91b09fcea8b), UINT64_C(0x636199d339c963f2),
	UINT64_C(0xdf7adabd7a6e2d6f), UINT64_C(0xa5a2aa754a5ba416),
	UINT64_C(0x2aca3b2d1a053f9d), UINT64_C(0x50124be52a30b6e4),
	UINT64_C(0x1f423fcee22f9be0), UINT64_C(0x659a4f06d21a1299),
	UINT64_C(0xeaf2de5e82448912), UINT64_C(0x902aae96b271006b),
	UINT64_C(0x74523609127ad31a), UINT64_C(0x0e8a46c1224f5a63),
	UINT64_C(0x81e2d7997211c1e8), UINT64_C(0xfb3aa75142244891),
	UINT64_C(0xb46ad37a8a3b6595), UINT64_C(0xceb2a3b2ba0eecec),
	UINT64_C(0x41da32eaea507767), UINT64_C(0x3b024222da65fe1e),
	UINT64_C(0xa2722586f2d042ee), UINT64_C(0xd8aa554ec2e5cb97),
	UINT64_C(0x57c2c41692bb501c), UINT64_C(0x2d1ab4dea28ed965),
	UINT64_C(0x624ac0f56a91f461), UINT64_C(0x1892b03d5aa47d18),
	UINT64_C(0x97fa21650afae693), UINT64_C(0xed2251ad3acf6fea),
	UINT64_C(0x095ac9329ac4bc9b), UINT64_C(0x7382b9faaaf135e2),
	UINT64_C(0xfcea28a2faafae69), UINT64_C(0x8632586aca9a2710),
	UINT64_C(0xc9622c4102850a14), UINT64_C(0xb3ba5c8932b0836d),
	UINT64_C(0x3cd2cdd162ee18e6), UINT64_C(0x460abd1952db919f),
	UINT64_C(0x256b24ca6b12f26d), UINT64_C(0x5fb354025b277b14),
	UINT64_C(0xd0dbc55a0b79e09f), UINT64_C(0xaa03b5923b4c69e6),
	UINT64_C(0xe553c1b9f35344e2), UINT64_C(0x9f8bb171c366cd9b),
	UINT64_C(0x10e3202993385610), UINT64_C(0x6a3b50e1a30ddf69),
	UINT64_C(0x8e43c87e03060c18), UINT64_C(0xf49bb8b633338561),
	UINT64_C(0x7bf329ee636d1eea), UINT64_C(0x012b592653589793),
	UINT64_C(0x4e7b2d0d9b47ba97), UINT64_C(0x34a35dc5ab7233ee),
	UINT64_C(0xbbcbcc9dfb2ca865), UINT64_C(0xc113bc55cb19211c),
	UINT64_C(0x5863dbf1e3ac9dec), UINT64_C(0x22bbab39d3991495),
	UINT64_C(0xadd33a6183c78f1e), UINT64_C(0xd70b4aa9b3f20667),
	UINT64_C(0x985b3e827bed2b63), UINT64_C(0xe2834e4a4bd8a21a),
	UINT64_C(0x6debdf121b863991), UINT64_C(0x1733afda2bb3b0e8),
	UINT64_C(0xf34b37458bb86399), UINT64_C(0x8993478dbb8deae0),
	UINT64_C(0x06fbd6d5ebd3716b), UINT64_C(0x7c23a61ddbe6f812),
	UINT64_C(0x3373d23613f9d516), UINT64_C(0x49aba2fe23cc5c6f),
	UINT64_C(0xc6c333a67392c7e4), UINT64_C(0xbc1b436e43a74e9d),
	UINT64_C(0x95ac9329ac4bc9b5), UINT64_C(0xef74e3e19c7e40cc),
	UINT64_C(0x601c72b9cc20db47), UINT64_C(0x1ac40271fc15523e),
	UINT64_C(0x5594765a340a7f3a), UINT64_C(0x2f4c0692043ff643),
	UINT64_C(0xa02497ca54616dc8), UINT64_C(0xdafce7026454e4b1),
	UINT64_C(0x3e847f9dc45f37c0), UINT64_C(0x445c0f55f46abeb9),
	UINT64_C(0xcb349e0da4342532), UINT64_C(0xb1eceec59401ac4b),
	UINT64_C(0xfebc9aee5c1e814f), UINT64_C(0x8464ea266c2b0836),
	UINT64_C(0x0b0c7b7e3c7593bd), UINT64_C(0x71d40bb60c401ac4),
	UINT64_C(0xe8a46c1224f5a634), UINT64_C(0x927c1cda14c02f4d),
	UINT64_C(0x1d148d82449eb4c6), UINT64_C(0x67ccfd4a74ab3dbf),
	UINT64_C(0x289c8961bcb410bb), UINT64_C(0x5244f9a98c8199c2),
	UINT64_C(0xdd2c68f1dcdf0249), UINT64_C(0xa7f41839ecea8b30),
	UINT64_C(0x438c80a64ce15841), UINT64_C(0x3954f06e7cd4d138),
	UINT64_C(0xb63c61362c8a4ab3), UINT64_C(0xcce411fe1cbfc3ca),
	UINT64_C(0x83b465d5d4a0eece), UINT64_C(0xf96c151de49567b7),
	UINT64_C(0x76048445b4cbfc3c), UINT64_C(0x0cdcf48d84fe7545),
	UINT64_C(0x6fbd6d5ebd3716b7), UINT64_C(0x15651d968d029fce),
	UINT64_C(0x9a0d8ccedd5c0445), UINT64_C(0xe0d5fc06ed698d3c),
	UINT64_C(0xaf85882d2576a038), UINT64_C(0xd55df8e515432941),
	UINT64_C(0x5a3569bd451db2ca), UINT64_C(0x20ed197575283bb3),
	UINT64_C(0xc49581ead523e8c2), UINT64_C(0xbe4df122e51661bb),
	UINT64_C(0x3125607ab548fa30), UINT64_C(0x4bfd10b2857d7349),
	UINT64_C(0x04ad64994d625e4d), UINT64_C(0x7e7514517d57d734),
	UINT64_C(0xf11d85092d094cbf), UINT64_C(0x8bc5f5c11d3cc5c6),
	UINT64_C(0x12b5926535897936), UINT64_C(0x686de2ad05bcf04f),
	UINT64_C(0xe70573f555e26bc4), UINT64_C(0x9ddd033d65d7e2bd),
	UINT64_C(0xd28d7716adc8cfb9), UINT64_C(0xa85507de9dfd46c0),
	UINT64_C(0x273d9686cda3dd4b), UINT64_C(0x5de5e64efd965432),
	UINT64_C(0xb99d7ed15d9d8743), UINT64_C(0xc3450e196da80e3a),
	UINT64_C(0x4c2d9f413df695b1), UINT64_C(0x36f5ef890dc31cc8),
	UINT64_C(0x79a59ba2c5dc31cc), UINT64_C(0x037deb6af5e9b8b5),
	UINT64_C(0x8c157a32a5b7233e), UINT64_C(0xf6cd0afa9582aa47),
	UINT64_C(0x4ad64994d625e4da), UINT64_C(0x300e395ce6106da3),
	UINT64_C(0xbf66a804b64ef628), UINT64_C(0xc5bed8cc867b7f51),
	UINT64_C(0x8aeeace74e645255), UINT64_C(0xf036dc2f7e51db2c),
	UINT64_C(0x7f5e4d772e0f40a7), UINT64_C(0x05863dbf1e3ac9de),
	UINT64_C(0xe1fea520be311aaf), UINT64_C(0x9b26d5e88e0493d6),
	UINT64_C(0x144e44b0de5a085d), UINT64_C(0x6e963478ee6f8124),
	UINT64_C(0x21c640532670ac20), UINT64_C(0x5b1e309b16452559),
	UINT64_C(0xd476a1c3461bbed2), UINT64_C(0xaeaed10b762e37ab),
	UINT64_C(0x37deb6af5e9b8b5b), UINT64_C(0x4d06c6676eae0222),
	UINT64_C(0xc26e573f3ef099a9), UINT64_C(0xb8b627f70ec510d0),
	UINT64_C(0xf7e653dcc6da3dd4), UINT64_C(0x8d3e2314f6efb4ad),
	UINT64_C(0x0256b24ca6b12f26), UINT64_C(0x788ec2849684a65f),
	UINT64_C(0x9cf65a1b368f752e), UINT64_C(0xe62e2ad306bafc57),
	UINT64_C(0x6946bb8b56e467dc), UINT64_C(0x139ecb4366d1eea5),
	UINT64_C(0x5ccebf68aecec3a1), UINT64_C(0x2616cfa09efb4ad8),
	UINT64_C(0xa97e5ef8cea5d153), UINT64_C(0xd3a62e30fe90582a),
	UINT64_C(0xb0c7b7e3c7593bd8), UINT64_C(0xca1fc72bf76cb2a1),
	UINT64_C(0x45775673a732292a), UINT64_C(0x3faf26bb9707a053),
	UINT64_C(0x70ff52905f188d57), UINT64_C(0x0a2722586f2d042e),
	UINT64_C(0x854fb3003f739fa5), UINT64_C(0xff97c3c80f4616dc),
	UINT64_C(0x1bef5b57af4dc5ad), UINT64_C(0x61372b9f9f784cd4),
	UINT64_C(0xee5fbac7cf26d75f), UINT64_C(0x9487ca0fff135e26),
	UINT64_C(0xdbd7be24370c7322), UINT64_C(0xa10fceec0739fa5b),
	UINT64_C(0x2e675fb4576761d0), UINT64_C(0x54bf2f7c6752e8a9),
	UINT64_C(0xcdcf48d84fe75459), UINT64_C(0xb71738107fd2dd20),
	UINT64_C(0x387fa9482f8c46ab), UINT64_C(0x42a7d9801fb9cfd2),
	UINT64_C(0x0df7adabd7a6e2d6), UINT64_C(0x772fdd63e7936baf),
	UINT64_C(0xf8474c3bb7cdf024), UINT64_C(0x829f3cf387f8795d),
	UINT64_C(0x66e7a46c27f3aa2c), UINT64_C(0x1c3fd4a417c62355),
	UINT64_C(0x935745fc4798b8de), UINT64_C(0xe98f353477ad31a7),
	UINT64_C(0xa6df411fbfb21ca3), UINT64_C(0xdc0731d78f8795da),
	UINT64_C(0x536fa08fdfd90e51), UINT64_C(0x29b7d047efec8728),
};

uint16_t crc16(const char *buf, int len)
{
	int counter;
	uint16_t crc = 0;
	for (counter = 0; counter < len; counter++)
		crc = (crc << 8) ^ crc16tab[((crc >> 8) ^ *buf++) & 0x00FF];
	return crc;
}

uint64_t crc64(uint64_t crc, const unsigned char *s, uint64_t l)
{
	uint64_t j;

	for (j = 0; j < l; j++)
	{
		uint8_t byte = s[j];
		crc = crc64_tab[(uint8_t)crc ^ byte] ^ (crc >> 8);
	}
	return crc;
}

/* Test main */
#ifdef REDIS_TEST
#include <stdio.h>

#define UNUSED(x) (void)(x)
int crc64Test(int argc, char *argv[])
{
	UNUSED(argc);
	UNUSED(argv);
	printf("e9c6d914c4b8d9ca == %016llx\n",
		(unsigned long long) crc64(0, (unsigned char*)"123456789", 9));
	return 0;
}

#endif

#define SHA1HANDSOFF
#define rol(value, bits) (((value) << (bits)) | ((value) >> (32 - (bits))))

/* blk0() and blk() perform the initial expand. */
/* I got the idea of expanding during the round function from SSLeay */
#if BYTE_ORDER == LITTLE_ENDIAN
#define blk0(i) (block->l[i] = (rol(block->l[i],24)&0xFF00FF00) \
    |(rol(block->l[i],8)&0x00FF00FF))
#elif BYTE_ORDER == BIG_ENDIAN
#define blk0(i) block->l[i]
#else
#error "Endianness not defined!"
#endif
#define blk(i) (block->l[i&15] = rol(block->l[(i+13)&15]^block->l[(i+8)&15] \
    ^block->l[(i+2)&15]^block->l[i&15],1))

/* (R0+R1), R2, R3, R4 are the different operations used in SHA1 */
#define R0(v,w,x,y,z,i) z+=((w&(x^y))^y)+blk0(i)+0x5A827999+rol(v,5);w=rol(w,30);
#define R1(v,w,x,y,z,i) z+=((w&(x^y))^y)+blk(i)+0x5A827999+rol(v,5);w=rol(w,30);
#define R2(v,w,x,y,z,i) z+=(w^x^y)+blk(i)+0x6ED9EBA1+rol(v,5);w=rol(w,30);
#define R3(v,w,x,y,z,i) z+=(((w|x)&y)|(w&x))+blk(i)+0x8F1BBCDC+rol(v,5);w=rol(w,30);
#define R4(v,w,x,y,z,i) z+=(w^x^y)+blk(i)+0xCA62C1D6+rol(v,5);w=rol(w,30);


/* Hash a single 512-bit block. This is the core of the algorithm. */

void SHA1Transform(uint32_t state[5], const unsigned char buffer[64])
{
	uint32_t a, b, c, d, e;
	typedef union {
		unsigned char c[64];
		uint32_t l[16];
	} CHAR64LONG16;
#ifdef SHA1HANDSOFF
	CHAR64LONG16 block[1];  /* use array to appear as a pointer */
	memcpy(block, buffer, 64);
#else
	/* The following had better never be used because it causes the
	 * pointer-to-const buffer to be cast into a pointer to non-const.
	 * And the result is written through.  I threw a "const" in, hoping
	 * this will cause a diagnostic.
	 */
	CHAR64LONG16* block = (const CHAR64LONG16*)buffer;
#endif
	/* Copy context->state[] to working vars */
	a = state[0];
	b = state[1];
	c = state[2];
	d = state[3];
	e = state[4];
	/* 4 rounds of 20 operations each. Loop unrolled. */
	R0(a, b, c, d, e, 0); R0(e, a, b, c, d, 1); R0(d, e, a, b, c, 2); R0(c, d, e, a, b, 3);
	R0(b, c, d, e, a, 4); R0(a, b, c, d, e, 5); R0(e, a, b, c, d, 6); R0(d, e, a, b, c, 7);
	R0(c, d, e, a, b, 8); R0(b, c, d, e, a, 9); R0(a, b, c, d, e, 10); R0(e, a, b, c, d, 11);
	R0(d, e, a, b, c, 12); R0(c, d, e, a, b, 13); R0(b, c, d, e, a, 14); R0(a, b, c, d, e, 15);
	R1(e, a, b, c, d, 16); R1(d, e, a, b, c, 17); R1(c, d, e, a, b, 18); R1(b, c, d, e, a, 19);
	R2(a, b, c, d, e, 20); R2(e, a, b, c, d, 21); R2(d, e, a, b, c, 22); R2(c, d, e, a, b, 23);
	R2(b, c, d, e, a, 24); R2(a, b, c, d, e, 25); R2(e, a, b, c, d, 26); R2(d, e, a, b, c, 27);
	R2(c, d, e, a, b, 28); R2(b, c, d, e, a, 29); R2(a, b, c, d, e, 30); R2(e, a, b, c, d, 31);
	R2(d, e, a, b, c, 32); R2(c, d, e, a, b, 33); R2(b, c, d, e, a, 34); R2(a, b, c, d, e, 35);
	R2(e, a, b, c, d, 36); R2(d, e, a, b, c, 37); R2(c, d, e, a, b, 38); R2(b, c, d, e, a, 39);
	R3(a, b, c, d, e, 40); R3(e, a, b, c, d, 41); R3(d, e, a, b, c, 42); R3(c, d, e, a, b, 43);
	R3(b, c, d, e, a, 44); R3(a, b, c, d, e, 45); R3(e, a, b, c, d, 46); R3(d, e, a, b, c, 47);
	R3(c, d, e, a, b, 48); R3(b, c, d, e, a, 49); R3(a, b, c, d, e, 50); R3(e, a, b, c, d, 51);
	R3(d, e, a, b, c, 52); R3(c, d, e, a, b, 53); R3(b, c, d, e, a, 54); R3(a, b, c, d, e, 55);
	R3(e, a, b, c, d, 56); R3(d, e, a, b, c, 57); R3(c, d, e, a, b, 58); R3(b, c, d, e, a, 59);
	R4(a, b, c, d, e, 60); R4(e, a, b, c, d, 61); R4(d, e, a, b, c, 62); R4(c, d, e, a, b, 63);
	R4(b, c, d, e, a, 64); R4(a, b, c, d, e, 65); R4(e, a, b, c, d, 66); R4(d, e, a, b, c, 67);
	R4(c, d, e, a, b, 68); R4(b, c, d, e, a, 69); R4(a, b, c, d, e, 70); R4(e, a, b, c, d, 71);
	R4(d, e, a, b, c, 72); R4(c, d, e, a, b, 73); R4(b, c, d, e, a, 74); R4(a, b, c, d, e, 75);
	R4(e, a, b, c, d, 76); R4(d, e, a, b, c, 77); R4(c, d, e, a, b, 78); R4(b, c, d, e, a, 79);
	/* Add the working vars back into context.state[] */
	state[0] += a;
	state[1] += b;
	state[2] += c;
	state[3] += d;
	state[4] += e;
	/* Wipe variables */
	a = b = c = d = e = 0;
#ifdef SHA1HANDSOFF
	memset(block, '\0', sizeof(block));
#endif
}


/* SHA1Init - Initialize new context */

void SHA1Init(SHA1_CTX* context)
{
	/* SHA1 initialization constants */
	context->state[0] = 0x67452301;
	context->state[1] = 0xEFCDAB89;
	context->state[2] = 0x98BADCFE;
	context->state[3] = 0x10325476;
	context->state[4] = 0xC3D2E1F0;
	context->count[0] = context->count[1] = 0;
}


/* Run your data through this. */

void SHA1Update(SHA1_CTX* context, const unsigned char* data, uint32_t len)
{
	uint32_t i, j;

	j = context->count[0];
	if ((context->count[0] += len << 3) < j)
		context->count[1]++;
	context->count[1] += (len >> 29);
	j = (j >> 3) & 63;
	if ((j + len) > 63) {
		memcpy(&context->buffer[j], data, (i = 64 - j));
		SHA1Transform(context->state, context->buffer);
		for (; i + 63 < len; i += 64) {
			SHA1Transform(context->state, &data[i]);
		}
		j = 0;
	}
	else i = 0;
	memcpy(&context->buffer[j], &data[i], len - i);
}


/* Add padding and return the message digest. */

void SHA1Final(unsigned char digest[20], SHA1_CTX* context)
{
	unsigned i;
	unsigned char finalcount[8];
	unsigned char c;

#if 0	/* untested "improvement" by DHR */
	/* Convert context->count to a sequence of bytes
	 * in finalcount.  Second element first, but
	 * big-endian order within element.
	 * But we do it all backwards.
	 */
	unsigned char *fcp = &finalcount[8];

	for (i = 0; i < 2; i++)
	{
		uint32_t t = context->count[i];
		int j;

		for (j = 0; j < 4; t >>= 8, j++)
			*--fcp = (unsigned char)t;
	}
#else
	for (i = 0; i < 8; i++) {
		finalcount[i] = (unsigned char)((context->count[(i >= 4 ? 0 : 1)]
			>> ((3 - (i & 3)) * 8)) & 255);  /* Endian independent */
	}
#endif
	c = 0200;
	SHA1Update(context, &c, 1);
	while ((context->count[0] & 504) != 448) {
		c = 0000;
		SHA1Update(context, &c, 1);
	}
	SHA1Update(context, finalcount, 8);  /* Should cause a SHA1Transform() */
	for (i = 0; i < 20; i++) {
		digest[i] = (unsigned char)
			((context->state[i >> 2] >> ((3 - (i & 3)) * 8)) & 255);
	}
	/* Wipe variables */
	memset(context, '\0', sizeof(*context));
	memset(&finalcount, '\0', sizeof(finalcount));
}


#ifdef REDIS_TEST
#define BUFSIZE 4096

#define UNUSED(x) (void)(x)
int sha1Test(int argc, char **argv)
{
	SHA1_CTX ctx;
	unsigned char hash[20], buf[BUFSIZE];
	int i;

	UNUSED(argc);
	UNUSED(argv);

	for (i = 0; i < BUFSIZE; i++)
		buf[i] = i;

	SHA1Init(&ctx);
	for (i = 0; i < 1000; i++)
		SHA1Update(&ctx, buf, BUFSIZE);
	SHA1Final(hash, &ctx);

	printf("SHA1=");
	for (i = 0; i < 20; i++)
		printf("%02x", hash[i]);
	printf("\n");
	return 0;
}
#endif




const uint32_t dict_hash_function_seed = 5381;

/* And a case insensitive hash function (based on djb hash) */
uint32_t dictGenCaseHashFunction(const char *buf, int32_t len)
{
	uint32_t hash = (uint32_t)dict_hash_function_seed;

	while (len--)
		hash = ((hash << 5) + hash) + (tolower(*buf++)); /* hash * 33 + c */
	return hash;
}

uint32_t dictGenHashFunction(const void *key, int32_t len)
{
	/* 'm' and 'r' are mixing constants generated offline.
	 They're no really 'magic', they just happen to work well.  */
	uint32_t seed = dict_hash_function_seed;
	const uint32_t m = 0x5bd1e995;
	const int32_t r = 24;

	/* Initialize the hash to a 'random' value */
	uint32_t h = seed ^ len;

	/* Mix 4 bytes at a time into the hash */
	const char *data = (const char *)key;

	while (len >= 4)
	{
		uint32_t k = *(uint32_t*)data;

		k *= m;
		k ^= k >> r;
		k *= m;

		h *= m;
		h ^= k;

		data += 4;
		len -= 4;
	}

	/* Handle the last few bytes of the input array  */
	switch (len)
	{
	case 3: h ^= data[2] << 16;
	case 2: h ^= data[1] << 8;
	case 1: h ^= data[0]; h *= m;
	};

	/* Do a few final mixes of the hash to ensure the last few
	 * bytes are well-incorporated. */
	h ^= h >> 13;
	h *= m;
	h ^= h >> 15;

	return h;
}


int32_t string2ll(const char *s, size_t slen, int64_t *value)
{
	const char *p = s;
	size_t plen = 0;
	int32_t negative = 0;
	int64_t v;

	if (plen == slen)
		return 0;

	if (slen == 1 && p[0] == '0')
	{
		if (value != nullptr) *value = 0;
		return 1;
	}

	if (p[0] == '-')
	{
		negative = 1;
		p++; plen++;

		if (plen == slen)
			return 0;
	}

	if (p[0] >= '1' && p[0] <= '9')
	{
		v = p[0] - '0';
		p++; plen++;
	}
	else if (p[0] == '0' && slen == 1)
	{
		*value = 0;
		return 1;
	}
	else
	{
		return 0;
	}

	while (plen < slen && p[0] >= '0' && p[0] <= '9')
	{
		if (v > (ULLONG_MAX / 10))
			return 0;
		v *= 10;

		if (v > (ULLONG_MAX - (p[0] - '0')))
			return 0;
		v += p[0] - '0';

		p++; plen++;
	}

	if (plen < slen)
		return 0;

	if (negative)
	{
		if (v > ((unsigned long long)(-(LLONG_MIN + 1)) + 1))
			return 0;
		if (value != nullptr) *value = -v;
	}
	else
	{
		if (v > LLONG_MAX) /* Overflow. */
			return 0;
		if (value != nullptr) *value = v;
	}
	return 1;

}

int32_t ll2string(char *s, size_t len, int64_t value)
{
	char buf[32], *p;
	uint64_t v;
	size_t l;

	if (len == 0) return 0;
	v = (value < 0) ? -value : value;
	p = buf + 31; /* point to the last character */
	do
	{
		*p-- = '0' + (v % 10);
		v /= 10;
	} while (v);
	if (value < 0) *p-- = '-';
	p++;
	l = 32 - (p - buf);
	if (l + 1 > len) l = len - 1; /* Make sure it fits, including the nul term */
	memcpy(s, p, l);
	s[l] = '\0';
	return l;
}

/* Glob-style pattern matching. */
int32_t stringmatchlen(const char *pattern, int32_t patternLen,
	const char *string, int32_t stringLen, int32_t nocase)
{
	while (patternLen)
	{
		switch (pattern[0])
		{
		case '*':
			while (pattern[1] == '*')
			{
				pattern++;
				patternLen--;
			}
			if (patternLen == 1)
				return 1; /* match */
			while (stringLen)
			{
				if (stringmatchlen(pattern + 1, patternLen - 1,
					string, stringLen, nocase))
					return 1; /* match */
				string++;
				stringLen--;
			}
			return 0; /* no match */
			break;
		case '?':
			if (stringLen == 0)
				return 0; /* no match */
			string++;
			stringLen--;
			break;
		case '[':
		{
			int no, match;
			pattern++;
			patternLen--;
			no = pattern[0] == '^';
			if (no)
			{
				pattern++;
				patternLen--;
			}
			match = 0;
			while (1)
			{
				if (pattern[0] == '\\' && patternLen >= 2)
				{
					pattern++;
					patternLen--;
					if (pattern[0] == string[0])
						match = 1;
				}
				else if (pattern[0] == ']')
				{
					break;
				}
				else if (patternLen == 0)
				{
					pattern--;
					patternLen++;
					break;
				}
				else if (pattern[1] == '-' && patternLen >= 3)
				{
					int start = pattern[0];
					int end = pattern[2];
					int c = string[0];
					if (start > end)
					{
						int t = start;
						start = end;
						end = t;
					}
					if (nocase)
					{
						start = tolower(start);
						end = tolower(end);
						c = tolower(c);
					}
					pattern += 2;
					patternLen -= 2;
					if (c >= start && c <= end)
						match = 1;
				}
				else
				{
					if (!nocase)
					{
						if (pattern[0] == string[0])
							match = 1;
					}
					else
					{
						if (tolower((int)pattern[0]) == tolower((int)string[0]))
							match = 1;
					}
				}
				pattern++;
				patternLen--;
			}
			if (no)
				match = !match;
			if (!match)
				return 0; /* no match */
			string++;
			stringLen--;
			break;
		}
		case '\\':
			if (patternLen >= 2)
			{
				pattern++;
				patternLen--;
			}
			/* fall through */
		default:
			if (!nocase)
			{
				if (pattern[0] != string[0])
					return 0; /* no match */
			}
			else
			{
				if (tolower((int)pattern[0]) != tolower((int)string[0]))
					return 0; /* no match */
			}
			string++;
			stringLen--;
			break;
		}
		pattern++;
		patternLen--;
		if (stringLen == 0)
		{
			while (*pattern == '*')
			{
				pattern++;
				patternLen--;
			}
			break;
		}
	}

	if (patternLen == 0 && stringLen == 0)
		return 1;
	return 0;
}

int32_t stringmatch(const char *pattern, const char *string, int32_t nocase)
{
	return stringmatchlen(pattern, strlen(pattern), string, strlen(string), nocase);
}

void getRandomHexChars(char *p, uint32_t len)
{
#ifndef _WIN32
	char *charset = "0123456789abcdef";
	uint32_t j;

	/* Global state. */
	static int seedInitialized = 0;
	static unsigned char seed[20]; /* The SHA1 seed, from /dev/urandom. */
	static uint64_t counter = 0; /* The counter we hash with the seed. */

	if (!seedInitialized)
	{
		/* Initialize a seed and use SHA1 in counter mode, where we hash
		* the same seed with a progressive counter. For the goals of this
		* function we just need non-colliding strings, there are no
		* cryptographic security needs. */
		FILE *fp = fopen("/dev/urandom", "r");
		if (fp && fread(seed, sizeof(seed), 1, fp) == 1)
			seedInitialized = 1;
		if (fp) fclose(fp);
	}

	if (seedInitialized)
	{
		while (len)
		{
			unsigned char digest[20];
			SHA1_CTX ctx;
			uint32_t copylen = len > 20 ? 20 : len;

			SHA1Init(&ctx);
			SHA1Update(&ctx, seed, sizeof(seed));
			SHA1Update(&ctx, (unsigned char*)&counter, sizeof(counter));
			SHA1Final(digest, &ctx);
			counter++;

			memcpy(p, digest, copylen);
			/* Convert to hex digits. */
			for (j = 0; j < copylen; j++) p[j] = charset[p[j] & 0x0F];
			len -= copylen;
			p += copylen;
		}
	}
	else
	{
		/* If we can't read from /dev/urandom, do some reasonable effort
		* in order to create some entropy, since this function is used to
		* generate run_id and cluster instance IDs */
		char *x = p;
		uint32_t l = len;
		struct timeval tv;
		pid_t pid = getpid();

		/* Use time and PID to fill the initial array. */
		gettimeofday(&tv, nullptr);
		if (l >= sizeof(tv.tv_usec))
		{
			memcpy(x, &tv.tv_usec, sizeof(tv.tv_usec));
			l -= sizeof(tv.tv_usec);
			x += sizeof(tv.tv_usec);
		}
		if (l >= sizeof(tv.tv_sec))
		{
			memcpy(x, &tv.tv_sec, sizeof(tv.tv_sec));
			l -= sizeof(tv.tv_sec);
			x += sizeof(tv.tv_sec);
		}
		if (l >= sizeof(pid))
		{
			memcpy(x, &pid, sizeof(pid));
			l -= sizeof(pid);
			x += sizeof(pid);
		}
		/* Finally xor it with rand() output, that was already seeded with
		* time() at startup, and convert to hex digits. */
		for (j = 0; j < len; j++)
		{
			p[j] ^= rand();
			p[j] = charset[p[j] & 0x0F];
		}
	}
#endif
}

int64_t ustime(void)
{
#ifdef _WIN32
	auto time_now = std::chrono::system_clock::now();
	auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(time_now.time_since_epoch());
	return microseconds.count();
#else
	struct timeval tv;
	int64_t ust;
	gettimeofday(&tv, nullptr);
	ust = ((int64_t)tv.tv_sec) * 1000000;
	ust += tv.tv_usec;
	return ust;
#endif
}

/* Return the UNIX time in milliseconds */
int64_t mstime(void)
{
	return ustime() / 1000;
}

/* Return the UNIX time in seconds */
int64_t setime(void)
{
	return ustime() / 1000 / 1000;
}

/* Convert an amount of bytes into a human readable string in the form
 * of 100B, 2G, 100M, 4K, and so forth. */
void bytesToHuman(char *s, uint64_t n)
{
	double d;

	if (n < 1024)
	{
		/* Bytes */
		sprintf(s, "%lluB", n);
		return;
	}
	else if (n < (1024 * 1024))
	{
		d = (double)n / (1024);
		sprintf(s, "%.2fK", d);
	}
	else if (n < (1024LL * 1024 * 1024))
	{
		d = (double)n / (1024 * 1024);
		sprintf(s, "%.2fM", d);
	}
	else if (n < (1024LL * 1024 * 1024 * 1024))
	{
		d = (double)n / (1024LL * 1024 * 1024);
		sprintf(s, "%.2fG", d);
	}
	else if (n < (1024LL * 1024 * 1024 * 1024 * 1024))
	{
		d = (double)n / (1024LL * 1024 * 1024 * 1024);
		sprintf(s, "%.2fT", d);
	}
	else if (n < (1024LL * 1024 * 1024 * 1024 * 1024 * 1024))
	{
		d = (double)n / (1024LL * 1024 * 1024 * 1024 * 1024);
		sprintf(s, "%.2fP", d);
	}
	else
	{
		/* Let's hope we never need this */
		sprintf(s, "%lluB", n);
	}
}


/* Toggle the 16 bit unsigned integer pointed by *p from little endian to
 * big endian */
void memrev16(void *p)
{
	unsigned char *x = (unsigned char *)p, t;

	t = x[0];
	x[0] = x[1];
	x[1] = t;
}

/* Toggle the 32 bit unsigned integer pointed by *p from little endian to
 * big endian */
void memrev32(void *p)
{
	unsigned char *x = (unsigned char *)p, t;

	t = x[0];
	x[0] = x[3];
	x[3] = t;
	t = x[1];
	x[1] = x[2];
	x[2] = t;
}

/* Toggle the 64 bit unsigned integer pointed by *p from little endian to
 * big endian */
void memrev64(void *p)
{
	unsigned char *x = (unsigned char *)p, t;

	t = x[0];
	x[0] = x[7];
	x[7] = t;
	t = x[1];
	x[1] = x[6];
	x[6] = t;
	t = x[2];
	x[2] = x[5];
	x[5] = t;
	t = x[3];
	x[3] = x[4];
	x[4] = t;
}

uint16_t intrev16(uint16_t v)
{
	memrev16(&v);
	return v;
}

uint32_t intrev32(uint32_t v)
{
	memrev32(&v);
	return v;
}

uint64_t intrev64(uint64_t v)
{
	memrev64(&v);
	return v;
}

#ifdef REDIS_TEST
#include <stdio.h>

#define UNUSED(x) (void)(x)
int endianconvTest(int argc, char *argv[])
{
	char buf[32];

	UNUSED(argc);
	UNUSED(argv);

	sprintf(buf, "ciaoroma");
	memrev16(buf);
	printf("%s\n", buf);

	sprintf(buf, "ciaoroma");
	memrev32(buf);
	printf("%s\n", buf);

	sprintf(buf, "ciaoroma");
	memrev64(buf);
	printf("%s\n", buf);

	return 0;
}
#endif



static const std::string base64_chars =
"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
"abcdefghijklmnopqrstuvwxyz"
"0123456789+/";


static bool is_base64(unsigned char c)
{
	return (isalnum(c) || (c == '+') || (c == '/'));
}

std::string base64Encode(unsigned char const* bytesToEncode, unsigned int in_len)
{
	std::string ret;
	int i = 0;
	int j = 0;
	unsigned char char_array_3[3];
	unsigned char char_array_4[4];

	while (in_len--)
	{
		char_array_3[i++] = *(bytesToEncode++);
		if (i == 3) {
			char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
			char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
			char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
			char_array_4[3] = char_array_3[2] & 0x3f;

			for (i = 0; (i < 4); i++)
				ret += base64_chars[char_array_4[i]];
			i = 0;
		}
	}

	if (i)
	{
		for (j = i; j < 3; j++)
			char_array_3[j] = '\0';

		char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
		char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
		char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
		char_array_4[3] = char_array_3[2] & 0x3f;

		for (j = 0; (j < i + 1); j++)
			ret += base64_chars[char_array_4[j]];

		while ((i++ < 3))
			ret += '=';
	}
	return ret;
}

std::string base64Decode(std::string const& encodedString)
{
	size_t in_len = encodedString.size();
	int i = 0;
	int j = 0;
	int in_ = 0;
	unsigned char char_array_4[4], char_array_3[3];
	std::string ret;

	while (in_len-- && (encodedString[in_] != '=') && is_base64(encodedString[in_]))
	{
		char_array_4[i++] = encodedString[in_]; in_++;
		if (i == 4) {
			for (i = 0; i < 4; i++)
				char_array_4[i] = base64_chars.find(char_array_4[i]);

			char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
			char_array_3[1] = ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);
			char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];

			for (i = 0; (i < 3); i++)
				ret += char_array_3[i];
			i = 0;
		}
	}

	if (i)
	{
		for (j = i; j < 4; j++)
			char_array_4[j] = 0;

		for (j = 0; j < 4; j++)
			char_array_4[j] = base64_chars.find(char_array_4[j]);

		char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
		char_array_3[1] = ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);
		char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];

		for (j = 0; (j < i - 1); j++) ret += char_array_3[j];
	}
	return ret;
}









