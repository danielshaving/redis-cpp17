//
// Created by zhanghao on 2018/6/17.
//
#pragma once

#define LZF_VERSION 0x0105 /* 1.5, API version */
unsigned int
lzf_compress (const void *const in_data,  unsigned int in_len,
              void             *out_data, unsigned int out_len);

unsigned int
lzf_decompress (const void *const in_data,  unsigned int in_len,
                void             *out_data, unsigned int out_len);


