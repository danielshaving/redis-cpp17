#pragma once
#include "all.h"
#include "xZmalloc.h"
#include "xTimer.h"

class xPriorityQueue
{
public:
	xPriorityQueue();
	~xPriorityQueue();
	void dtor();
	bool push(xTimer *e);
	xTimer *pop();
	bool  erase(xTimer *e);
	int   size();
	xTimer *head();
	void clear();
	xTimer *top();
	void reserve();
	void shiftUp(int hole_index, xTimer *e);
	void shiftDown(int hole_index, xTimer *e);
	
	xTimer **p;
	int n,a;
};
