#include "xPriorityQueue.h"

xPriorityQueue::xPriorityQueue()
{
	p = 0;
	n = 0;
	a = 0;
}


void xPriorityQueue::dtor()
{
	if(p)
	{
		delete []p;
	}
}
xPriorityQueue::~xPriorityQueue()
{
	dtor();
}


bool xPriorityQueue::push(xTimer *e)
{
	if (!reserve(n + 1))
		return false;
	shiftUp(n++, e); 
	return true;
}
xTimer *xPriorityQueue::pop()
{
	if (n)
	{
		xTimer *e = *p; 
		shiftDown(0, p[--n]); 
		*(int *)e = -1; 
		return e; 
	}
	return nullptr;
}


int  xPriorityQueue::size()
{
	return n;	
}

bool   xPriorityQueue::erase(xTimer *e)
{
	if (-1 != *(int *)e)
	{
		xTimer *last = p[--n]; 
		int parent = (*(int *)e - 1) / 2; 
		/* we replace e with the last element in the heap.  We might need to
		shift it upward if it is less than its parent, or downward if it is
		greater than one or both its children. Since the children are known
		to be less than the parent, it can't need to shift both up and
		down. */
		if (*(int *)e > 0 && (p[parent]->when  >  last->when) > 0)
			shiftUp(*(int *)e, last); 
		else
			shiftDown(*(int *)e, last); 
		*(int *)e = -1; 
		return 0; 
	}
	return -1; 
}
xTimer *xPriorityQueue::head()
{
	return p[0];
}
	
void xPriorityQueue::clear()
{
	p = 0;
	n = 0;
	a = 0;
}

xTimer *xPriorityQueue::top()
{
	return n?*p:nullptr;
}

bool xPriorityQueue::reserve(int n)
{
	if(a < n)
	{
		xTimer **pp;
		int aa = a ? a *2:80;
		if(aa < n)
		{
			aa = n;
		}
		
		p = pp = new xTimer * [aa];
		a = aa;
	}

	return true;
}


void xPriorityQueue::shiftUp(int hole_index, xTimer *e)
{
	int parent = (hole_index - 1) / 2; 
	while (hole_index && ((p[parent])->when >  e->when) > 0)
	{
		*(int *)(p[hole_index] = p[parent]) = hole_index; 
		hole_index = parent; 
		parent = (hole_index - 1) / 2; 
	}
	*(int *)(p[hole_index] = e) = hole_index; 
}


void xPriorityQueue::shiftDown(int hole_index, xTimer *e)
{
	int min_child = 2 * (hole_index + 1); 
	while (min_child <= n)
	{
		min_child -= min_child == n || (p[min_child]->when  >  p[min_child - 1]->when) > 0; 
		if (!((e->when >  p[min_child]->when ) > 0))
			break; 
		*(int *)(p[hole_index] = p[min_child]) = hole_index; 
		hole_index = min_child; 
		min_child = 2 * (hole_index + 1); 
	}
	*(int *)(p[hole_index] = e) = hole_index; 
}



