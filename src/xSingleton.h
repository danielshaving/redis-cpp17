#ifndef __xSingleton_H__
#define __xSingleton_H__
#include "all.h"
  
template<typename T>
struct xSingleton
{
protected:
	static std::unique_ptr<T>& GetInstanceCore()
	{
		static std::unique_ptr<T> instance;
		return instance;
	}

	xSingleton() = default;
	xSingleton( const xSingleton& ) = delete;
	xSingleton &operator =( const xSingleton& ) = delete;

public:
	static T* GetInstance()
	{
		assert( GetInstanceCore() != nullptr );
		return GetInstanceCore().get();
	}
	template<typename ...PTS>
	static void InitInstance( PTS&& ... ps )
	{
		assert( GetInstanceCore() == nullptr );
		GetInstanceCore().reset( new T( std::forward<PTS>( ps )... ) ); // todo: new T{} 这样写可兼容初始化列表 ?
	}
	static void DestroyInstance()
	{
		assert( GetInstanceCore() != nullptr );
		GetInstanceCore().reset();
	}
};


#endif
