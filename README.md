# Memory_region_aware_copy

Memcpy is very slow because though the actual memory usage is low, memcpy copies the entire memory region both data and holes. For example, 8G VM memory usage is around 650MB but with memcpy the entire 8G is copied which takes around 10 seconds. Efficient way to copy is using lseek, this implementation does that. 
