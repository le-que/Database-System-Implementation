for fixing page
we want to lock a global mutex so that other threads cant access it
we then want to lock the page, so that the page can finish what they are doing 
fix the page, if the page is being fixed again we want to put it in LRU
and if it is in LRU we want to put it at the end of the list
then we want to unlock the global lock, and lock the page

for  evicting
We choose the page to evict by first looking at FIFO, then LRU
we a copy of the page that is written to the file so that other threads can continue using it while it is being written
we then erase it from the FIFO or LRU and the buffer frame