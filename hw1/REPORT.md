Lab 1: CPP_TUTORIAL
1) Find number of chunks and chunks size, we also don't want to cut off any bits of the int so we are restricting that it will only read only complete uint64_t
2) Create vector which stores unique pointers to temp files
3) Read chunks from input, sort the ints, and then put it into temp file
4) Create structs which has info about temp files, like the top value, read_offset, eof_offset, and the ID/location of it in the vector
5) Read the first value, and put each structs inside another vector which will be used for the priority queue
6) create priority queue and a way to sort the priority queue by comparing the top value
7) resize the output file so that it can fit all the values
8) get the top value from the priority queue
9) write the top value to the priority queue, making sure we are offseting correctly so that it doesn't write on top of an already written value
10) Remove the top value from priority queue
11) check if next number is not end of file, before increasing the offset, reading the next value from the temp file and adding it back to the priority queue
12) continue 5-11 until we read all the files