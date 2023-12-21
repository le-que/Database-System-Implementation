#include <algorithm>
#include <cassert>
#include <cmath>
#include <functional>
#include <iostream>
#include <map>
#include <queue>
#include <thread>
#include <vector>

#include "external_sort/external_sort.h"
#include "storage/file.h"

#define UNUSED(p)  ((void)(p))

namespace buzzdb {

void external_sort(File &input, size_t num_values, File &output,
                   size_t mem_size) {
      
      // Housekeeping: Check file modes
      auto imode = input.get_mode();
      auto omode = output.get_mode();
      // If the files are open in wrong modes
      if (imode!= File::Mode::READ || omode != File::Mode::WRITE) return;
      mem_size -= mem_size % sizeof(uint64_t); // Restrict usable memeory to read only complete uint64_t
      output.resize(input.size());

      // Find the number of chunks
      auto num_chunks = int(std::ceil(1.*num_values*sizeof(uint64_t)/mem_size)); 
      size_t chunk_size = mem_size/sizeof(uint64_t); // Number of values in a chunk
      auto overflow = num_values*sizeof(uint64_t)% mem_size;
      size_t last_chunk_size = overflow?overflow/sizeof(uint64_t):chunk_size; //If there is no overflow, last chunk is the same size as others.
      // std::cout<<num_values<<'\t'<<mem_size<<'\t'<<num_chunks<<'\t'<<chunk_size<<'\t'<<overflow<<'\n';
      // Make temporary files.
      std::vector<std::unique_ptr<File>> chunk_file_registry(num_chunks); 
      // Read data in chunks, sort them and then write them to temp files
      for (auto i=0; i<num_chunks; i++){ // All chunks except the last one
            auto this_chunk_size = i==num_chunks-1?last_chunk_size:chunk_size;
            auto this_mem_size = overflow==0?mem_size:i==num_chunks-1?overflow:mem_size;
            // std::cout<<" Chunk "<<i<<'\t';      
            chunk_file_registry[i] = std::move(File::make_temporary_file());
            // unique_ptr -- smart pointer for automatically releasing memory
            // Creates pointer for an array of uint64_t containing chunk_size values
            auto chunk = std::make_unique<uint64_t[]>(this_chunk_size);
            input.read_block(i*mem_size, this_mem_size, reinterpret_cast<char *>(chunk.get()));
            //Sort the numbers
            // for (size_t j=0; j<this_chunk_size; j++){std::cout<<j<<"->"<<*(chunk.get()+j)<<'\t';} std::cout<<'\n';
            std::sort(chunk.get(), chunk.get()+this_chunk_size);
            // for (size_t j=0; j<this_chunk_size; j++){std::cout<<j<<"->"<<chunk.get()[j]<<'\t';} std::cout<<'\n';
            // //  Write to temporary file after sorting
            chunk_file_registry[i]->write_block( reinterpret_cast<char *>(chunk.get()), 0, this_mem_size);
      }

      struct element {
      uint64_t value;
      uint64_t read_offset = -1;
      uint64_t eof_offset = -1;
      int chunk_id = -1;
      };

      std::vector<element> structs_vector; // A vector to be made into a priority queue
      uint64_t value_buffer; // To store the number that we just read
      // Make the initial comparison vector and turn it into a priority queue
      for(auto i=0; i<num_chunks; i++){
            // Read the first character in each temporary file
            chunk_file_registry[i]->read_block(0, sizeof(uint64_t), reinterpret_cast<char *>(&value_buffer)); 
            struct element temp_element = {
                  .value = value_buffer, 
                  .read_offset=0, 
                  .eof_offset = i==num_chunks-1? last_chunk_size: chunk_size, 
                  .chunk_id = i};
            structs_vector.push_back(temp_element);
      }

            // chunk_file_registry[e.chunk_id]->read_block(e.read_offset, sizeof(uint64_t), reinterpret_cast<char *>(&value_buffer));
      //Check if the correct numbers are printed
      // for(auto i=0; i<num_chunks; i++) std::cout<<structs_vector[i].value<<'\t';

      auto lambdag = [](const element &e1, const element &e2) -> bool {return e1.value>e2.value;};
      // Checking lambda function
      // std::cout<<'\n'<<lambdag(structs_vector[0], structs_vector[1]);
      
      //Testing priority queue
      // std::priority_queue pq_test(structs_vector.begin(), structs_vector.end(), lambdag); //Only when printing
      // for (std::cout << "\nPQ: \t"; !pq_test.empty(); pq_test.pop()) std::cout<<pq_test.top().value << ' ';
      // std::cout << '\n';

      std::priority_queue pq(structs_vector.begin(), structs_vector.end(), lambdag); 
      // Write and merge
      size_t write_pos = 0; //Tracking position in the outfile. 
      while(!pq.empty() && write_pos<num_values){
            // Write to the file
            element e_buffer = pq.top(); //Get the smallest element
            // std::cout<<write_pos<<'\t'<<e_buffer.value<<'\t'<<e_buffer.chunk_id<<'\t'<<e_buffer.read_offset<<'\t'<<e_buffer.eof_offset<<'\n';
            output.write_block(reinterpret_cast<char *> (&(e_buffer.value)), write_pos++*sizeof(uint64_t), sizeof(uint64_t));
            pq.pop(); //Remove the element
            if (e_buffer.read_offset<e_buffer.eof_offset-1){ // If the file still has numbers
                  e_buffer.read_offset++; //Move the read offset to the next position
                  // Read next entry from the same file
                  chunk_file_registry[e_buffer.chunk_id]->read_block(e_buffer.read_offset*sizeof(uint64_t), sizeof(uint64_t), reinterpret_cast<char *>(&value_buffer));
                  e_buffer.value = value_buffer; //Update with the new value
                  pq.push(e_buffer);
            }
      }
}
}  // namespace buzzdb
