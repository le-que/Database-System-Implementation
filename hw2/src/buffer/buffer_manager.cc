#include <unordered_map>
#include <system_error>
#include <string_view>
#include <array>
#include <string>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <algorithm>
#include <thread>
#include <sstream>

#include "buffer/buffer_manager.h"
#include "common/macros.h"
#include "storage/file.h"

#define UNUSED(p)  ((void)(p))

namespace buzzdb {

char* BufferFrame::get_data() { 
    return data; 
}

BufferFrame::BufferFrame(const uint64_t pageId, char* data, list_position  fifo_position, list_position lru_position)
 : pId(pageId), data(data), fifo_position(fifo_position), lru_position(lru_position) {}

void BufferFrame::lock(const bool exclusive_lock) {
    if (!exclusive_lock) {
        shared_mutex.lock_shared();
    } else {
        shared_mutex.lock();
        this->exclusively_locked = true;
    }
}

void BufferFrame::unlock() {
    if (!this->exclusively_locked) {
        shared_mutex.unlock_shared();
    } else {
        this->exclusively_locked = false;
        shared_mutex.unlock();
    }
}

BufferManager::BufferManager(size_t page_size, size_t page_count) :
 page_size(page_size), page_count(page_count), loaded_pages(std::make_unique<char[]>(page_count * page_size)) {}

BufferManager::~BufferManager() {
    std::unique_lock u_lock(global_mutex);
    for (auto& bufferframe: bufferframes) {
        auto& file = *segment_files.find(get_segment_id(bufferframe.second.pId))->second.file;
        u_lock.unlock();
        file.write_block(bufferframe.second.data, get_segment_page_id(bufferframe.second.pId) * page_size, page_size);
        u_lock.lock();
        bufferframe.second.isDirty = false;
    }
}

BufferFrame& BufferManager::fix_page(uint64_t page_id, bool exclusive) {
    std::unique_lock u_lock(global_mutex);
    while (true) {
        auto i = bufferframes.find(page_id);
        if (i != bufferframes.end()) {
            auto& page = i->second;
            page.set_num_fixed(page.get_num_fixed() + 1);
            if (page.state == BufferFrame::NEW) {
                /// Another thread is trying to evict another page for this wait for the other thread to finish by locking the page exclusively.
                u_lock.unlock();
                page.lock(true);
                page.unlock();
                u_lock.lock();
                if (page.state == BufferFrame::NEW) {
                    page.set_num_fixed(page.get_num_fixed() - 1);
                    if (page.get_num_fixed() == 0) {
                        bufferframes.erase(i);
                    }
                    continue;
                }
            } else if (page.state == BufferFrame::EVICT) {
                page.state = BufferFrame::RELOAD;
            } 
            if (page.lru_position == lru_list.end()) {
                assert(page.fifo_position != fifo_list.end());
                /// Page is in the FIFO List and being fixed again => Hot Page => move it the the LRU List
                fifo_list.erase(page.fifo_position);
                page.fifo_position = fifo_list.end();
                page.lru_position = lru_list.insert(lru_list.end(), &page);
            } else {
                /// Page is in LRU List => Update it to the end of LRU List
                lru_list.erase(page.lru_position);
                page.lru_position = lru_list.insert(lru_list.end(), &page);
            }
            u_lock.unlock();
            page.lock(exclusive);
            return page;
        } else {
            break;
        }
    }
    assert(bufferframes.find(page_id) == bufferframes.end());
    auto& page = bufferframes.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(page_id),
            std::forward_as_tuple(page_id, nullptr, fifo_list.end(), lru_list.end())
            ).first->second;
    page.set_num_fixed(page.get_num_fixed() + 1);
    page.lock(true);
    char* data;
    if (bufferframes.size() - 1 >= page_count) {
        data = evict_page(u_lock);
        if (data == nullptr) {
            page.set_num_fixed(page.get_num_fixed() - 1);
            page.unlock();
            if (page.get_num_fixed() == 0) {
                assert(page.lru_position == lru_list.end() && page.fifo_position == fifo_list.end());
                bufferframes.erase(page_id);
            }
            throw buffer_full_error();
        }
    } else {
        data = &loaded_pages[(bufferframes.size() - 1) * page_size];
    }
    page.data = data;
    page.state = BufferFrame::UNMOD;
    page.fifo_position = fifo_list.insert(fifo_list.end(), &page);
    auto segment_id = get_segment_id(page.pId);
    auto segment_page_id = get_segment_page_id(page.pId);
    SegmentFile* segment_file;
    auto i = segment_files.find(segment_id);
    if (i != segment_files.end()) {
        segment_file = &i->second;
    } else {
        auto filename = std::to_string(segment_id);
        //write dirty pages to file
        segment_file = &segment_files.emplace(segment_id, File::open_file(filename.c_str(), File::WRITE)).first->second;
    }
    std::unique_lock file_latch{segment_file->file_latch};
    auto& file = *segment_file->file;
    if (file.size() < (segment_page_id + 1) * page_size) {
        file.resize((segment_page_id + 1) * page_size);
        file_latch.unlock();
        std::memset(page.data, 0, page_size);
    } else {
        file_latch.unlock();
        u_lock.unlock();
        file.read_block(segment_page_id * page_size, page_size, page.data);
        u_lock.lock();
    }
    page.state = BufferFrame::MOD;
    page.isDirty = false;
    page.unlock();
    u_lock.unlock();
    page.lock(exclusive);
    return page;
}

void BufferManager::unfix_page(BufferFrame& page, bool is_dirty) {
    page.unlock();
    std::unique_lock u_lock(global_mutex);
    if (is_dirty) {
        page.isDirty = true;
    }
    page.set_num_fixed(page.get_num_fixed() - 1);
}

std::vector<uint64_t> BufferManager::get_fifo_list() const {
    std::vector<uint64_t> v;
    for (const auto& fifo : fifo_list) {
        v.push_back(fifo->pId);
    }
    return v;
}

std::vector<uint64_t> BufferManager::get_lru_list() const {
    std::vector<uint64_t> v;
    for (const auto& lru: lru_list) {
        v.push_back(lru->pId);
    }
    return v;
}

char* BufferManager::evict_page(unique_lock<mutex>& latch) {
    BufferFrame* page_to_evict;
    while (true) {
        /// Need to evict another page. If no page can be evict
        bool found = false;
        page_to_evict = nullptr;
        for (auto* page : fifo_list) {
            if (page->state == BufferFrame::MOD && page->get_num_fixed() == 0) {
                page_to_evict = page;
                found = true;
                break;
            }
        }
        /// If FIFO list is empty or all pages in FIFO List are fixed, try to evcit in LRU List
        if (!found) {
            for (auto* page : lru_list) {
                if (page->state == BufferFrame::MOD && page->get_num_fixed() == 0) {
                    page_to_evict = page;
                    break;
                }
            }
        }
        if (page_to_evict == nullptr) {
            return nullptr;
        }
        assert(page_to_evict->state == BufferFrame::MOD);
        page_to_evict->state = BufferFrame::EVICT;
        if (!page_to_evict->isDirty) {
            break;
        }
        /// Create a copy pf the page that is written to the file so that other threads can continue using it while it is being written
        auto page_data = std::make_unique<char[]>(page_size);
        std::memcpy(page_data.get(), page_to_evict->data, page_size);
        BufferFrame page_copy{page_to_evict->pId, page_data.get(), fifo_list.end(), lru_list.end()};
        auto& file = *segment_files.find(get_segment_id(page_copy.pId))->second.file;
        latch.unlock();
        file.write_block(page_copy.data, get_segment_page_id(page_copy.pId) * page_size, page_size);
        latch.lock();
        page_copy.isDirty = false;
        assert(page_to_evict->state == BufferFrame::EVICT || page_to_evict->state == BufferFrame::RELOAD);
        if (page_to_evict->state == BufferFrame::EVICT) {
            break;
        }
        page_to_evict->state = BufferFrame::MOD;
    }
    if (page_to_evict->lru_position == lru_list.end()) {
        assert(page_to_evict->fifo_position != fifo_list.end());
        fifo_list.erase(page_to_evict->fifo_position);
    } else {
        lru_list.erase(page_to_evict->lru_position);
    }
    char* data = page_to_evict->data;
    bufferframes.erase(page_to_evict->pId);
    return data;
}
}