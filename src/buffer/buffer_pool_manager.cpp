//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

// 重要：frame_id是page_数组的索引
Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  std::scoped_lock<std::mutex> lock{latch_};
  // std::cout << "FetchPage page_id: " << page_id << std::endl;
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  if (page_id == -1) {
    return nullptr;
  }
  frame_id_t frame_id;
  if (page_table_.find(page_id) != page_table_.end()) {
    // std::cout << "Got from page table\n";
    auto p = page_table_.find(page_id);
    frame_id = p->second;
    Page *page = pages_ + frame_id;
    // if(page->GetPageId() != page_id) {
    //   for(size_t i = 0; i < pool_size_; i ++) {
    //     [[maybe_unused]] Page *test_page = pages_ + i;
    //     std::cout<<"page id "<<test_page->GetPageId()<<std::endl;
    //   }
    // }
    page->pin_count_++;
    replacer_->Pin(frame_id);
    // std::cout << "Got from page table\n";
    return page;
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    frame_id_t *f = new int(0);
    bool got_ = replacer_->Victim(f);
    if (got_) {
      frame_id = *f;
      delete f;
    } else {
      // 走到这里说明freelist和replacer里unpinned_fids都为空，说明pages_所有page都是pinned状态
      delete f;
      std::cout << "all pages are pinned\n";
      return nullptr;
    }
  }
  Page *page = pages_ + frame_id;
  // 2.     If R is dirty, write it back to the disk.
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }
  // 3.     Delete R from the page table and insert P.
  // std::cout << "Got a new page\n";
  page_table_.erase(page->GetPageId());
  page_table_[page_id] = frame_id;
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  page->page_id_ = page_id;
  page->is_dirty_ = false;
  page->ResetMemory();
  disk_manager_->ReadPage(page_id, page->data_);
  page->pin_count_ = 1;
  replacer_->Pin(frame_id);
  // std::cout << "Got a new page\n";
  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::scoped_lock<std::mutex> lock{latch_};
  // std::cout << "unpinng page_id: " << page_id << std::endl;
  frame_id_t frame_id;
  auto p = page_table_.find(page_id);
  if (p == page_table_.end()) {
    return false;
  }
  frame_id = p->second;
  Page *page = pages_ + frame_id;
  if (is_dirty) {
    page->is_dirty_ = true;
  }
  if (page->pin_count_ > 0) {
    page->pin_count_--;
    if (page->pin_count_ == 0) {
      replacer_->Unpin(frame_id);
    }
  } else {
    return false;
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  std::scoped_lock<std::mutex> lock{latch_};
  // std::cout << "Flush Page page_id: " << page_id << std::endl;
  // Make sure you call DiskManager::WritePage!
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto p = page_table_.find(page_id);
  if (p == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = p->second;
  Page *page = pages_ + frame_id;
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  std::scoped_lock<std::mutex> lock{latch_};
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    frame_id_t *f = new int(0);
    bool got_ = replacer_->Victim(f);
    if (got_) {
      frame_id = *f;
      delete f;
      Page *page = pages_ + frame_id;
      if (page->is_dirty_) {
        disk_manager_->WritePage(page->page_id_, page->data_);
        page->is_dirty_ = false;
      }
      page_table_.erase(page->page_id_);
      // std::cout << "erased page_id: " << page->page_id_ << std::endl;
    } else {
      // 走到这里说明freelist和replacer里unpinned_fids都为空，说明pages_所有page都是pinned状态
      delete f;
      return nullptr;
    }
  }
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  *page_id = disk_manager_->AllocatePage();
  page_table_[*page_id] = frame_id;
  Page *page = pages_ + frame_id;
  page->ResetMemory();
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  replacer_->Pin(frame_id);
  // disk_manager_->ReadPage(*page_id, page->data_); //此时不要读盘，盘里没有东西
  // 4.   Set the page ID output parameter. Return a pointer to P.
  // std::cout << "new page_id: " << *page_id << " with frame id: " << frame_id << std::endl;
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  std::scoped_lock<std::mutex> lock{latch_};
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  auto p = page_table_.find(page_id);
  // 1.   If P does not exist, return true.
  if (p == page_table_.end()) {
    return true;
  }
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  frame_id_t frame_id = p->second;
  Page *page = pages_ + frame_id;
  if (page->pin_count_ != 0) {
    return false;
  }
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  disk_manager_->DeallocatePage(page_id);
  page_table_.erase(page_id);
  page->page_id_ = INVALID_PAGE_ID;
  page->ResetMemory();
  page->is_dirty_ = false;
  replacer_->Pin(frame_id);
  free_list_.emplace_back(frame_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  std::scoped_lock<std::mutex> lock{latch_};
  // You can do it!
  for (auto p : page_table_) {
    FlushPageImpl(p.first);
  }
}

}  // namespace bustub
