//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { max_num_pages = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock<std::mutex> lock{latch_};
  if (unpinned_fids_.empty()) {
    return false;
  }
  // 设定：vector类似一个队列，先进先出
  *frame_id = unpinned_fids_.front();
  unpinned_fids_.pop_front();
  frame2iter_.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock{latch_};
  auto p = frame2iter_.find(frame_id);
  if(p != frame2iter_.end()){
    auto iter = p->second;
    unpinned_fids_.erase(iter);
    frame2iter_.erase(p);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock{latch_};
  if (unpinned_fids_.size() == max_num_pages) {
    return;
  }
  if(frame2iter_.find(frame_id) != frame2iter_.end()){
    return;
  }
  unpinned_fids_.push_back(frame_id);
  frame2iter_[frame_id] = --unpinned_fids_.end();
}

size_t LRUReplacer::Size() { return unpinned_fids_.size(); }

}  // namespace bustub
