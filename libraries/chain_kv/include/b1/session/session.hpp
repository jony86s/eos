#pragma once

#include <optional>
#include <queue>
#include <set>
#include <stack>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <variant>

#include <b1/session/cache.hpp>
#include <b1/session/shared_bytes.hpp>

namespace eosio::session {

template <class... Ts>
struct overloaded : Ts... {
   using Ts::operator()...;
};
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

// Defines a session for reading/write data to a cache and persistent data store.
//
// \tparam Parent The parent of this session
template <typename Parent>
class session {
 private:
   struct session_impl;

 public:
   mutable bool debug = false;
   void set_debug(bool d) const {
      if (debug != d) {
         debug = d;
         std::visit(overloaded{ [&](session<Parent>* p) {
                                 p->set_debug(debug);
                              },
                              [&](Parent* p) {
                                 p->debug = debug;
                              } },
              m_parent);
      }
   };
   template<typename Key>
   bool is_special_key(const Key& key) const {
      const static std::vector<char> the_one = {
//         static_cast<char>(0x12), static_cast<char>(0xcb), static_cast<char>(0x54), static_cast<char>(0x95), static_cast<char>(0x53), static_cast<char>(0x0c), static_cast<char>(0x34), static_cast<char>(0x95), static_cast<char>(0x80), static_cast<char>(0xcb), static_cast<char>(0x54), static_cast<char>(0x95), static_cast<char>(0x53), static_cast<char>(0x0c), static_cast<char>(0x34), static_cast<char>(0x95), static_cast<char>(0x80), static_cast<char>(0x4b), static_cast<char>(0x90), static_cast<char>(0xa6), static_cast<char>(0x1a), static_cast<char>(0x4a), static_cast<char>(0xc0), static_cast<char>(0x00), static_cast<char>(0x00), static_cast<char>(0x00), static_cast<char>(0x00), static_cast<char>(0x00), static_cast<char>(0x00), static_cast<char>(0x00), static_cast<char>(0x00), static_cast<char>(0x02), static_cast<char>(0x52)
//         static_cast<char>(0x12), static_cast<char>(0xcb), static_cast<char>(0x54), static_cast<char>(0x95), static_cast<char>(0x53), static_cast<char>(0x0c), static_cast<char>(0x34), static_cast<char>(0x95), static_cast<char>(0x80), static_cast<char>(0xcb), static_cast<char>(0x54), static_cast<char>(0x95), static_cast<char>(0x53), static_cast<char>(0x0c), static_cast<char>(0x34), static_cast<char>(0x95), static_cast<char>(0x80), static_cast<char>(0x4b), static_cast<char>(0x90), static_cast<char>(0xa6), static_cast<char>(0x1a), static_cast<char>(0x4a), static_cast<char>(0xc0)
         static_cast<char>(0xff), static_cast<char>(0xcb), static_cast<char>(0x54), static_cast<char>(0x95), static_cast<char>(0x53), static_cast<char>(0x0c), static_cast<char>(0x34), static_cast<char>(0x95), static_cast<char>(0x80), static_cast<char>(0xcb), static_cast<char>(0x54), static_cast<char>(0x95), static_cast<char>(0x53), static_cast<char>(0x0c), static_cast<char>(0x34), static_cast<char>(0x95), static_cast<char>(0x80), static_cast<char>(0x4b), static_cast<char>(0x90), static_cast<char>(0xa6), static_cast<char>(0x1a), static_cast<char>(0x4a), static_cast<char>(0xc0)
      };
      if (key.size() < the_one.size())
         return false;
      for(unsigned int i = 0; i < key.size(); ++i) {
         if (key[i] != the_one[i])
            return false;
      }
      return true;
   }
   template<typename Key>
   void special_key(const Key& key) const {
      if (!debug) {
         const bool spec = is_special_key(key);
         if (!spec)
            return;
         ilog("REM  !!! found special !!!");
         set_debug(true);
      }
   }
   template<typename Key>
   void key_to_buf(const Key& key, char* buf_ptr) const {
      if (!debug)
         return;
      unsigned i = 0;
      for (; i < key.size(); ++i) {
         const auto count = sprintf(buf_ptr, "%02x", (uint8_t)key[i]);
         buf_ptr += count;
      }
      *buf_ptr = '\0';
   };
   void print_buf(const char* desc, const char* buffer) const {
      if (!debug)
         return;
      std::string padding;
      char buffer2[500];
      sprintf(buffer2,"%-40s", desc);
      ilog("REM ${desc}: ${s}",("desc",buffer2)("s",buffer));
   };
   template<typename Key1, typename Key2>
   void compare(const Key1& lhs, const Key2& rhs, const char* desc) const {
      auto size = lhs.size();
      if (rhs.size() > size) {
         size = rhs.size();
      }
      if (debug) {
         int32_t cmp = std::memcmp(lhs.data(), rhs.data(), std::min(lhs.size(), rhs.size()));
         const auto temp1 = cmp > 0;
         const auto temp2 = cmp == 0;
         const auto temp3 = lhs.size() >= rhs.size();
         const auto temp4 = cmp == 0 && lhs.size() >= rhs.size();
         const auto gte = cmp > 0 || (cmp == 0 && lhs.size() >= rhs.size());
         ilog("REM cmp: ${cmp}, cmp>0: ${temp1}, cmp==0: ${temp2}, lhs.size()>=rhs.size(): ${temp3}, cmp==0 && size()>=rhs.size(): ${temp4}, gte: ${gte}",("cmp",cmp)("temp1",temp1)("temp2",temp2)("temp3",temp3)("temp4",temp4)("gte",gte));
      }
      char buffer1[500];
      char* buf_ptr1 = buffer1;
      char buffer2[500];
      char* buf_ptr2 = buffer2;
      bool equal = true;
      auto add_comp = [](auto lhs, auto rhs, char*& buf_ptr) {
         const char* desc = "greater";
         if (lhs < rhs) {
            desc = "lesser";
         }
         const auto count = sprintf(buf_ptr, " -- %s", desc);
         buf_ptr += count;
      };
      for(unsigned int i = 0; i < size; ++i) {
         const auto count = sprintf(buf_ptr1, "%02x", (uint8_t)lhs[i]);
         buf_ptr1 += count;
         sprintf(buf_ptr2, "%02x", (uint8_t)rhs[i]);
         buf_ptr2 += count;
         const auto l = static_cast<unsigned char>(lhs[i]);
         const auto r = static_cast<unsigned char>(rhs[i]);
         if (l != r) {
            equal = false;
            add_comp(l, r, buf_ptr1);
            break;
         }
      }
      if (equal && lhs.size() != rhs.size()) {
         add_comp(lhs.size(), rhs.size(), buf_ptr1);
         equal = false;
      }
      *buf_ptr1 = '\0';
      *buf_ptr2 = '\0';
      if (!equal) {
         print_buf(desc, "compare !=");
         print_buf("compare lhs", buffer1);
         print_buf("compare rhs", buffer2);
      } else {
         print_buf(desc, "compare ==");
      }
   }
   template<typename Key>
   void print_key(const Key& key, const char* desc) const {
      special_key(key);
      if (!debug)
         return;
      char buffer[500];
      key_to_buf(key, buffer);
      print_buf(desc, buffer);
   };
   template<typename Iter1, typename Iter2>
   void print_iter(const Iter1& iter, const char* desc, const Iter2& end) const {
      const bool at_end = iter == end;
      if (!at_end)
         special_key(iter->first);
      if (!debug)
         return;
      if (at_end) {
         print_buf(desc, "<end iterator>");
         return;
      }
      const bool spec = true;
      //const bool spec = is_special_key(iter->first);
      print_key(iter->first, desc);
      if (spec) {
         ilog("REM previous: ${prev}, next: ${next}",("prev", iter->second.previous_in_cache)("next", iter->second.next_in_cache));
      }
   };
   struct iterator_state {
      bool     next_in_cache{ false };
      bool     previous_in_cache{ false };
      bool     deleted{ false };
      uint64_t version{ 0 };
   };

   using parent_type           = std::variant<session*, Parent*>;
   using cache_data_store_type = cache;
   using iterator_cache_type   = std::map<shared_bytes, iterator_state>;

   friend Parent;

   // Defines a key ordered, cyclical iterator for traversing a session (which includes parents and children of each
   // session). Basically we are iterating over a list of sessions and each session has its own cache of key/value
   // pairs, along with iterating over a persistent data store all the while maintaining key order with the iterator.
   template <typename Iterator_traits>
   class session_iterator {
    public:
      using difference_type   = typename Iterator_traits::difference_type;
      using value_type        = typename Iterator_traits::value_type;
      using pointer           = typename Iterator_traits::pointer;
      using reference         = typename Iterator_traits::reference;
      using iterator_category = typename Iterator_traits::iterator_category;
      friend session;

    public:
      session_iterator(session* active_session, typename Iterator_traits::iterator_cache_iterator it, uint64_t version);
      session_iterator()                           = default;
      session_iterator(const session_iterator& it) = default;
      session_iterator(session_iterator&&)         = default;

      session_iterator& operator=(const session_iterator& it) = default;
      session_iterator& operator=(session_iterator&&) = default;

      session_iterator& operator++();
      session_iterator& operator--();
      value_type        operator*() const;
      value_type        operator->() const;
      bool              operator==(const session_iterator& other) const;
      bool              operator!=(const session_iterator& other) const;

      bool                deleted() const;
      const shared_bytes& key() const;

    protected:
      template <typename Test_predicate, typename Move_predicate, typename Cache_update>
      void move_(const Test_predicate& test, const Move_predicate& move, Cache_update& update_cache);
      void move_next_();
      void move_previous_();

    private:
      uint64_t                                          m_iterator_version{ 0 };
      typename Iterator_traits::iterator_cache_iterator m_active_iterator;
      session*                                          m_active_session{ nullptr };
   };

   struct iterator_traits {
      using difference_type         = std::ptrdiff_t;
      using value_type              = std::pair<shared_bytes, std::optional<shared_bytes>>;
      using pointer                 = value_type*;
      using reference               = value_type&;
      using iterator_category       = std::bidirectional_iterator_tag;
      using iterator_cache_iterator = typename iterator_cache_type::iterator;
   };
   using iterator = session_iterator<iterator_traits>;

 public:
   session() = default;
   session(Parent& parent);
   session(session& parent);
   session(const session&) = delete;
   session(session&& other);
   ~session();

   session& operator=(const session&) = delete;
   session& operator                  =(session&& other);

   parent_type parent() const;

   const std::unordered_set<shared_bytes>& updated_keys() const;
   const std::unordered_set<shared_bytes>& deleted_keys() const;

   void attach(Parent& parent);
   void attach(session& parent);
   void detach();

   // undo stack operations
   void undo();
   void commit();

   // this part also identifies a concept.  don't know what to call it yet
   std::optional<shared_bytes> read(const shared_bytes& key) const;
   void                        write(const shared_bytes& key, const shared_bytes& value);
   bool                        contains(const shared_bytes& key) const;
   void                        erase(const shared_bytes& key);
   void                        clear();

   // returns a pair containing the key/value pairs found and the keys not found.
   template <typename Iterable>
   const std::pair<std::vector<std::pair<shared_bytes, shared_bytes>>, std::unordered_set<shared_bytes>>
   read(const Iterable& keys) const;

   template <typename Iterable>
   void write(const Iterable& key_values);

   // returns a list of the keys not found.
   template <typename Iterable>
   void erase(const Iterable& keys);

   template <typename Other_data_store, typename Iterable>
   void write_to(Other_data_store& ds, const Iterable& keys) const;

   template <typename Other_data_store, typename Iterable>
   void read_from(const Other_data_store& ds, const Iterable& keys);

   iterator find(const shared_bytes& key) const;
   iterator begin() const;
   iterator end() const;
   iterator lower_bound(const shared_bytes& key) const;

 private:
   void prime_cache_();
   template <typename It, typename Parent_it>
   void next_key_(It& it, Parent_it& pit, Parent_it& pend) const;
   template <typename It, typename Parent_it>
   void previous_key_(It& it, Parent_it& pit, Parent_it& pbegin, Parent_it& pend) const;
   typename iterator_traits::iterator_cache_iterator update_iterator_cache_(const shared_bytes& key, bool deleted, bool overwrite) const;
   template <typename It>
   It& first_not_deleted_in_iterator_cache_(It& it, const It& end, bool& previous_in_cache) const;
   template <typename It>
   It& first_not_deleted_in_iterator_cache_(It& it, const It& end) const;

 private:
   parent_type m_parent{ nullptr };

   // The cache used by this session instance.  This will include all new/update key/value pairs
   // and may include values read from the persistent data store.
   mutable eosio::session::cache m_cache;

   // Indicates if the next/previous key in lexicographical order for a given key exists in the cache.
   // The first value in the pair indicates if the previous key in lexicographical order exists in the cache.
   // The second value in the pair indicates if the next key lexicographical order exists in the cache.
   mutable iterator_cache_type m_iterator_cache;

   // keys that have been updated during this session.
   mutable std::unordered_set<shared_bytes> m_updated_keys;

   // keys that have been deleted during this session.
   mutable std::unordered_set<shared_bytes> m_deleted_keys;
};

template <typename Parent>
typename session<Parent>::parent_type session<Parent>::parent() const {
   return m_parent;
}

template <typename Parent>
void session<Parent>::prime_cache_() {
   // Get the bounds of the parent iterator cache.
   auto lower_key = shared_bytes{};
   auto upper_key = shared_bytes{};

   std::visit(overloaded{ [&](session<Parent>* p) {
                            auto begin = std::begin(p->m_iterator_cache);
                            auto end   = std::end(p->m_iterator_cache);
                            if (begin == end) {
                               return;
                            }
                            lower_key = begin->first;
                            upper_key = (--end)->first;
                         },
                          [&](Parent* p) {
                             auto begin = std::begin(*p);
                             auto end   = std::end(*p);
                             if (begin == end) {
                                return;
                             }
                             lower_key = begin.key();
                             --end;
                             upper_key = end.key();
                          } },
              m_parent);

   if (debug) ilog("REM session cache: ${s}",("s", m_iterator_cache.size()));
   if (lower_key) {
      auto temp = m_iterator_cache.emplace(lower_key, iterator_state{});
      print_iter(temp.first,"prime_cache_ lower", std::end(m_iterator_cache));
      if (debug) ilog("REM ${d}",("d", (temp.second ? "added" : "existed")));
   }
   if (upper_key) {
      auto temp = m_iterator_cache.emplace(upper_key, iterator_state{});
      print_iter(temp.first,"prime_cache_ upper", std::end(m_iterator_cache));
      if (debug) ilog("REM ${d}",("d", (temp.second ? "added" : "existed")));
   }
}

template <typename Parent>
void session<Parent>::clear() {
   m_deleted_keys.clear();
   m_updated_keys.clear();
   m_cache.clear();
   m_iterator_cache.clear();
   if (debug) ilog("REM session clear cache");
}

template <typename Parent>
session<Parent>::session(Parent& parent) : m_parent{ &parent } {
   if (parent.debug)
     this->debug = true;
   attach(parent);
}

template <typename Parent>
session<Parent>::session(session& parent) : m_parent{ &parent } {
   if (parent.debug)
     this->debug = true;
   attach(parent);
}

template <typename Parent>
session<Parent>::session(session&& other)
    : m_parent{ std::move(other.m_parent) }, m_cache{ std::move(other.m_cache) }, m_iterator_cache{ std::move(
                                                                                        other.m_iterator_cache) },
      m_updated_keys{ std::move(other.m_updated_keys) }, m_deleted_keys{ std::move(other.m_deleted_keys) } {
   debug = other.debug;
   session* null_parent = nullptr;
   other.m_parent       = null_parent;
}

template <typename Parent>
session<Parent>& session<Parent>::operator=(session&& other) {
   if (this == &other) {
      return *this;
   }

   m_parent         = std::move(other.m_parent);
   m_cache          = std::move(other.m_cache);
   m_iterator_cache = std::move(other.m_iterator_cache);
   m_updated_keys   = std::move(other.m_updated_keys);
   m_deleted_keys   = std::move(other.m_deleted_keys);

   session* null_parent = nullptr;
   other.m_parent       = null_parent;
   debug = other.debug;

   return *this;
}

template <typename Parent>
session<Parent>::~session() {
   commit();
   undo();
}

template <typename Parent>
void session<Parent>::undo() {
   detach();
   clear();
}

template <typename Parent>
template <typename It, typename Parent_it>
void session<Parent>::previous_key_(It& it, Parent_it& pit, Parent_it& pbegin, Parent_it& pend) const {
   if (it->first) {
      print_iter(it, "previous_key_ start", std::end(m_iterator_cache));
      print_key(pit.key(), "previous_key_ parent start");
      if (pit != pbegin) {
         size_t decrement_count = 0;
         while (pit.key() >= it->first) {
            compare(pit.key(), it->first, "previous_key_ compare");
            --pit;
            print_key(pit.key(), "previous_key_ parent decremented");
            ++decrement_count;
            if (pit == pend) {
               break;
            }
         }
         compare(pit.key(), it->first, "previous_key_ compare lhs selected");

         if (pit != pend) {
            auto temp = m_iterator_cache.emplace(pit.key(), iterator_state{});
            print_iter(temp.first, ( temp.second ? "previous_key_ added parent to cache" :  "previous_key_ NOT added parent to cache"), std::end(m_iterator_cache));
         }

         for (size_t i = 0; i < decrement_count; ++i) { ++pit; }
      } else {
         print_buf("previous_key_ parent start is pbegin","");
      }

      auto& it_cache = const_cast<iterator_cache_type&>(m_iterator_cache);
      auto  lower_it = it_cache.lower_bound(it->first);
      if (lower_it != std::begin(it_cache)) {
         print_iter(lower_it, "previous_key_ lower_bound", std::end(m_iterator_cache));
         --lower_it;
         lower_it->second.next_in_cache = true;
         print_iter(lower_it, "previous_key_ result set start previous and this next", std::end(m_iterator_cache));
         it->second.previous_in_cache   = true;
         print_iter(it, "previous_key_ set previous", std::end(m_iterator_cache));
      }
   }
}

template <typename Parent>
template <typename It, typename Parent_it>
void session<Parent>::next_key_(It& it, Parent_it& pit, Parent_it& pend) const {
   if (it->first) {
      print_iter(it, "next_key_ start", std::end(m_iterator_cache));
      bool decrement = false;
      print_key(pit.key(), "next_key_ parent start");
      if (pit.key() == it->first) {
         ++pit;
         print_iter(it, "next_key_ parent next", std::end(m_iterator_cache));
         decrement = true;
      }
      if (pit != pend) {
         auto temp = m_iterator_cache.emplace(pit.key(), iterator_state{});
         print_iter(temp.first, (temp.second ? "next_key_ added parent to cache" : "next_key_ NOT added parent to cache"), std::end(m_iterator_cache));
      }
      if (decrement) {
         --pit;
      }

      auto& it_cache = const_cast<iterator_cache_type&>(m_iterator_cache);
      auto  lower_it = it_cache.lower_bound(it->first);
      auto  end      = std::end(it_cache);
      if (lower_it != end) {
         print_iter(lower_it, "next_key_ lower_bound", std::end(m_iterator_cache));
         if ((*lower_it).first == it->first) {
            ++lower_it;
         }
         if (lower_it != end) {
            lower_it->second.previous_in_cache = true;
            it->second.next_in_cache           = true;
            print_iter(lower_it, "next_key_ start's next and this previous set true", std::end(m_iterator_cache));
         }
      }
   }
}

template <typename Parent>
const typename std::unordered_set<shared_bytes>& session<Parent>::updated_keys() const {
   return m_updated_keys;
}

template <typename Parent>
const typename std::unordered_set<shared_bytes>& session<Parent>::deleted_keys() const {
   return m_deleted_keys;
}

template <typename Parent>
void session<Parent>::attach(Parent& parent) {
   m_parent = &parent;
   prime_cache_();
}

template <typename Parent>
void session<Parent>::attach(session& parent) {
   m_parent = &parent;
   prime_cache_();
}

template <typename Parent>
void session<Parent>::detach() {
   if (debug) ilog("REM session detatch parent");
   session* null_parent = nullptr;
   m_parent             = null_parent;
}

template <typename Parent>
void session<Parent>::commit() {
   if (m_updated_keys.empty() && m_deleted_keys.empty()) {
      // Nothing to commit.
      return;
   }

   auto write_through = [&](auto& ds) {
      ds.erase(m_deleted_keys);
      m_cache.write_to(ds, m_updated_keys);
      clear();
   };

   std::visit(
         [&](auto* p) {
            if (!p) {
               return;
            }
            write_through(*p);
         },
         m_parent);
}

template <typename Parent>
typename session<Parent>::iterator_traits::iterator_cache_iterator
session<Parent>::update_iterator_cache_(const shared_bytes& key, bool deleted, bool overwrite) const {
   auto  result = m_iterator_cache.emplace(key, iterator_state{});
   auto& it     = result.first;
   const bool orig_debug = debug;
   if (debug) ilog("REM update_iterator_cache_ size: ${s}", ("s", m_iterator_cache.size()));

   if (overwrite) {
      it->second.deleted = deleted;
      if (deleted) {
         print_iter(it, "update_iterator_cache_ deleted", std::end(m_iterator_cache));
         ++it->second.version;
      }
   }
   if (debug) ilog("REM update_iterator_cache_ size: ${s}", ("s", m_iterator_cache.size()));

   print_key(key, "update_iterator_cache_ passed in key");

   if (result.second) {
      // The two keys that this new key is being inserted inbetween may have been contiguous in the global order.
      // If so, that means we already know the global order of this new key.
      print_iter(it, "update_iterator_cache_ start", std::end(m_iterator_cache));
      if (it != std::begin(m_iterator_cache)) {
         auto previous = it;
         --previous;
         print_iter(previous, "update_iterator_cache_ previous", std::end(m_iterator_cache));
         if (previous->second.next_in_cache) {
            print_buf("setting start's previous and next","");
            it->second.previous_in_cache = true;
            it->second.next_in_cache     = true;
            return it;
         }
      }
      else {
         auto end = std::end(m_iterator_cache);
         if (it != end) {
            auto next = it;
            ++next;
            print_iter(next, "update_iterator_cache_ next", std::end(m_iterator_cache));
            if (next != end && next->second.previous_in_cache) {
               print_buf("setting start's previous and next","");
               it->second.next_in_cache     = true;
               it->second.previous_in_cache = true;
               return it;
            }
         }
      }

      if (!it->second.next_in_cache || !it->second.previous_in_cache) {
         std::visit(
               [&](auto* p) {
                  auto pit     = p->lower_bound(key);
                  auto end     = std::end(*p);
                  if (!it->second.next_in_cache) {
                     next_key_(it, pit, end);
                  }
                  if (!it->second.previous_in_cache) {
                     auto begin   = std::begin(*p);
                     previous_key_(it, pit, begin, end);
                  }
               },
               m_parent);
      }

   } else {
      print_iter(it, "update_iterator_cache_ not added", std::end(m_iterator_cache));
   }
   if (orig_debug && !debug) {
      print_key(key, "update_iterator_cache_ passed in key");
   }
   if (debug) ilog("REM now update_iterator_cache_ size: ${s}", ("s", m_iterator_cache.size()));

   return it;
}

template <typename Parent>
std::optional<shared_bytes> session<Parent>::read(const shared_bytes& key) const {
   // Find the key within the session.
   // Check this level first and then traverse up to the parent to see if this key/value
   // has been read and/or update.
   print_key(key, "read");
   if (m_deleted_keys.find(key) != std::end(m_deleted_keys)) {
      // key has been deleted at this level.
      return {};
   }

   auto value = m_cache.read(key);
   if (value) {
      return value;
   }

   std::visit(
         [&](auto* p) {
            if (p) {
               value = p->read(key);
            }
         },
         m_parent);

   if (value) {
      m_cache.write(key, *value);
      update_iterator_cache_(key, false, false);
   }

   return value;
}

template <typename Parent>
void session<Parent>::write(const shared_bytes& key, const shared_bytes& value) {
   m_updated_keys.emplace(key);
   m_deleted_keys.erase(key);
   m_cache.write(key, value);
   print_key(key, "write");
   auto it            = update_iterator_cache_(key, false, true);
   it->second.deleted = false;
}

template <typename Parent>
bool session<Parent>::contains(const shared_bytes& key) const {
   // Traverse the heirarchy to see if this session (and its parent session)
   // has already read the key into memory.

   print_key(key, "contains");
   if (m_deleted_keys.find(key) != std::end(m_deleted_keys)) {
      return false;
   }
   if (m_cache.find(key) != std::end(m_cache)) {
      return true;
   }

   return std::visit(
         [&](auto* p) {
            if (p && p->contains(key)) {
               update_iterator_cache_(key, false, false);
               return true;
            }
            return false;
         },
         m_parent);
}

template <typename Parent>
void session<Parent>::erase(const shared_bytes& key) {
   m_deleted_keys.emplace(key);
   m_updated_keys.erase(key);
   m_cache.erase(key);
   print_key(key, "erase");
   auto it            = update_iterator_cache_(key, true, true);
   it->second.deleted = true;
}

// Reads a batch of keys from the session.
//
// \tparam Iterable Any type that can be used within a range based for loop and returns shared_bytes instances in its
// iterator. \param keys An iterable instance that returns shared_bytes instances in its iterator. \returns An std::pair
// where the first item is list of the found key/value pairs and the second item is a set of the keys not found.
template <typename Parent>
template <typename Iterable>
const std::pair<std::vector<std::pair<shared_bytes, shared_bytes>>, std::unordered_set<shared_bytes>>
session<Parent>::read(const Iterable& keys) const {
   auto not_found = std::unordered_set<shared_bytes>{};
   auto kvs       = std::vector<std::pair<shared_bytes, shared_bytes>>{};

   for (const auto& key : keys) {
      auto value = read(key);
      if (value != shared_bytes{}) {
         kvs.emplace_back(key, value);
      } else {
         not_found.emplace(key);
      }
   }

   return { std::move(kvs), std::move(not_found) };
}

// Writes a batch of key/value pairs to the session.
//
// \tparam Iterable Any type that can be used within a range based for loop and returns key/value pairs in its
// iterator. \param key_values An iterable instance that returns key/value pairs in its iterator.
template <typename Parent>
template <typename Iterable>
void session<Parent>::write(const Iterable& key_values) {
   // Currently the batch write will just iteratively call the non batch write
   for (const auto& kv : key_values) { write(kv.first, kv.second); }
}

// Erases a batch of key_values from the session.
//
// \tparam Iterable Any type that can be used within a range based for loop and returns shared_bytes instances in its
// iterator. \param keys An iterable instance that returns shared_bytes instances in its iterator.
template <typename Parent>
template <typename Iterable>
void session<Parent>::erase(const Iterable& keys) {
   // Currently the batch erase will just iteratively call the non batch erase
   for (const auto& key : keys) { erase(key); }
}

template <typename Parent>
template <typename Other_data_store, typename Iterable>
void session<Parent>::write_to(Other_data_store& ds, const Iterable& keys) const {
   auto results = std::vector<std::pair<shared_bytes, shared_bytes>>{};
   for (const auto& key : keys) {
      auto value = read(key);
      if (value != shared_bytes{}) {
         results.emplace_back(shared_bytes(key.data(), key.size()), shared_bytes(value.data(), value.size()));
      }
   }
   ds.write(results);
}

template <typename Parent>
template <typename Other_data_store, typename Iterable>
void session<Parent>::read_from(const Other_data_store& ds, const Iterable& keys) {
   ds.write_to(*this, keys);
}

template <typename Parent>
template <typename It>
It& session<Parent>::first_not_deleted_in_iterator_cache_(It& it, const It& end, bool& previous_in_cache) const {
   auto previous_known       = true;
   auto update_previous_flag = [&](auto& it) {
      if (previous_known) {
         previous_known = it->second.previous_in_cache;
      }
      if(debug) ilog("REM previous_known: ${p}",("p", previous_known));
   };
   while (it != end && it->second.deleted) {
      print_iter(it, "find not deleted", std::end(m_iterator_cache));
      update_previous_flag(it);
      ++it;
   }
   if(it != end)
      print_iter(it, "found not deleted", std::end(m_iterator_cache));
   update_previous_flag(it);
   previous_in_cache = previous_known;
   return it;
}

template <typename Parent>
template <typename It>
It& session<Parent>::first_not_deleted_in_iterator_cache_(It& it, const It& end) const {
   while (it != end) {
      auto find_it = m_iterator_cache.find(it.key());
      if (find_it != std::end(m_iterator_cache))
         print_iter(find_it, "checking cache", std::end(m_iterator_cache));
      if (find_it == std::end(m_iterator_cache) || !find_it->second.deleted) {
         return it;
      }
      ++it;
   }
   if(debug) ilog("REM reached end");

   return it;
}

template <typename Parent>
typename session<Parent>::iterator session<Parent>::find(const shared_bytes& key) const {
   auto  version  = uint64_t{ 0 };
   auto& it_cache = const_cast<iterator_cache_type&>(m_iterator_cache);
   auto  end      = std::end(it_cache);
   auto  it       = it_cache.find(key);
   if (it == end) {
      std::visit(
            [&](auto* p) {
               auto pit = p->find(key);
               if (pit != std::end(*p)) {
                  it = it_cache.emplace(key, iterator_state{}).first;
                  print_iter(it, "found in parent", std::end(m_iterator_cache));
               }
            },
            m_parent);
   }

   if (it != end) {
      print_iter(it, "found", std::end(m_iterator_cache));
      version = it->second.version;
      if (it->second.deleted) {
         if (debug) ilog("REM but deleted");
         it = std::move(end);
      }
   }
   return { const_cast<session*>(this), std::move(it), version };
}

template <typename Parent>
typename session<Parent>::iterator session<Parent>::begin() const {
   auto& it_cache = const_cast<iterator_cache_type&>(m_iterator_cache);
   auto  end      = std::end(it_cache);
   auto  begin    = std::begin(it_cache);
   auto  it       = begin;
   auto  version  = uint64_t{ 0 };

   bool previous_in_cache = true;
   print_iter(it, "begin start", end);
   first_not_deleted_in_iterator_cache_(it, end, previous_in_cache);
   if ((it == end) || (it != begin && !previous_in_cache)) {
      // We have a begin iterator in this session, but we don't have enough
      // information to determine if that iterator is globally the begin iterator.
      // We need to ask the parent for its begin iterator and compare the two
      // to see which comes first lexicographically.
      auto pending_key = shared_bytes{};
      if (it != end) {
         pending_key = (*it).first;
         print_key(pending_key, "pending_key");
      }
      std::visit(
            [&](auto* p) {
               auto pit  = std::begin(*p);
               auto pend = std::end(*p);
               print_key(pit.key(), "pit start");
               first_not_deleted_in_iterator_cache_(pit, pend);
               if (pit != pend) {
                  print_key(pit.key(), "pit skip deletes");
                  if (!pending_key || pit.key() < pending_key) {
                     auto temp = it_cache.emplace(pit.key(), iterator_state{});
                     it = temp.first;
                     print_iter(it, "pit instead of pending_key", end);
                     if (debug) {
                        if (temp.second)
                           ilog("REM added pit");
                        else
                           ilog("REM pit existed");
                     }
                  }
               }
            },
            m_parent);
   }

   if (it != end) {
      version = it->second.version;
   }
   return { const_cast<session*>(this), std::move(it), version };
}

template <typename Parent>
typename session<Parent>::iterator session<Parent>::end() const {
   auto& it_cache = const_cast<iterator_cache_type&>(m_iterator_cache);
   return { const_cast<session*>(this), std::end(it_cache), 0 };
}

template <typename Parent>
typename session<Parent>::iterator session<Parent>::lower_bound(const shared_bytes& key) const {
   auto& it_cache = const_cast<iterator_cache_type&>(m_iterator_cache);
   auto  version  = uint64_t{ 0 };
   auto  end      = std::end(it_cache);
   auto  it       = it_cache.lower_bound(key);

   if (it != end)
      print_iter(it, "first one", end);

   bool previous_in_cache = true;
   first_not_deleted_in_iterator_cache_(it, end, previous_in_cache);
   if (it == end || ((*it).first != key && !previous_in_cache)) {
      // So either:
      // 1.  We didn't find a key in the iterator cache (pending_key is invalid).
      // 2.  The pending_key is not exactly the key and the found key in the iterator cache currently doesn't know what
      // its previous key is.
      auto pending_key = shared_bytes{};
      if (it != end) {
         pending_key = (*it).first;
         print_key(pending_key, "pending_key");
      }
      std::visit(
            [&](auto* p) {
               auto pit  = p->lower_bound(key);
               auto pend = std::end(*p);
               first_not_deleted_in_iterator_cache_(pit, pend);
               if (pit != pend) {
                  if(pending_key) print_key(pending_key, "pending_key");
                  print_key(pit.key(), "pit.key");
                  if (!pending_key || pit.key() < pending_key) {
                     print_buf("using parent's key", "");
                     auto temp = it_cache.emplace(pit.key(), iterator_state{});
                     it = temp.first;
                     print_iter(it, (temp.second ? "added parent's key" : "parent's key existed"), end);
                  }
               }
            },
            m_parent);
   }
   if (it != end) {
      version = it->second.version;
      if (it->second.deleted) {
         print_iter(it, "deleting", end);
         it = std::move(end);
      }
   }
   return { const_cast<session*>(this), std::move(it), version };
}

template <typename Parent>
template <typename Iterator_traits>
session<Parent>::session_iterator<Iterator_traits>::session_iterator(
      session* active_session, typename Iterator_traits::iterator_cache_iterator it, uint64_t version)
    : m_iterator_version{ version }, m_active_iterator{ std::move(it) }, m_active_session{ active_session } {}

// Moves the current iterator.
//
// \tparam Predicate A functor that indicates if we are incrementing or decrementing the current iterator.
// \tparam Comparator A functor used to determine what the current iterator is.
template <typename Parent>
template <typename Iterator_traits>
template <typename Test_predicate, typename Move_predicate, typename Cache_update>
void session<Parent>::session_iterator<Iterator_traits>::move_(const Test_predicate& test, const Move_predicate& move,
                                                               Cache_update& update_cache) {
   do {
      if (m_active_iterator != std::end(m_active_session->m_iterator_cache) && !test(m_active_iterator)) {
         // Force an update to see if we pull in a next or previous key from the current key.
         if (!update_cache(m_active_iterator)) {
            // The test still fails.  We are at the end.
            m_active_iterator = std::end(m_active_session->m_iterator_cache);
            break;
         }
      }
      // Move to the next iterator in the cache.
      move(m_active_iterator);
      if (m_active_iterator == std::end(m_active_session->m_iterator_cache) || !m_active_iterator->second.deleted) {
         // We either found a key that hasn't been deleted or we hit the end of the iterator cache.
         break;
      }
   } while (true);
}

template <typename Parent>
template <typename Iterator_traits>
void session<Parent>::session_iterator<Iterator_traits>::move_next_() {
   auto move         = [](auto& it) { ++it; };
   auto test         = [](auto& it) { return it->second.next_in_cache; };
   auto update_cache = [&](auto& it) mutable {
      auto pending_key = eosio::session::shared_bytes{};
      auto end         = std::end(m_active_session->m_iterator_cache);
      if (it != end) {
         m_active_session->print_iter(it, "starting key", end);
         ++it;
         if (it != end) {
            pending_key = it->first;
            m_active_session->print_iter(it, "pending_key", end);
         }
         --it;
         m_active_session->print_iter(it, "starting key", end);
      }

      auto key = std::visit(
            [&, session=m_active_session](auto* p) {
               auto pit = p->lower_bound(it->first);
               session->print_key(pit.key(), "next pit");
               if (pit != std::end(*p) && pit.key() == it->first) {
                  ++pit;
                  session->print_key(pit.key(), "next pit incremented");
               }
               return pit.key();
            },
            m_active_session->m_parent);

      // We have two candidates for the next key.
      // 1. The next key in order in this sessions cache.
      // 2. The next key in lexicographical order retrieved from the sessions parent.
      // Choose which one it is.
      m_active_session->print_key(key,"instead of pending_key");
      if (pending_key && pending_key < key) {
         key = pending_key;
         m_active_session->print_key(key,"chose pending_key");
      }

      if (key) {
         auto nit                            = m_active_session->m_iterator_cache.emplace(key, iterator_state{});
         if (m_active_session->debug) {
            if (!nit.second) {
               m_active_session->print_iter(nit.first, "updating set starting key next and this prev", std::end(m_active_session->m_iterator_cache));
            } else {
               m_active_session->print_iter(nit.first, "adding set starting key next and this prev", std::end(m_active_session->m_iterator_cache));
            }
         }
         nit.first->second.previous_in_cache = true;
         it->second.next_in_cache            = true;
         m_active_session->print_iter(nit.first, "now", std::end(m_active_session->m_iterator_cache));
         return true;
      }
      return false;
   };

   if (m_active_iterator == std::end(m_active_session->m_iterator_cache)) {
      m_active_iterator = std::begin(m_active_session->m_iterator_cache);
   } else {
      move_(test, move, update_cache);
   }
}

template <typename Parent>
template <typename Iterator_traits>
void session<Parent>::session_iterator<Iterator_traits>::move_previous_() {
   auto move = [](auto& it) { --it; };
   auto test = [&](auto& it) {
      if (it != std::end(m_active_session->m_iterator_cache)) {
         return it->second.previous_in_cache;
      }
      return true;
   };
   auto update_cache = [&](auto& it) mutable {
      auto pending_key = eosio::session::shared_bytes{};
      if (it != std::begin(m_active_session->m_iterator_cache)) {
         m_active_session->print_iter(it, "starting key", std::end(m_active_session->m_iterator_cache));
         --it;
         pending_key = it->first;
         m_active_session->print_iter(it, "pending_key", std::end(m_active_session->m_iterator_cache));
         ++it;
         m_active_session->print_iter(it, "starting key", std::end(m_active_session->m_iterator_cache));
      }

      auto key = std::visit(
            [&,session=m_active_session](auto* p) {
               auto pit = p->lower_bound(it->first);
               if (pit != std::begin(*p)) {
                  if (pit != std::end(*p))
                     session->print_key(pit.key(), "pit");
                  --pit;
               } else {
                  session->print_buf("no pit", "");
                  return eosio::session::shared_bytes{};
               }
               if (pit != std::end(*p))
                  session->print_key(pit.key(), "pit key");
               return pit.key();
            },
            m_active_session->m_parent);

      // We have two candidates to consider.
      // 1. The key returned by decrementing the iterator on this session's cache.
      // 2. The key returned by calling lower_bound on the parent of this session.
      // We want the larger of the two.
      if (pending_key && pending_key > key) {
         key = pending_key;
         m_active_session->print_key(key,"chose pending");
      }

      if (key) {
         auto nit                        = m_active_session->m_iterator_cache.emplace(key, iterator_state{});
         if (m_active_session->debug) {
            if (!nit.second) {
               m_active_session->print_iter(nit.first, "updating  set starting key previous and this next", std::end(m_active_session->m_iterator_cache));
               ilog("REM was next's prev: ${p}, and next: ${n}",("p",it->second.previous_in_cache)("n",nit.first->second.next_in_cache));
            } else {
               m_active_session->print_iter(nit.first, "adding  set starting key previous and this next", std::end(m_active_session->m_iterator_cache));
            }
         }
         nit.first->second.next_in_cache = true;
         it->second.previous_in_cache    = true;
         return true;
      }
      else {
         m_active_session->print_buf("no key","");
      }
      return false;
   };

   if (m_active_iterator == std::begin(m_active_session->m_iterator_cache)) {
      m_active_iterator = std::end(m_active_session->m_iterator_cache);
   } else {
      move_(test, move, update_cache);
   }
}

template <typename Parent>
template <typename Iterator_traits>
typename session<Parent>::template session_iterator<Iterator_traits>&
session<Parent>::session_iterator<Iterator_traits>::operator++() {
   move_next_();
   return *this;
}

template <typename Parent>
template <typename Iterator_traits>
typename session<Parent>::template session_iterator<Iterator_traits>&
session<Parent>::session_iterator<Iterator_traits>::operator--() {
   move_previous_();
   return *this;
}

template <typename Parent>
template <typename Iterator_traits>
bool session<Parent>::session_iterator<Iterator_traits>::deleted() const {
   if (m_active_iterator == std::end(m_active_session->m_iterator_cache)) {
      return false;
   }

   return m_active_iterator->second.deleted || m_iterator_version != m_active_iterator->second.version;
}

template <typename Parent>
template <typename Iterator_traits>
const shared_bytes& session<Parent>::session_iterator<Iterator_traits>::key() const {
   if (m_active_iterator == std::end(m_active_session->m_iterator_cache)) {
      static auto empty = shared_bytes{};
      return empty;
   }
   return m_active_iterator->first;
}

template <typename Parent>
template <typename Iterator_traits>
typename session<Parent>::template session_iterator<Iterator_traits>::value_type
session<Parent>::session_iterator<Iterator_traits>::operator*() const {
   if (m_active_iterator == std::end(m_active_session->m_iterator_cache)) {
      return std::pair{ shared_bytes{}, std::optional<shared_bytes>{} };
   }
   return std::pair{ m_active_iterator->first, m_active_session->read(m_active_iterator->first) };
}

template <typename Parent>
template <typename Iterator_traits>
typename session<Parent>::template session_iterator<Iterator_traits>::value_type
session<Parent>::session_iterator<Iterator_traits>::operator->() const {
   if (m_active_iterator == std::end(m_active_session->m_iterator_cache)) {
      return std::pair{ shared_bytes{}, std::optional<shared_bytes>{} };
   }
   return std::pair{ m_active_iterator->first, m_active_session->read(m_active_iterator->first) };
}

template <typename Parent>
template <typename Iterator_traits>
bool session<Parent>::session_iterator<Iterator_traits>::operator==(const session_iterator& other) const {
   if (m_active_iterator == std::end(m_active_session->m_iterator_cache) &&
       m_active_iterator == other.m_active_iterator) {
      return true;
   }
   if (other.m_active_iterator == std::end(m_active_session->m_iterator_cache)) {
      return false;
   }
   return this->m_active_iterator == other.m_active_iterator;
}

template <typename Parent>
template <typename Iterator_traits>
bool session<Parent>::session_iterator<Iterator_traits>::operator!=(const session_iterator& other) const {
   return !(*this == other);
}

} // namespace eosio::session
