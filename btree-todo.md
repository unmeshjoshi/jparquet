# BoltDB-Style B+Tree Implementation TODO

## Phase 1: File Setup and Metadata

### 1.1 DB Class Setup
- [x] **Page Structure**
  - [x] Implement BoltDB-style page layout with 16-byte header
  - [x] Create Element class for page entries
  - [x] Add methods for page operations (get/set elements)
  - [x] Implement binary search for key lookup

- [x] **Page Manager**
  - [x] Implement PageManager for allocation, reading, and writing
  - [x] Add simple in-memory page caching
  - [x] Support file-based persistence
  - [x] Test page allocation and retrieval

- [x] **DB Class (Empty)**
  - [x] Create an empty DB class
  - [x] Write basic test for DB class initialization

- [x] **DB Constructor (File Path, Options)**
  - [x] Implement constructor with file path parameter
  - [x] Add options for page size configuration
  - [x] Test file creation if it doesn't exist
  - [x] Test opening existing file

### 1.2 Metadata Management
- [x] **Metadata Class**
  - [x] Create Metadata class (rootPageId, freelistPageId, pageSize, nextPageId)
  - [x] Implement serialization/deserialization to/from ByteBuffer
  - [x] Test metadata serialization and deserialization

- [x] **Initial File Writing (Metadata)**
  - [x] Modify DB constructor to write initial metadata to page 0
  - [x] Test metadata initialization for new database files

- [x] **Loading Metadata**
  - [x] Add logic to read metadata from existing database files
  - [x] Test correct loading of metadata values

## Phase 2: Memory Mapping

### 2.1 Memory-Mapped File I/O
- [x] **mmap() Implementation**
  - [x] Create helper method to memory-map database file
  - [x] Use FileChannel.map() for memory mapping
  - [x] Test reading metadata from MappedByteBuffer
  - [x] Handle cases where file size is smaller than mapping size

- [x] **munmap() Implementation**
  - [x] Add method to unmap MappedByteBuffer (using reflection)
  - [x] Test proper resource release

- [x] **Page Access Methods**
  - [x] Create methods to access pages by pageId
  - [x] Implement offset calculation within mapped region
  - [x] Test page access functionality

## Phase 3: Freelist Management

### 3.1 Freelist Implementation
- [x] **Freelist Class**
  - [x] Create Freelist class to manage free page IDs
  - [x] Implement methods to add, remove, and allocate page IDs
  - [x] Design serialization format for freelist

- [x] **Page Allocation**
  - [x] Implement allocation from freelist
  - [x] Add fallback to allocate from end of file
  - [x] Test page allocation logic

- [x] **Page Freeing**
  - [x] Implement page freeing (return to freelist)
  - [x] Test page freeing functionality

- [x] **Freelist Persistence**
  - [x] Implement serialization to dedicated page(s)
  - [x] Add deserialization from page(s)
  - [x] Test persistence across DB restarts

## Phase 4: B+Tree Node Implementation

### 4.1 Node Structure with Page IDs
- [x] **Node Class Refactoring**
  - [x] Refactor BTreeNode to store page IDs instead of direct references
  - [x] Add pageId field to Node class
  - [x] Update serialization/deserialization for page IDs
  - [x] Test node structure with page IDs

- [x] **Node Loading from Page ID**
  - [x] Implement method to load Node from pageId
  - [x] Test node loading functionality

- [x] **Node Writing to Page ID**
  - [x] Implement method to write Node to pageId
  - [x] Test node writing functionality

## Phase 5: B+Tree Implementation with Page IDs

### 5.1 Core B+Tree Operations
- [x] **BTree Class Refactoring**
  - [x] Refactor BTree class to work with page IDs
  - [x] Update tree traversal to use page loading/writing

- [x] **Search Implementation**
  - [x] Refactor search() to work with page IDs
  - [x] Test search across multiple levels

- [x] **Insert Implementation**
  - [x] Refactor insert() to work with page IDs
  - [x] Handle node splitting with page allocation
  - [x] Test insert with splits

- [x] **Delete Implementation**
  - [x] Refactor delete() to work with page IDs
  - [x] Handle node merging and rebalancing
  - [x] Test delete operations

## Phase 6: Copy-on-Write Transactions

### 6.1 Transaction Management
- [x] **Transaction Class**
  - [x] Create Transaction class
  - [x] Implement read-only and read-write transaction types
  - [x] Test transaction creation

- [x] **Read-Only Transactions**
  - [x] Implement read-only transaction logic
  - [x] Test concurrent read-only transactions
  - [x] Verify isolation between transactions

- [x] **Read-Write Transactions (CoW)**
  - [x] Implement copy-on-write for page modifications
  - [x] Track modified pages in transaction
  - [x] Test isolation of uncommitted changes

- [x] **Transaction Commit**
  - [x] Implement commit() method
  - [x] Write modified pages to disk
  - [x] Update root page ID atomically
  - [x] Update freelist
  - [x] Test commit functionality

- [x] **Transaction Rollback**
  - [x] Implement rollback() method
  - [x] Release allocated pages
  - [x] Test rollback functionality

## Phase 7: Buckets Implementation

### 7.1 Bucket Abstraction
- [x] **Bucket Class**
  - [x] Create Bucket class wrapping a B+Tree
  - [x] Implement bucket creation, retrieval, deletion
  - [x] Support nested buckets
  - [x] Test bucket operations

## Phase 8: BoltDB-like API

### 8.1 High-Level API
- [x] **DB.Update() and DB.View()**
  - [x] Implement Update() for read-write transactions
  - [x] Implement View() for read-only transactions
  - [x] Support callback functions (lambdas)
  - [x] Test high-level API

## Implementation Notes

### Key Considerations
- Ensure page size is a multiple of OS page size (4KB)
- Implement atomic updates for metadata
- Handle I/O exceptions properly
- Manage concurrency with transactions
- Carefully implement freelist serialization

### Performance Optimization
- [ ] Optimize memory management
- [ ] Enhance page caching strategy
- [ ] Improve search and insertion algorithms
- [ ] Benchmark and profile critical operations

### Security and Error Handling
- [ ] Add robust error handling
- [ ] Implement recovery mechanisms
- [ ] Consider access control for future versions
