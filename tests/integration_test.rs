use std::alloc::{GlobalAlloc, Layout, System};
use std::mem::size_of;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use rusty_fork::rusty_fork_test;

use glommio::channels::channel_mesh::FullMesh;
use glommio::enclose;
use glommio::prelude::*;
use hst_alloc::{HstAlloc, MemoryLocation};
use log::LevelFilter;

#[global_allocator]
static ALLOCATOR: HstAlloc<System> = HstAlloc::new(System);

struct Zero;

#[derive(Default)]
struct Object {
    f2: u32,
    f1: usize,
}

fn init_logger() {
    pretty_env_logger::formatted_builder()
        .filter_level(LevelFilter::Info)
        .try_init()
        .ok();
}

rusty_fork_test! {
    #[test]
    fn allocate_zero_sized_object() {
        init_logger();
        let nr_shards = 1;
        let id_gen = Arc::new(AtomicU8::new(0));

        ALLOCATOR.initialize(nr_shards);

        let handles = LocalExecutorPoolBuilder::new(1)
            .on_all_shards(move || async move {
                let layout = Layout::new::<Zero>();
                let ptr1 = unsafe { ALLOCATOR.alloc(layout.clone()) };

                let id = id_gen.fetch_add(1, Ordering::AcqRel);
                ALLOCATOR.create_local_allocator(id);

                let ptr2 = unsafe { ALLOCATOR.alloc(layout) };
                assert_eq!(ptr1, ptr2);
                assert_eq!(ALLOCATOR.memory_location(ptr1), MemoryLocation::Global);

                unsafe { ALLOCATOR.dealloc(ptr1, layout) }
                unsafe { ALLOCATOR.dealloc(ptr2, layout) }
            })
            .unwrap();

        for result in handles.join_all() {
            result.unwrap();
        }
    }
}

rusty_fork_test! {
    #[test]
    fn allocate_non_zero_sized_object() {
        init_logger();

        #[derive(Debug)]
        struct Ptr(*mut u8);
        unsafe impl Send for Ptr {}

        let nr_shards = 2;

        ALLOCATOR.enable_log();
        ALLOCATOR.initialize(nr_shards);

        let mesh = FullMesh::full(nr_shards as _, 1);

        let shard1 = LocalExecutorBuilder::new()
            .spawn(enclose!((mesh) move || async move {
                let (senders, receivers) = mesh.join().await.unwrap();
                unsafe {
                    let shard_id = senders.peer_id();
                    let peer_id = 1 - senders.peer_id();

                    let ptr1 = ALLOCATOR.alloc(Layout::new::<Object>());
                    assert_eq!(MemoryLocation::Global, ALLOCATOR.memory_location(ptr1));
                    assert_eq!(None, ALLOCATOR.allocated_bytes());

                    ALLOCATOR.create_local_allocator(shard_id as _);
                    let ptr2 = ALLOCATOR.alloc(Layout::new::<Object>());
                    assert_eq!(MemoryLocation::Global, ALLOCATOR.memory_location(ptr1));
                    assert_eq!(MemoryLocation::Local(shard_id as _), ALLOCATOR.memory_location(ptr2));
                    assert_eq!(Some(size_of::<Object>()), ALLOCATOR.allocated_bytes());

                    senders.send_to(peer_id, Ptr(ptr2)).await.unwrap();

                    let ptr = receivers.recv_from(peer_id).await.unwrap().unwrap();
                    ALLOCATOR.dealloc(ptr.0, Layout::new::<Object>());
                }
            }))
            .unwrap();

        let shard2 = LocalExecutorBuilder::new()
            .spawn(enclose!((mesh) move || async move {
                let (senders, receivers) = mesh.join().await.unwrap();
                let shard_id = senders.peer_id();
                let peer_id = 1 - senders.peer_id();

                ALLOCATOR.create_local_allocator(shard_id as _);

                let ptr = receivers.recv_from(peer_id).await.unwrap().unwrap();
                assert_eq!(MemoryLocation::Foreign(peer_id as _), ALLOCATOR.memory_location(ptr.0));

                senders.send_to(peer_id, ptr).await.unwrap();
            }))
            .unwrap();

        shard2.join().unwrap();
        shard1.join().unwrap();
    }
}

rusty_fork_test! {
    #[test]
    fn allocate_all_sizes() {
        init_logger();

        let nr_shards = 1;

        ALLOCATOR.initialize(nr_shards);

        LocalExecutorBuilder::new()
            .spawn(move || async move {
                unsafe {
                    let shard_id = 0;
                    ALLOCATOR.create_local_allocator(shard_id as _);
                    for i in 0..8192 {
                        let layout = Layout::from_size_align(i, 8).unwrap();
                        let ptr = ALLOCATOR.alloc(layout);
                        assert!(!ptr.is_null());
                        ALLOCATOR.dealloc(ptr, layout);
                    }
                }
            })
            .unwrap()
            .join()
            .unwrap();
    }
}

rusty_fork_test! {
    #[test]
    fn allocated_bytes() {
        init_logger();

        let nr_shards = 1;

        ALLOCATOR.initialize(nr_shards);

        LocalExecutorBuilder::new()
            .spawn(move || async move {
                unsafe {
                    let shard_id = 0;
                    ALLOCATOR.create_local_allocator(shard_id as _);
                    assert_eq!(0, ALLOCATOR.allocated_bytes().unwrap());

                    let small_layout = Layout::from_size_align(837, 1).unwrap();
                    let small = ALLOCATOR.alloc(small_layout);
                    assert_eq!(896, ALLOCATOR.allocated_bytes().unwrap());
                    ALLOCATOR.dealloc(small, small_layout);
                    assert_eq!(0, ALLOCATOR.allocated_bytes().unwrap());

                    let large_layout = Layout::from_size_align(1223471, 4096).unwrap();
                    let large = ALLOCATOR.alloc(large_layout);
                    assert_eq!(2097152, ALLOCATOR.allocated_bytes().unwrap());
                    ALLOCATOR.dealloc(large, large_layout);
                    assert_eq!(0, ALLOCATOR.allocated_bytes().unwrap());
                }
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
