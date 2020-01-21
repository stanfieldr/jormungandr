use crate::network::{
    client::ConnectHandle,
    p2p::{
        comm::{PeerComms, PeerInfo, PeerStats},
        Id,
    },
};
use linked_hash_map::LinkedHashMap;
use std::net::SocketAddr;
use std::net::IpAddr;
use std::time::SystemTime;

pub struct PeerMap {
    map: LinkedHashMap<Id, PeerData>,
    capacity: usize,
}

#[derive(Default)]
struct PeerData {
    addr: Option<SocketAddr>,
    comms: PeerComms,
    stats: PeerStats,
    connecting: Option<ConnectHandle>,
    error: bool,
}

pub enum CommStatus<'a> {
    Connecting(&'a mut PeerComms),
    Established(&'a mut PeerComms),
}

impl PeerData {
    fn new(comms: PeerComms, addr: SocketAddr) -> Self {
        PeerData {
            addr: Some(addr),
            comms,
            stats: PeerStats::default(),
            connecting: None,
            error: false,
        }
    }

    fn update_comm_status(&mut self) -> CommStatus<'_> {
        if let Some(ref mut handle) = self.connecting {
            match handle.try_complete() {
                Ok(None) => return CommStatus::Connecting(&mut self.comms),
                Ok(Some(comms)) => {
                    self.connecting = None;
                    self.comms.update(comms);
                }
                Err(_) => {
                    self.connecting = None;
                    self.error = true;
                }
            }
        }
        CommStatus::Established(&mut self.comms)
    }

    fn server_comms(&mut self) -> &mut PeerComms {
        // This method is called when a subscription request is received
        // by the server, normally at the beginning of the peer connecting
        // as a client. Cancel client connection if it is pending.
        self.connecting = None;
        self.comms.clear_pending();
        &mut self.comms
    }
}

impl<'a> CommStatus<'a> {
    fn comms(self) -> &'a mut PeerComms {
        match self {
            CommStatus::Connecting(comms) => comms,
            CommStatus::Established(comms) => comms,
        }
    }
}

impl PeerMap {
    pub fn new(capacity: usize) -> Self {
        PeerMap {
            map: LinkedHashMap::new(),
            capacity,
        }
    }

    pub fn entry<'a>(&'a mut self, id: Id) -> Option<Entry<'a>> {
        use linked_hash_map::Entry::*;

        match self.map.entry(id) {
            Vacant(_) => None,
            Occupied(entry) => Some(Entry { inner: entry }),
        }
    }

    /// for clearing the peer map
    pub fn clear(&mut self) {
        self.map.clear()
    }

    pub fn gc(&mut self, at_most: usize) -> usize {
        let mut len = 0;

        for entry in self.map.entries() {
            let ip_addr = entry.get().addr.as_ref().map(|socket| socket.ip());

            if ip_addr.is_some() {
                let is_private = match ip_addr.unwrap() {
                    IpAddr::V4(ip4) => ip4.is_private(),
                    IpAddr::V6(ip6) => false
                };

                if is_private {
                    continue;
                }
            }

            let time_since = match entry.get().stats.last_block_received() {
                Some(t) => match SystemTime::now().duration_since(t) {
                    Ok(n) => n.as_secs(),
                    Err(_) => 42
                },
                None => 42
            };

            if time_since < 42 {
                // TODO: check for more reasons to evict
            } else if entry.get().connecting.is_none() {
                let _peer = entry.remove();
                len += 1;
            }

            if len >= at_most {
                break;
            }
        }

        len
    }

    pub fn refresh_peer(&mut self, id: &Id) -> Option<&mut PeerStats> {
        self.map.get_refresh(&id).map(|data| &mut data.stats)
    }

    pub fn peer_comms(&mut self, id: &Id) -> Option<&mut PeerComms> {
        self.map
            .get_mut(id)
            .map(|data| data.update_comm_status().comms())
    }

    fn ensure_peer(&mut self, id: Id) -> &mut PeerData {
        if !self.map.contains_key(&id) {
            self.evict_if_full();
        }
        self.map.entry(id).or_insert_with(Default::default)
    }

    pub fn server_comms(&mut self, id: Id) -> &mut PeerComms {
        self.ensure_peer(id).server_comms()
    }

    pub fn insert_peer(&mut self, id: Id, comms: PeerComms, addr: SocketAddr) {
        self.evict_if_full();
        let data = PeerData::new(comms, addr);
        self.map.insert(id, data);
    }

    pub fn add_connecting(&mut self, id: Id, handle: ConnectHandle) -> &mut PeerComms {
        let data = self.ensure_peer(id);
        data.connecting = Some(handle);
        data.update_comm_status().comms()
    }

    pub fn remove_peer(&mut self, id: Id) -> Option<PeerComms> {
        self.map.remove(&id).map(|mut data| {
            // A bit tricky here: use PeerData::update_comm_status for the
            // side effect, then return the up-to-date member.
            data.update_comm_status();
            data.comms
        })
    }

    pub fn next_peer_for_block_fetch(&mut self) -> Option<(Id, &mut PeerComms)> {
        self.map
            .iter_mut()
            .next_back()
            .map(|(&id, data)| (id, data.update_comm_status().comms()))
    }

    pub fn infos(&self) -> Vec<PeerInfo> {
        self.map
            .iter()
            .map(|(&id, data)| PeerInfo {
                id,
                addr: data.addr,
                stats: data.stats.clone(),
            })
            .collect()
    }

    fn evict_if_full(&mut self) {
        if self.map.len() >= self.capacity {
            self.map.pop_front();
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
}

pub struct Entry<'a> {
    inner: linked_hash_map::OccupiedEntry<'a, Id, PeerData>,
}

impl<'a> Entry<'a> {
    pub fn update_comm_status(&mut self) -> CommStatus<'_> {
        self.inner.get_mut().update_comm_status()
    }

    pub fn remove(self) {
        self.inner.remove();
    }
}
