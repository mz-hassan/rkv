//! Distributed lock / lease service built on the store's linearizable CAS-with-TTL.
//!
//! A lease is the key `__lease/<name>` holding `owner|token`, written by a CAS from "absent" with a TTL. Expiry is decided by
//! the replicated state machine (an expired value counts as absent for CAS), so two clients can never both believe they hold
//! the lock at the store. Clients can still pause (GC, SIGSTOP) after being granted a lease and act on it after expiry; the
//! monotonically increasing fencing `token` lets the protected resource reject such stale holders (Kleppmann's fencing tokens).
use crate::raw::RawClient;
use crate::router::ClientError;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    pub name: String,
    pub owner: String,
    /// strictly increases with every successful acquisition of this lock name
    pub token: u64,
    pub ttl_ms: u64,
}

pub struct LeaseClient {
    raw: Arc<RawClient>,
}

fn lease_key(name: &str) -> Vec<u8> {
    format!("__lease/{name}").into_bytes()
}
fn counter_key(name: &str) -> Vec<u8> {
    format!("__lease_token/{name}").into_bytes()
}
fn encode(owner: &str, token: u64) -> Vec<u8> {
    format!("{owner}|{token}").into_bytes()
}

impl LeaseClient {
    pub fn new(raw: Arc<RawClient>) -> LeaseClient {
        LeaseClient { raw }
    }

    async fn next_token(&self, name: &str) -> Result<u64, ClientError> {
        loop {
            let cur = self.raw.get(&counter_key(name)).await?;
            let n: u64 = cur.as_deref().map(|v| String::from_utf8_lossy(v).parse().unwrap_or(0)).unwrap_or(0);
            if self.raw.cas(&counter_key(name), cur.as_deref(), (n + 1).to_string().as_bytes()).await? {
                return Ok(n + 1);
            }
        }
    }

    /// Try to take the lock; `Ok(None)` if somebody else holds an unexpired lease.
    pub async fn acquire(&self, name: &str, owner: &str, ttl_ms: u64) -> Result<Option<Lease>, ClientError> {
        let token = self.next_token(name).await?;
        let ok = self.raw.cas_ttl(&lease_key(name), None, &encode(owner, token), ttl_ms).await?;
        Ok(ok.then(|| Lease { name: name.into(), owner: owner.into(), token, ttl_ms }))
    }

    /// Extend the lease. `Ok(false)` means it was lost (expired, possibly re-granted to someone else).
    pub async fn renew(&self, l: &Lease) -> Result<bool, ClientError> {
        let v = encode(&l.owner, l.token);
        self.raw.cas_ttl(&lease_key(&l.name), Some(&v), &v, l.ttl_ms).await
    }

    /// Release the lock if (and only if) this exact lease still holds it.
    pub async fn release(&self, l: &Lease) -> Result<bool, ClientError> {
        let v = encode(&l.owner, l.token);
        // overwrite with a value that expires immediately: later CAS-from-absent succeeds
        self.raw.cas_ttl(&lease_key(&l.name), Some(&v), b"released", 1).await
    }

    /// Current holder `(owner, token)` if any.
    pub async fn holder(&self, name: &str) -> Result<Option<(String, u64)>, ClientError> {
        Ok(self.raw.get(&lease_key(name)).await?.and_then(|v| {
            let s = String::from_utf8_lossy(&v).into_owned();
            let (o, t) = s.rsplit_once('|')?;
            Some((o.to_string(), t.parse().ok()?))
        }))
    }
}
