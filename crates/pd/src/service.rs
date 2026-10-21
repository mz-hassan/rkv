use crate::Pd;
use rkv_proto::pd_server::Pd as PdTrait;
use rkv_proto::raft_transport_server::RaftTransport;
use rkv_proto::*;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct PdSvc(pub Arc<Pd>);
pub struct PdRaftSvc(pub Arc<Pd>);

type Res<T> = Result<Response<T>, Status>;

#[tonic::async_trait]
impl PdTrait for PdSvc {
    async fn heartbeat(&self, req: Request<HeartbeatReq>) -> Res<HeartbeatResp> {
        Ok(Response::new(match self.0.heartbeat_full(req.into_inner()).await {
            Ok(r) => r,
            Err(e) => HeartbeatResp { error: Some(e), commands: vec![], stores: vec![] },
        }))
    }
    async fn get_route(&self, req: Request<RouteReq>) -> Res<RouteResp> {
        let r = req.into_inner();
        if let Err(e) = self.0.leader_check().await {
            return Ok(Response::new(RouteResp { error: Some(e), range: None }));
        }
        let info = if r.by_id { self.0.route_by_id(r.range_id) } else { self.0.route(&r.key) };
        Ok(Response::new(match info {
            Some(i) => RouteResp { error: None, range: Some(i) },
            None => RouteResp { error: Some(Error { code: 4, msg: "no route".into(), leader: None, ranges: vec![] }), range: None },
        }))
    }
    async fn get_all_ranges(&self, _: Request<Empty>) -> Res<AllRangesResp> {
        if let Err(e) = self.0.leader_check().await {
            return Ok(Response::new(AllRangesResp { error: Some(e), ranges: vec![], stores: vec![] }));
        }
        let (ranges, stores) = self.0.all();
        Ok(Response::new(AllRangesResp { error: None, ranges, stores }))
    }
    async fn tso(&self, req: Request<TsoReq>) -> Res<TsoResp> {
        let n = req.into_inner().count;
        Ok(Response::new(match self.0.tso(n).await {
            Ok(first) => TsoResp { error: None, first, count: n.max(1) },
            Err(e) => TsoResp { error: Some(e), first: 0, count: 0 },
        }))
    }
    async fn alloc_id(&self, req: Request<AllocIdReq>) -> Res<AllocIdResp> {
        Ok(Response::new(match self.0.alloc_id(req.into_inner().count).await {
            Ok(first) => AllocIdResp { error: None, first },
            Err(e) => AllocIdResp { error: Some(e), first: 0 },
        }))
    }
    async fn bootstrap(&self, req: Request<BootstrapReq>) -> Res<RouteResp> {
        let Some(store) = req.into_inner().store else { return Err(Status::invalid_argument("store required")) };
        Ok(Response::new(match self.0.bootstrap(store).await {
            Ok(Some(info)) => RouteResp { error: None, range: Some(info) },
            Ok(None) => RouteResp { error: Some(Error { code: 6, msg: "already bootstrapped".into(), leader: None, ranges: vec![] }), range: None },
            Err(e) => RouteResp { error: Some(e), range: None },
        }))
    }
    async fn set_config(&self, req: Request<SetConfigReq>) -> Res<AdminResp> {
        let r = req.into_inner();
        Ok(Response::new(match self.0.set_config(&r.key, &r.value).await {
            Ok(()) => AdminResp { error: None, msg: "ok".into() },
            Err(e) => AdminResp { error: Some(e), msg: "failed".into() },
        }))
    }
}

#[tonic::async_trait]
impl RaftTransport for PdRaftSvc {
    async fn send(&self, req: Request<RaftBatch>) -> Res<Empty> {
        for m in req.into_inner().msgs {
            if let Some(msg) = raft::codec::decode_message(&m.data) {
                self.0.deliver(msg);
            }
        }
        Ok(Response::new(Empty {}))
    }
}

pub async fn serve(pd: Arc<Pd>, addr: std::net::SocketAddr) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .tcp_nodelay(true)
        .add_service(rkv_proto::pd_server::PdServer::new(PdSvc(pd.clone())))
        .add_service(rkv_proto::raft_transport_server::RaftTransportServer::new(PdRaftSvc(pd)))
        .serve(addr)
        .await
}
