mod follower;
mod leader;

pub use follower::Follower;
pub use leader::Leader;

pub enum Role {
    Leader(Leader),
    Follower(Follower),
    NotReady,
}
