#[derive(Copy, Clone)]
pub enum Tag {
    User,
    ScreenName,
    ShortStatus,
    FullStatus,
    Delete,
    CompletedFile,
}

impl Tag {
    pub fn value(self) -> u8 {
        match self {
            Tag::User => 0,
            Tag::ScreenName => 1,
            Tag::FullStatus => 2,
            Tag::ShortStatus => 3,
            Tag::Delete => 4,
            Tag::CompletedFile => 16,
        }
    }
}
