use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct RecordMessage {
    key: String,
    data: String,
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
