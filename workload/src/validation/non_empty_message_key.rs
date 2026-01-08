use crate::domain::{
    TestEvent, TestLogLine, TestValidator,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct NonEmptyMessageKeyValidator;

impl TestValidator for NonEmptyMessageKeyValidator {
    fn validator_name(&self) -> &'static str {
        "non-empty-message-key"
    }

    fn validate_event(&mut self, log: &TestLogLine) {
        match &log.data.fields {
            TestEvent::MessageWriteSucceeded(event) => {
                let key = &event.message.data.key;

                if key.as_ref().map_or(true, |k| k.is_empty()) {
                    antithesis_sdk::assert_unreachable!(
                        "Message write succeeded with missing or empty key",
                        &json!({
                            "producer": event.producer,
                            "message": event.message
                        })
                    );
                }
            }

            TestEvent::MessageReadSucceeded(event) => {
                let key = &event.message.data.key;

                if key.as_ref().map_or(true, |k| k.is_empty()) {
                    antithesis_sdk::assert_unreachable!(
                        "Message read succeeded with missing or empty key",
                        &json!({
                            "consumer": event.consumer,
                            "message": event.message
                        })
                    );
                }
            }

            _ => {}
        }
    }

    fn load_state(&mut self, _data: &str) -> Result<()> {
        Ok(())
    }

    fn save_state(&self) -> Result<String> {
        Ok("{}".to_string())
    }
}