//! Shared validation and stable failure mapping for HTTP and gRPC publishing.

use std::sync::Arc;

use router_core::{PublishCommand, PublishError, PublishErrorKind};
use uuid::Uuid;

use crate::ApiError;

pub(crate) fn effective_message_id(message_id: Option<String>) -> Arc<str> {
    message_id.map_or_else(|| Arc::from(Uuid::new_v4().to_string()), Arc::<str>::from)
}

pub(crate) fn validate_command(
    command: PublishCommand,
    maximum_payload_bytes: usize,
) -> Result<PublishCommand, ApiError> {
    if command.payload.len() > maximum_payload_bytes {
        return Err(ApiError::BadRequest(format!(
            "payload exceeds maximum size of {maximum_payload_bytes} bytes"
        )));
    }
    command
        .validate()
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    Ok(command)
}

pub(crate) fn validate_json_content_type(content_type: &str) -> Result<(), ApiError> {
    let parsed = content_type
        .parse::<mime::Mime>()
        .map_err(|_| ApiError::BadRequest("content_type must be a valid MIME type".to_owned()))?;
    if parsed.type_() == mime::APPLICATION
        && (parsed.subtype() == mime::JSON || parsed.suffix() == Some(mime::JSON))
    {
        Ok(())
    } else {
        Err(ApiError::BadRequest(
            "JSON payload requires an application/json or application/*+json content_type"
                .to_owned(),
        ))
    }
}

pub(crate) fn map_publish_error(error: &PublishError) -> ApiError {
    match error.kind() {
        PublishErrorKind::InvalidInput => ApiError::BadRequest(error.to_string()),
        PublishErrorKind::Timeout => ApiError::PublisherTimeout,
        PublishErrorKind::QueueFull => ApiError::PublisherQueueFull,
        PublishErrorKind::Backend => ApiError::Backend("publish backend failed".to_owned()),
    }
}
