use stream_replay_rs::auth::{M2MAuthenticator, DEFAULT_SCOPE, DEFAULT_SERVICE_ACCOUNT};

#[tokio::test]
async fn test_m2m_auth_default_offline_token() {
    let mut auth = M2MAuthenticator::new(None, None, None, None);
    assert_eq!(auth.service_account, DEFAULT_SERVICE_ACCOUNT);

    let token = auth.authenticate().await.unwrap();
    assert!(token.contains("mock_m2m_jwt_svc_stream_replay"));
    assert!(auth.is_authenticated);
    assert_eq!(auth.get_token(), Some(token.as_str()));
    assert_eq!(auth.auth_header(), Some(format!("Bearer {token}")));
}

#[tokio::test]
async fn test_m2m_auth_custom_bearer_token() {
    let mut auth = M2MAuthenticator::new(
        None,
        Some("svc_stream_replay".to_string()),
        None,
        Some("pre_issued_kanidm_token_xyz".to_string()),
    );

    let token = auth.authenticate().await.unwrap();
    assert_eq!(token, "pre_issued_kanidm_token_xyz");
    assert!(auth.is_authenticated);
    assert_eq!(
        auth.auth_header(),
        Some("Bearer pre_issued_kanidm_token_xyz".to_string())
    );
}

#[tokio::test]
async fn test_m2m_auth_client_secret_exchange() {
    let mut auth = M2MAuthenticator::new(
        Some("https://localhost:8443".to_string()),
        Some("svc_stream_replay".to_string()),
        Some("super_secret_m2m_token_key".to_string()),
        None,
    );

    let token = auth.authenticate().await.unwrap();
    assert!(token.starts_with("kanidm_m2m_svc_stream_replay"));
    assert!(auth.is_authenticated);
    assert_eq!(DEFAULT_SCOPE, "stream:produce:intake telemetry:write");
}
