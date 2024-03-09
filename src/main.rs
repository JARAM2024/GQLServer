use std;
use std::sync::Arc;

use async_trait::async_trait;

use pgwire::api::auth::md5pass::{hash_md5_password, MakeMd5PasswordAuthStartupHandler};
use pgwire::api::auth::{AuthSource, DefaultServerParameterProvider, LoginInfo, Password};
use pgwire::api::MakeHandler;
use pgwire::error::PgWireResult;
use pgwire::tokio::process_socket;

use tokio::net::TcpListener;

use git_backend::MakeGitQLBackend;

mod git_backend;

struct DummyAuthSource;

#[async_trait]
impl AuthSource for DummyAuthSource {
    async fn get_password(&self, login_info: &LoginInfo) -> PgWireResult<Password> {
        println!("login info: {:?}", login_info);

        let salt = vec![0, 0, 0, 0];
        let password = "pencil";

        let hash_password =
            hash_md5_password(login_info.user().as_ref().unwrap(), password, salt.as_ref());
        Ok(Password::new(Some(salt), hash_password.as_bytes().to_vec()))
    }
}

#[tokio::main]
async fn main() {
    let mut parameters = DefaultServerParameterProvider::default();
    parameters.server_version = String::from("15");
    let authenticator = Arc::new(MakeMd5PasswordAuthStartupHandler::new(
        Arc::new(DummyAuthSource),
        Arc::new(parameters),
    ));

    let backend = MakeGitQLBackend::new(None);
    let processor = Arc::new(backend);

    let server_addr = "127.0.0.1:5321";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let authenticator_ref = authenticator.make();
        let processor_ref = processor.make();
        tokio::spawn(async move {
            process_socket(
                incoming_socket.0,
                None,
                authenticator_ref,
                processor_ref.clone(),
                processor_ref,
            )
            .await
        });
    }
}
