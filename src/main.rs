use std;
use std::io::{Error, ErrorKind};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream;
use futures::Stream;
use gitql_ast::object::GitQLObject;
use gitql_ast::types::DataType;
use pgwire::api::auth::md5pass::{hash_md5_password, MakeMd5PasswordAuthStartupHandler};
use pgwire::api::auth::{AuthSource, DefaultServerParameterProvider, LoginInfo, Password};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler, StatementOrPortal};
use pgwire::api::results::{
    DataRowEncoder, DescribeResponse, FieldFormat, FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::api::stmt::NoopQueryParser;
use pgwire::api::{ClientInfo, MakeHandler, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::tokio::process_socket;
use tokio::io;
use tokio::net::TcpListener;

use git_data_provider::GitDataProvider;
use git_schema::TABLES_FIELDS_NAMES;
use git_schema::TABLES_FIELDS_TYPES;
use gitql_ast::environment::Environment;
use gitql_ast::schema::Schema;
use gitql_ast::value::Value;
use gitql_engine::data_provider::DataProvider;
use gitql_engine::engine::EvaluationResult::SelectedGroups;
use gitql_engine::engine::{self};
use gitql_parser::parser;
use gitql_parser::tokenizer;

mod git_data_provider;
mod git_schema;

pub struct GitQLBackend {
    repositories: Arc<[String]>,
    query_parser: Arc<NoopQueryParser>,
}

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

#[async_trait]
impl SimpleQueryHandler for GitQLBackend {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let git_repo_result = validate_git_repositories(&self.repositories);

        if git_repo_result.is_err() {
            println!("Failed to load git repositories");
            return Err(PgWireError::IoError(Error::new(
                ErrorKind::Other,
                "Failed to laod git repositories",
            )));
        }

        let repos = git_repo_result.ok().unwrap();
        let schema = Schema {
            tables_fields_names: TABLES_FIELDS_NAMES.to_owned(),
            tables_fields_types: TABLES_FIELDS_TYPES.to_owned(),
        };

        let mut env = Environment::new(schema);
        let front_start = std::time::Instant::now();
        let tokenizer_result = tokenizer::tokenize(query.to_string());
        if tokenizer_result.is_err() {
            println!("Cannot tokenize result");
            return Err(PgWireError::IoError(Error::new(
                ErrorKind::Other,
                "Failed to tokenize result",
            )));
        }

        let tokens = tokenizer_result.ok().unwrap();
        if tokens.is_empty() {
            println!("Empty Tokens");
            return Err(PgWireError::IoError(Error::new(
                ErrorKind::Other,
                "Empty Tokens",
            )));
        }

        let parser_result = parser::parse_gql(tokens, &mut env);
        if parser_result.is_err() {
            println!("Cannot parse result");
            return Err(PgWireError::IoError(Error::new(
                ErrorKind::Other,
                "Failed to parse result",
            )));
        }

        let query_node = parser_result.ok().unwrap();
        let front_duration = front_start.elapsed();

        let engine_start = std::time::Instant::now();
        let provider: Box<dyn DataProvider> = Box::new(GitDataProvider::new(repos.to_vec()));
        let evaluation_result = engine::evaluate(&mut env, &provider, query_node);

        if evaluation_result.is_err() {
            println!("Cannot evaluate result");
            return Err(PgWireError::IoError(Error::new(
                ErrorKind::Other,
                "Failed to evaluate result",
            )));
        }
        let engine_result = evaluation_result.ok().unwrap();

        if let SelectedGroups(mut groups, hidden_selection) = engine_result {
            let mut indexes = vec![];
            for (index, title) in groups.titles.iter().enumerate() {
                if hidden_selection.contains(title) {
                    indexes.insert(0, index);
                }
            }

            if groups.len() > 1 {
                groups.flat();
            }

            for index in indexes {
                groups.titles.remove(index);

                for row in &mut groups.groups[0].rows {
                    row.values.remove(index);
                }
            }

            let mut fields_info: Vec<FieldInfo> = vec![];

            for (index, title) in groups.titles.iter().enumerate() {
                let field_result = encode_title(title, index);
                if field_result.is_err() {
                    continue;
                }
                fields_info.push(field_result.ok().unwrap());
            }

            let result = encode_row(&groups, Arc::new(fields_info.clone()));
            return Ok(vec![Response::Query(QueryResponse::new(
                Arc::new(fields_info),
                result,
            ))]);
        }

        return Err(PgWireError::IoError(Error::new(
            ErrorKind::Other,
            "Failed to make result",
        )));
    }
}

#[async_trait]
impl ExtendedQueryHandler for GitQLBackend {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        client: &mut C,
        portal: &'a Portal<Self::Statement>,
        max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42846".to_owned(),
            format!("Unimplemented"),
        ))))
    }

    async fn do_describe<C>(
        &self,
        client: &mut C,
        target: StatementOrPortal<'_, Self::Statement>,
    ) -> PgWireResult<DescribeResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42846".to_owned(),
            format!("Unimplemented"),
        ))))
    }
}

struct MakeGitQLBackend {
    repositories: Arc<[String]>,
    query_parser: Arc<NoopQueryParser>,
}

impl MakeGitQLBackend {
    fn new() -> MakeGitQLBackend {
        MakeGitQLBackend {
            repositories: Arc::from([String::from("/Users/yong/Workspace/PHP/HakSul")]),
            query_parser: Arc::new(NoopQueryParser::new()),
        }
    }
}

impl MakeHandler for MakeGitQLBackend {
    type Handler = Arc<GitQLBackend>;

    fn make(&self) -> Self::Handler {
        Arc::new(GitQLBackend {
            repositories: self.repositories.clone(),
            query_parser: self.query_parser.clone(),
        })
    }
}

#[tokio::main]
async fn main() {
    let parameters = DefaultServerParameterProvider::default();
    let authenticator = Arc::new(MakeMd5PasswordAuthStartupHandler::new(
        Arc::new(DummyAuthSource),
        Arc::new(parameters),
    ));

    let processor = Arc::new(MakeGitQLBackend::new());

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

fn validate_git_repositories(repositories: &Arc<[String]>) -> Result<Vec<gix::Repository>, String> {
    let mut git_repositories: Vec<gix::Repository> = vec![];
    for repository in repositories.iter() {
        let git_repository = gix::open(repository);
        if git_repository.is_err() {
            return Err(git_repository.err().unwrap().to_string());
        }
        git_repositories.push(git_repository.ok().unwrap());
    }
    Ok(git_repositories)
}

fn encode_row(
    groups: &GitQLObject,
    fields_info: Arc<Vec<FieldInfo>>,
) -> impl Stream<Item = PgWireResult<DataRow>> {
    let mut elements = vec![];

    for row in &groups.groups[0].rows {
        let mut encoder = DataRowEncoder::new(fields_info.clone().into());
        for value in row.values.iter() {
            match value {
                Value::Text(text) => encoder.encode_field(&text).unwrap(),
                Value::Integer(int) => encoder.encode_field(&int).unwrap(),
                Value::Float(float) => encoder.encode_field(&float).unwrap(),
                Value::Boolean(bool) => encoder.encode_field(&bool).unwrap(),
                Value::Time(time) => encoder.encode_field(&time).unwrap(),
                Value::Date(date) => encoder.encode_field(&date).unwrap(),
                Value::DateTime(date) => encoder.encode_field(&date).unwrap(),
                _ => encoder.encode_field(&None::<i8>).unwrap(),
            }
        }
        elements.push(encoder.finish());
    }

    stream::iter(elements.into_iter())
}

fn encode_title(string: &str, index: usize) -> PgWireResult<FieldInfo> {
    match TABLES_FIELDS_TYPES[string] {
        DataType::Text => Ok(FieldInfo::new(
            String::from(string),
            None,
            None,
            Type::TEXT,
            Format::UnifiedText.format_for(index),
        )),
        DataType::Integer => Ok(FieldInfo::new(
            String::from(string),
            None,
            None,
            Type::INT8,
            Format::UnifiedText.format_for(index),
        )),
        DataType::Float => Ok(FieldInfo::new(
            String::from(string),
            None,
            None,
            Type::FLOAT8,
            Format::UnifiedText.format_for(index),
        )),
        DataType::Boolean => Ok(FieldInfo::new(
            String::from(string),
            None,
            None,
            Type::BOOL,
            Format::UnifiedText.format_for(index),
        )),
        DataType::Time => Ok(FieldInfo::new(
            String::from(string),
            None,
            None,
            Type::TIME,
            Format::UnifiedText.format_for(index),
        )),
        DataType::Date => Ok(FieldInfo::new(
            String::from(string),
            None,
            None,
            Type::DATE,
            Format::UnifiedText.format_for(index),
        )),
        DataType::DateTime => Ok(FieldInfo::new(
            String::from(string),
            None,
            None,
            Type::TIME,
            Format::UnifiedText.format_for(index),
        )),
        _ => Err(PgWireError::IoError(Error::new(
            ErrorKind::Other,
            "Failed to get type of data",
        ))),
    }
}
